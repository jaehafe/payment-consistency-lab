package idempotentconsumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// DeadLetter: 계속 실패하는 메시지(poison message)를 DLT 로 치운다.
// Spring Kafka 의 DefaultErrorHandler + DeadLetterPublishingRecoverer 를
// 손으로 옮긴 것 (README "Spring Kafka 와 비교").
// ---------------------------------------------------------------------------

// DLT 레코드에 붙이는 헤더. 이름은 Spring Kafka KafkaHeaders.DLT_* 와 같다.
// 차이: Spring 은 partition/offset 을 4/8바이트 big-endian 으로 넣지만,
// 여기서는 rpk 로 바로 읽히게 전부 UTF-8 문자열로 넣는다.
const (
	HeaderDLTOriginalTopic     = "kafka_dlt-original-topic"
	HeaderDLTOriginalPartition = "kafka_dlt-original-partition"
	HeaderDLTOriginalOffset    = "kafka_dlt-original-offset"
	HeaderDLTOriginalGroup     = "kafka_dlt-original-consumer-group"
	HeaderDLTExceptionFQCN     = "kafka_dlt-exception-fqcn"
	HeaderDLTExceptionMessage  = "kafka_dlt-exception-message"
	HeaderDLTAttempts          = "kafka_dlt-attempts" // lab 전용
)

// DLTTopic 은 원래 토픽의 dead letter topic 이름.
// Spring Kafka 3.3+ 기본값과 같은 "<topic>-dlt" (그 전엔 ".DLT").
func DLTTopic(topic string) string { return topic + "-dlt" }

// DeadLetter 는 ConsumerLoop 의 DLT 설정 + 실패 횟수 기록.
// NewDeadLetter 로 만든다 (설정 검증). 구조체 리터럴로 만들어도
// PollOnce/Run 이 시작 전에 Validate 를 부른다.
//
// ★ 실패 횟수는 이 인스턴스의 메모리에만 있다 = MaxAttempts 는 "컨슈머
// 인스턴스 하나 기준". 재시작하거나 리밸런스로 파티션이 넘어가면 0 부터
// 다시 센다 (README "한계"). Spring 의 FailedRecordTracker 도 메모리다.
type DeadLetter struct {
	Producer    *kgo.Client // DLT 로 보낼 클라이언트 (기본 acks=all)
	MaxAttempts int         // retryable 에러로 이 횟수만큼 실패하면 DLT 로 (>= 1)
	Group       string      // 헤더용: 어느 컨슈머 그룹에서 실패했나

	// ConsumerLoop 는 고루틴 하나지만, 리밸런스 훅(ForgetPartitions)은
	// franz-go 내부 고루틴에서 불린다 → map 을 락으로 지킨다.
	mu sync.Mutex
	// (토픽, 파티션, offset) → 지금까지 retryable 실패 횟수.
	attempts map[recordKey]int
}

// ErrInvalidDeadLetter 는 DeadLetter 설정이 잘못됐다는 뜻.
var ErrInvalidDeadLetter = errors.New("invalid dead letter config")

// NewDeadLetter 는 설정을 검증하고 DeadLetter 를 만든다.
func NewDeadLetter(
	producer *kgo.Client, maxAttempts int, group string,
) (*DeadLetter, error) {
	d := &DeadLetter{
		Producer: producer, MaxAttempts: maxAttempts, Group: group,
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

// Validate 는 첫 실패 때가 아니라 시작할 때 설정 실수를 잡는다.
//   - Producer 가 nil 이면 첫 DLT 발행에서 panic
//   - MaxAttempts <= 0 이면 첫 실패에 곧장 DLT (재시도 없음)
func (d *DeadLetter) Validate() error {
	if d.Producer == nil {
		return fmt.Errorf("%w: Producer is nil", ErrInvalidDeadLetter)
	}
	if d.MaxAttempts < 1 {
		return fmt.Errorf("%w: MaxAttempts must be >= 1, got %d",
			ErrInvalidDeadLetter, d.MaxAttempts)
	}
	return nil
}

// recordKey 는 Kafka 안에서 레코드 하나를 가리키는 좌표.
// 구조체는 == 비교가 되므로 map 키로 쓸 수 있다 (Kotlin data class).
type recordKey struct {
	topic     string
	partition int32
	offset    int64
}

func keyOf(r *kgo.Record) recordKey {
	return recordKey{r.Topic, r.Partition, r.Offset}
}

// failed 는 rec 의 실패 1회를 분류·기록하고, 누적 횟수와
// "이제 DLT 로 보낼 차례인가"를 돌려준다 (errclass.go).
//   - transient: 세지 않는다, DLT 로 안 간다 (Run 이 백오프로 재시도)
//   - not-retryable: 1번 만에 DLT
//   - retryable: MaxAttempts 번째에 DLT
func (d *DeadLetter) failed(
	rec *kgo.Record, err error,
) (attempts int, class errorClass, giveUp bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.attempts == nil { // nil map 에 쓰면 panic → 처음에 만든다
		d.attempts = map[recordKey]int{}
	}
	k := keyOf(rec)
	class = classify(err)
	if class == classTransient {
		return d.attempts[k], class, false
	}
	d.attempts[k]++ // 없는 키는 0 에서 시작
	n := d.attempts[k]
	return n, class, n >= d.MaxAttempts || class == classNotRetryable
}

// succeeded 는 처리에 성공한 rec 의 실패 기록을 지운다.
// 포인터 리시버는 nil 이어도 호출된다 → nil 체크로 Kotlin 의
// deadLetter?.succeeded(rec) 처럼 쓴다.
func (d *DeadLetter) succeeded(rec *kgo.Record) {
	if d == nil {
		return
	}
	d.forget(keyOf(rec))
}

func (d *DeadLetter) forget(k recordKey) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.attempts, k) // 없는 키, nil map 이어도 안전
}

// ForgetPartitions 는 이 인스턴스에서 빠진 파티션의 실패 기록을 지운다.
// 시그니처가 kgo.OnPartitionsRevoked / kgo.OnPartitionsLost 콜백과 같아서
// 그룹 클라이언트를 만들 때 그대로 건다:
//
//	dead, _ := NewDeadLetter(producer, 3, group)
//	cl, _ := kgo.NewClient(...,
//		kgo.OnPartitionsRevoked(dead.ForgetPartitions),
//		kgo.OnPartitionsLost(dead.ForgetPartitions))
//
// 걸지 않으면 넘어간 파티션의 기록이 map 에 계속 남는다 (메모리 누수,
// 나중에 같은 파티션을 다시 받으면 낡은 횟수부터 센다).
// ConsumerLoop 는 실패해도 클라이언트를 닫지 않으므로(SetOffsets 로
// 되감기) 재시도 중에 이 훅이 불려 횟수가 리셋되는 일은 없다.
func (d *DeadLetter) ForgetPartitions(
	_ context.Context, _ *kgo.Client, lost map[string][]int32,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.attempts { // 순회 중 delete 는 Go 에서 안전하다
		for _, p := range lost[k.topic] {
			if k.partition == p {
				delete(d.attempts, k)
			}
		}
	}
}

// attemptsOf 는 테스트 관찰용: 그 레코드의 지금까지 실패 횟수.
func (d *DeadLetter) attemptsOf(topic string, partition int32, offset int64) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attempts[recordKey{topic, partition, offset}]
}

// publish 는 rec 을 DLT 로 보내고 브로커 ack 까지 기다린다.
// key·value·헤더는 그대로, 실패 정보 헤더를 덧붙인다.
func (d *DeadLetter) publish(
	ctx context.Context, rec *kgo.Record, cause error, attempts int,
) error {
	// 새 슬라이스에 복사: rec.Headers 에 바로 append 하면
	// 원래 레코드의 배열을 건드릴 수 있다.
	hs := make([]kgo.RecordHeader, 0, len(rec.Headers)+7)
	hs = append(hs, rec.Headers...)
	add := func(k, v string) { // 클로저 (Kotlin 람다)
		hs = append(hs, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	add(HeaderDLTOriginalTopic, rec.Topic)
	add(HeaderDLTOriginalPartition, strconv.Itoa(int(rec.Partition)))
	add(HeaderDLTOriginalOffset, strconv.FormatInt(rec.Offset, 10))
	add(HeaderDLTOriginalGroup, d.Group)
	add(HeaderDLTExceptionFQCN, errorName(cause))
	add(HeaderDLTExceptionMessage, cause.Error())
	add(HeaderDLTAttempts, strconv.Itoa(attempts))

	// 파티션은 지정하지 않는다 → 같은 key 는 DLT 에서도 같은
	// 파티션 (Spring 기본은 "원래와 같은 번호의 파티션").
	out := &kgo.Record{
		Topic: DLTTopic(rec.Topic), Key: rec.Key, Value: rec.Value,
		Headers: hs,
	}
	// ProduceSync = 브로커 ack 까지 블록 (Kotlin send().get())
	if err := d.Producer.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("dead-letter publish to %s: %w",
			out.Topic, err)
	}
	d.forget(keyOf(rec))
	return nil
}

// errorName 은 Spring 의 exception-fqcn 헤더에 해당하는 값.
// Go 에는 예외 클래스가 없어서, 알려진 sentinel 에러는 이름을,
// 나머지는 동적 타입(%T)을 쓴다.
func errorName(err error) string {
	if errors.Is(err, ErrMissingMessageID) {
		return "idempotentconsumer.ErrMissingMessageID"
	}
	return fmt.Sprintf("%T", err)
}
