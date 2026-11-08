package idempotentconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageHandler 는 메시지 한 건을 처리한다. 반환값 processed=false 는 "중복이라 건너뜀".
// NaiveConsumer, IdempotentConsumer 둘 다 이 메서드를 갖고 있으므로 자동으로 이 인터페이스를 만족한다
// (Go는 implements 선언이 없다).
type MessageHandler interface {
	Handle(ctx context.Context, rec *kgo.Record) (processed bool, err error)
}

// ---------------------------------------------------------------------------
// NaiveConsumer: 받을 때마다 그대로 처리한다. 같은 메시지가 두 번 오면 포인트도 두 번 쌓인다.
// ---------------------------------------------------------------------------

type NaiveConsumer struct {
	DB *pgxpool.Pool
}

func (c *NaiveConsumer) Handle(ctx context.Context, rec *kgo.Record) (bool, error) {
	ev, err := decode(rec)
	if err != nil {
		return false, err
	}
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	if err := applyPayment(ctx, tx, ev); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// IdempotentConsumer: processed_messages 로 "이미 처리한 message_id" 를 기록해서 중복을 건너뛴다.
// ---------------------------------------------------------------------------

type IdempotentConsumer struct {
	DB    *pgxpool.Pool
	Group string // 컨슈머 그룹마다 처리 여부를 따로 관리한다 (포인트 서비스와 알림 서비스는 각자 처리해야 하므로)
}

func (c *IdempotentConsumer) Handle(ctx context.Context, rec *kgo.Record) (bool, error) {
	msgID := header(rec, "message_id")
	if msgID == "" {
		// %w 로 감싸면 errors.Is(err, ErrMissingMessageID) 로 판별 가능.
		// 어느 메시지인지(토픽-파티션@offset) 같이 남긴다.
		return false, fmt.Errorf("%w: %s-%d@%d (key=%s)",
			ErrMissingMessageID,
			rec.Topic, rec.Partition, rec.Offset, rec.Key)
	}
	ev, err := decode(rec)
	if err != nil {
		return false, err
	}

	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	// ★ 핵심: "처리 기록"과 "비즈니스 처리"를 같은 트랜잭션에 넣는다.
	//  - 둘 다 커밋되거나 둘 다 롤백된다 → "처리했는데 기록 안 됨" / "기록했는데 처리 안 됨" 이 없다.
	//  - 같은 메시지를 두 컨슈머가 동시에 처리해도, 늦은 쪽 INSERT 는 PK 충돌로 앞선 tx 커밋을 기다렸다가 0행이 된다.
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_messages (consumer_group, message_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, c.Group, msgID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil // 이미 처리한 메시지 → 아무것도 하지 않음 (tx 는 defer 로 롤백)
	}

	if err := applyPayment(ctx, tx, ev); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// applyPayment 는 실제 비즈니스 처리.
//   - orders 갱신: "PAID 로, 금액은 X 로" 덮어쓰기라 여러 번 해도 결과가 같다 (자연적으로 멱등).
//   - point_ledger insert: 할 때마다 행이 늘어난다 (멱등하지 않음) ← 중복 처리의 피해가 드러나는 곳.
//
// pgx.Tx 는 인터페이스 타입이라 어떤 트랜잭션 구현이든 받을 수 있다.
func applyPayment(ctx context.Context, tx pgx.Tx, ev PaymentCompleted) error {
	if _, err := tx.Exec(ctx,
		`UPDATE orders SET status = 'PAID', paid_amount = $2 WHERE id = $1`, ev.OrderID, ev.Amount); err != nil {
		return err
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO point_ledger (order_id, points) VALUES ($1, $2)`, ev.OrderID, ev.Amount/100) // 1% 적립
	return err
}

// ---------------------------------------------------------------------------
// ConsumerLoop: Kafka 에서 가져와 Handler 로 처리하고, 그 "다음에" offset 을 커밋한다.
// ---------------------------------------------------------------------------

type ConsumerLoop struct {
	Client  *kgo.Client // kgo.ConsumerGroup + kgo.DisableAutoCommit 으로 만든 클라이언트여야 한다
	Handler MessageHandler

	CrashBeforeOffsetCommit bool // 실패 주입: DB 커밋 후 offset 커밋 전에 죽음

	// nil 이면 DLT 없음: 실패한 메시지를 성공할 때까지 무한 재시도.
	// 있으면 에러를 분류해서 (errclass.go) retryable 은 MaxAttempts 번째,
	// not-retryable 은 1번 만에 DLT 로 보내고 넘어간다. transient(DB·네트워크
	// 장애)는 DLT 로 보내지 않고 Run 이 백오프로 끝없이 재시도한다.
	DeadLetter *DeadLetter

	// Run 의 재시도 간격 상한. 0 이면 DefaultMaxRetryDelay(30초).
	MaxRetryDelay time.Duration
}

// ErrMissingMessageID 는 message_id 헤더가 없는 메시지. 중복 제거 키가 없으니 처리할 수 없다.
var ErrMissingMessageID = errors.New("message_id header is missing")

// ErrSimulatedCrash 는 offset 커밋 직전 크래시를 흉내 낸다.
var ErrSimulatedCrash = errors.New("consumer crashed before offset commit (simulated)")

// PollStats 는 한 번 poll 한 결과.
type PollStats struct {
	Processed int // 실제 처리
	Skipped   int // 중복이라 건너뜀
	// 재시도를 포기하고 DLT 로 보냄 (DeadLetter 가 있을 때만)
	DeadLettered int
}

// PollOnce 는 한 번 가져온 레코드들을 처리하고 offset 을 커밋한다.
func (l *ConsumerLoop) PollOnce(ctx context.Context) (PollStats, error) {
	var stats PollStats
	if l.DeadLetter != nil { // 리터럴로 만든 잘못된 설정을 첫 실패 전에 잡는다
		if err := l.DeadLetter.Validate(); err != nil {
			return stats, err
		}
	}
	fetches := l.Client.PollFetches(ctx) // 레코드가 올 때까지(또는 ctx 취소까지) 기다린다
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	recs := fetches.Records()
	if errs := fetches.Errors(); len(errs) > 0 {
		// 에러가 난 fetch 에도 다른 파티션의 레코드가 섞여 올 수 있고,
		// 클라이언트 위치는 이미 그 뒤로 넘어갔다. 처리 없이 버리면
		// 다음 poll 이 그 뒤를 커밋해 유실된다 → 전부 되감는다.
		cause := fmt.Errorf("fetch: %v", errs[0].Err)
		return stats, l.rewindFrom(ctx, recs, 0, cause)
	}

	for i, rec := range recs {
		processed, err := l.Handler.Handle(ctx, rec)
		if err != nil {
			dead, dlErr := l.deadLetterOrRetry(ctx, rec, err)
			if dead { // DLT ack 받음 → 커밋 대상에 넣고 다음 레코드로
				stats.DeadLettered++
				continue
			}
			if dlErr != nil {
				err = dlErr
			}
			// 실패한 메시지 "앞"까지만 커밋하고, 실패한 메시지부터
			// 다시 받도록 되감는다. (아래 rewindFrom 설명 참고)
			return stats, l.rewindFrom(ctx, recs, i, err)
		}
		l.DeadLetter.succeeded(rec) // nil 이어도 안전 (deadletter.go)
		if processed {
			stats.Processed++
		} else {
			stats.Skipped++
		}
	}

	// ★ 순서가 중요하다: DB 커밋(위 Handle) → offset 커밋(아래).
	// 반대로 offset 을 먼저 커밋하고 DB 처리 중 죽으면, 메시지는 "읽은 것"으로 처리되어 영영 유실된다 (at-most-once).
	// 이 순서면 둘 사이에서 죽었을 때 메시지가 "다시" 온다 (at-least-once) → 그 중복은 processed_messages 가 걸러낸다.
	if l.CrashBeforeOffsetCommit {
		return stats, ErrSimulatedCrash
	}
	if len(recs) > 0 {
		if err := l.Client.CommitRecords(ctx, recs...); err != nil { // recs... = 슬라이스를 가변 인자로 펼쳐 전달 (Kotlin의 *array)
			return stats, err
		}
	}
	return stats, nil
}

// deadLetterOrRetry 는 처리 실패한 rec 을 어떻게 할지 정한다.
// (Spring DefaultErrorHandler 가 하는 일)
//
//	(false, nil) 아직 재시도 (transient 는 언제나) → 호출자가 rec 부터 되감는다
//	(true, nil)  DLT 발행 ack 받음 → rec 을 넘어 커밋해도 된다
//	(false, err) DLT 발행 실패 → 커밋하지 않고 되감는다
//
// ★ DLT ack 를 받은 "뒤에만" rec 을 커밋 대상에 넣는다. 커밋을
// 먼저 하고 DLT 발행이 실패하면 메시지는 어디에도 남지 않는다
// (유실). 이 순서면 최악이 "DLT 에 두 번"이다 (at-least-once).
func (l *ConsumerLoop) deadLetterOrRetry(
	ctx context.Context, rec *kgo.Record, cause error,
) (bool, error) {
	d := l.DeadLetter
	if d == nil {
		return false, nil
	}
	n, class, giveUp := d.failed(rec, cause)
	if class == classTransient {
		log.Printf("인프라 장애(transient), DLT 대상 아님 — 백오프 후 재시도: %s-%d@%d: %v",
			rec.Topic, rec.Partition, rec.Offset, cause)
		return false, nil
	}
	if !giveUp {
		log.Printf("처리 실패 %d/%d (%s), 재시도: %s-%d@%d: %v",
			n, d.MaxAttempts, class, rec.Topic, rec.Partition, rec.Offset,
			cause)
		return false, nil
	}
	if err := d.publish(ctx, rec, cause, n); err != nil {
		return false, errors.Join(cause, err)
	}
	log.Printf("DLT 로 보냄 (%s, 시도 %d번): %s-%d@%d → %s: %v",
		class, n, rec.Topic, rec.Partition, rec.Offset, DLTTopic(rec.Topic),
		cause)
	return true, nil
}

// rewindFrom 은 recs[failed] 처리 실패 후 정리한다.
//
// 그냥 return 만 하면 안 되는 이유: kgo.Client 는 메모리에 "다음에 가져올
// 위치"를 들고 있고, 그 위치는 이미 실패한 메시지 뒤로 넘어가 있다.
// 같은 클라이언트로 계속 poll 하면 그 뒤 메시지가 오고, 그걸 처리한 뒤
// CommitRecords 가 실패한 메시지를 넘어선 offset 을 커밋한다 → 유실.
// (Kotlin 의 ConsumerRunner 는 컨슈머를 새로 만들어서 피한다.)
//
//  1. 이번 fetch 에서 처리 못 한 파티션마다 첫 미처리 offset 으로 되감기
//     → 다음 PollOnce 가 실패한 메시지부터 다시 받는다.
//  2. 성공한 recs[:failed] 는 커밋 (진행분은 지킨다)
//
// 되감기를 먼저 하는 이유: 커밋이 실패해서 일찍 return 하더라도
// 되감기는 이미 끝나 있어야 한다. 커밋 실패는 무해하다 — 성공분이
// 다시 오면 processed_messages 가 skip 한다. 되감기 누락은 유실이다.
func (l *ConsumerLoop) rewindFrom(
	ctx context.Context, recs []*kgo.Record, failed int, cause error,
) error {
	// map[토픽]map[파티션]위치 — Kotlin의 Map<String, Map<Int, ...>>
	seek := map[string]map[int32]kgo.EpochOffset{}
	for _, r := range recs[failed:] {
		if seek[r.Topic] == nil { // 없는 키를 읽으면 zero 값(nil)
			seek[r.Topic] = map[int32]kgo.EpochOffset{}
		}
		// 파티션 안에서는 offset 이 오름차순 → 처음 본 것이 첫 미처리
		if _, ok := seek[r.Topic][r.Partition]; !ok {
			seek[r.Topic][r.Partition] = kgo.EpochOffset{
				Epoch: r.LeaderEpoch, Offset: r.Offset,
			}
		}
	}
	// Kotlin(Java) 의 consumer.seek(partition, offset) 에 해당.
	l.Client.SetOffsets(seek)

	if failed > 0 {
		done := recs[:failed] // 슬라이스 앞부분 [0, failed)
		if err := l.Client.CommitRecords(ctx, done...); err != nil {
			return errors.Join(cause, err) // 에러 둘을 묶는다
		}
	}
	return cause
}

// Run 은 ctx 가 취소될 때까지 PollOnce 를 반복한다. (Kotlin ConsumerRunner)
//
// 실패하면 PollOnce 가 실패한 메시지부터 다시 받도록 되감아 두므로
// 같은 클라이언트로 그대로 계속 돌면 된다. 실패가 이어지면 기다리는 시간을
// retryDelay, 2배, 4배, ... MaxRetryDelay(기본 30초) 까지 늘린다 (지수 백오프).
// 성공하면 다시 retryDelay 부터.
//   - DB 가 잠깐 죽었다면(transient) 몇 초~수십 초 간격으로 계속 두드리다
//     살아나면 처리한다. 시도 횟수는 안 세므로 DLT 로 가지 않는다.
//   - poison message 는 DeadLetter 가 있으면 MaxAttempts 번째에 DLT 로
//     보내고 넘어가고, 없으면 여기서 무한히 재시도한다.
//
// 설정이 잘못됐으면(DeadLetter.Validate) 바로 에러를 돌려준다.
// 그 외에는 ctx 가 취소될 때 ctx.Err() 를 돌려준다.
func (l *ConsumerLoop) Run(ctx context.Context, retryDelay time.Duration) error {
	if l.DeadLetter != nil {
		if err := l.DeadLetter.Validate(); err != nil {
			return err
		}
	}
	if retryDelay <= 0 {
		return fmt.Errorf("retryDelay must be > 0, got %v", retryDelay)
	}
	maxDelay := l.MaxRetryDelay
	if maxDelay <= 0 {
		maxDelay = DefaultMaxRetryDelay
	}
	failures := 0          // 연속 실패 횟수
	for ctx.Err() == nil { // ctx 가 취소되면 Err() 가 nil 이 아니게 된다
		_, err := l.PollOnce(ctx)
		if err == nil || ctx.Err() != nil {
			failures = 0
			continue
		}
		failures++
		wait := retryBackoff(retryDelay, maxDelay, failures)
		log.Printf("처리 실패 (연속 %d번), %v 뒤 같은 메시지부터 재시도: %v",
			failures, wait, err)
		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}
	return ctx.Err()
}

// ---- 헬퍼 ----

func decode(rec *kgo.Record) (PaymentCompleted, error) {
	var ev PaymentCompleted
	if err := json.Unmarshal(rec.Value, &ev); err != nil {
		return ev, fmt.Errorf("decode payload: %w", err)
	}
	return ev, nil
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
