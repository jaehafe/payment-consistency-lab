package dualwrite

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

// EventPublisher 는 이벤트를 브로커로 보내는 인터페이스.
//
// Go 인터페이스는 "메서드 목록"일 뿐이고, 구현체가 implements 를 선언하지 않는다.
// 같은 시그니처의 메서드만 가지고 있으면 자동으로 이 인터페이스를 만족한다 (덕 타이핑의 정적 버전).
// Kotlin으로 치면 interface EventPublisher { fun publish(...) } 인데 `: EventPublisher` 를 안 써도 되는 것.
type EventPublisher interface {
	Publish(ctx context.Context, key, messageID string, payload []byte) error
}

// KafkaPublisher 는 franz-go 클라이언트로 동기 발행한다.
type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
}

// Publish 는 브로커가 받았다고 응답(acks=all)할 때까지 기다린다.
func (p *KafkaPublisher) Publish(ctx context.Context, key, messageID string, payload []byte) error {
	// &kgo.Record{...} : 구조체를 만들고 그 주소(포인터)를 얻는다. Kotlin의 Record(...) 생성과 비슷.
	rec := &kgo.Record{
		Topic:   p.Topic,
		Key:     []byte(key), // 메시지 key = orderId → 같은 주문의 이벤트는 같은 파티션으로 가서 순서가 보장된다.
		Value:   payload,
		Headers: []kgo.RecordHeader{{Key: "message_id", Value: []byte(messageID)}},
	}
	// ProduceSync 는 결과 묶음을 돌려주고, FirstErr() 로 첫 에러를 꺼낸다.
	return p.Client.ProduceSync(ctx, rec).FirstErr()
}

// ErrBrokerDown 은 실패 주입용 에러.
var ErrBrokerDown = errors.New("broker unavailable (simulated)")

// ToggleablePublisher 는 진짜 Publisher 를 감싸서 "브로커 장애"를 켰다 껐다 할 수 있게 한다.
// 실제 Redpanda 컨테이너를 쓰되, 장애 상황만 스위치로 흉내 낸다.
type ToggleablePublisher struct {
	Inner EventPublisher // 인터페이스 타입 필드: KafkaPublisher 든 뭐든 들어갈 수 있다.
	down  atomic.Bool    // 여러 goroutine 에서 안전하게 읽고 쓰는 bool (Kotlin의 AtomicBoolean)
}

func (p *ToggleablePublisher) SetDown(down bool) { p.down.Store(down) }

func (p *ToggleablePublisher) Publish(ctx context.Context, key, messageID string, payload []byte) error {
	if p.down.Load() {
		return ErrBrokerDown
	}
	return p.Inner.Publish(ctx, key, messageID, payload)
}
