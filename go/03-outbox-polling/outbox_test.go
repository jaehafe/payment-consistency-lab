package outboxpolling

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// 헬퍼: 테이블 행 수 세기
func count(t *testing.T, sql string, args ...any) int { // args ...any = 가변 인자 (Kotlin의 vararg args: Any)
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// header 는 Kafka 레코드 헤더에서 key 에 해당하는 값을 찾는다.
func header(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func Test_pay__writes_payment_and_outbox_atomically(t *testing.T) {
	resetDB(t)
	svc := &PaymentService{DB: testPool, Gateway: NewFakePaymentGateway()}

	p, err := svc.Pay(context.Background(), "order-1", 10000)
	if err != nil {
		t.Fatal(err)
	}
	if n := count(t, `SELECT count(*) FROM payments WHERE id = $1`, p.ID); n != 1 {
		t.Fatalf("payments 1행 기대: %d", n)
	}
	// outbox 행이 같은 커밋으로 생겼고, payload 에 결제 정보가 들어 있다
	if n := count(t, `
		SELECT count(*) FROM outbox
		 WHERE aggregate_type = 'payment' AND aggregate_id = 'order-1' AND event_type = 'PaymentCompleted'
		   AND payload->>'paymentId' = $1 AND (payload->>'amount')::bigint = 10000
		   AND published_at IS NULL`, p.ID); n != 1 {
		t.Fatalf("outbox 1행 기대: %d", n)
	}
}

func Test_pay__db_failure__no_payment_no_outbox(t *testing.T) {
	resetDB(t)
	svc := &PaymentService{DB: testPool, Gateway: NewFakePaymentGateway(), FailBeforeCommit: true}

	if _, err := svc.Pay(context.Background(), "order-1", 10000); !errors.Is(err, ErrDBCommitFailed) {
		t.Fatalf("커밋 실패 에러 기대: %v", err)
	}
	// 01 의 dual write 와 달리 "둘 다 없음" → 불일치가 생기지 않는다
	if n := count(t, `SELECT count(*) FROM payments`); n != 0 {
		t.Fatalf("payments 0행 기대: %d", n)
	}
	if n := count(t, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatalf("outbox 0행 기대: %d", n)
	}
}

func Test_broker_down__payment_saved__event_delivered_after_recovery(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	topic := newTopic(t)
	publisher := &ToggleablePublisher{Inner: newKafkaPublisher(t, topic)}
	svc := &PaymentService{DB: testPool, Gateway: NewFakePaymentGateway()}
	relay := &OutboxRelay{DB: testPool, Publisher: publisher}

	publisher.SetDown(true) // 브로커 장애

	// 결제는 브로커와 무관하게 성공한다 (Kafka 를 직접 부르지 않으므로)
	if _, err := svc.Pay(ctx, "order-1", 10000); err != nil {
		t.Fatal(err)
	}
	if _, err := relay.RelayOnce(ctx, 10); !errors.Is(err, ErrBrokerDown) {
		t.Fatalf("릴레이는 브로커 장애로 실패해야 한다: %v", err)
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 1 {
		t.Fatalf("이벤트는 outbox 에 안전하게 남아 있어야 한다: %d", n)
	}
	if recs := consumeAll(t, topic); len(recs) != 0 {
		t.Fatalf("아직 Kafka 엔 없어야 한다: %d", len(recs))
	}

	publisher.SetDown(false) // 브로커 복구

	if n, err := relay.RelayOnce(ctx, 10); err != nil || n != 1 {
		t.Fatalf("복구 후 1건 발행 기대: n=%d err=%v", n, err)
	}
	recs := consumeAll(t, topic)
	if len(recs) != 1 || string(recs[0].Key) != "order-1" {
		t.Fatalf("복구 후 이벤트가 전달돼야 한다: %d건", len(recs))
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`); n != 1 {
		t.Fatalf("published_at 이 찍혀야 한다: %d", n)
	}
}

func Test_two_relays_concurrently__each_event_published_once(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	topic := newTopic(t)
	svc := &PaymentService{DB: testPool, Gateway: NewFakePaymentGateway()}

	const events = 30
	for i := 0; i < events; i++ {
		if _, err := svc.Pay(ctx, fmt.Sprintf("order-%02d", i), 1000); err != nil {
			t.Fatal(err)
		}
	}

	// 같은 브로커로 보내는 릴레이 2개. 발행마다 20ms 지연을 줘서 두 릴레이의 트랜잭션이 확실히 겹치게 한다.
	kp := newKafkaPublisher(t, topic)
	relays := []*OutboxRelay{
		{DB: testPool, Publisher: &ToggleablePublisher{Inner: kp, Delay: 20 * time.Millisecond}},
		{DB: testPool, Publisher: &ToggleablePublisher{Inner: kp, Delay: 20 * time.Millisecond}},
	}
	published := make([]int, len(relays))

	var wg sync.WaitGroup // goroutine 들이 끝날 때까지 기다리는 카운터
	for i, relay := range relays {
		wg.Add(1)
		go func() { // 각 릴레이를 별도 goroutine 에서 동시에 실행 (Kotlin의 launch { })
			defer wg.Done()
			for {
				n, err := relay.RelayOnce(ctx, 5)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return // 더 가져갈 게 없음 (남은 행은 전부 발행됐거나 상대가 잠금 중)
				}
				published[i] += n
			}
		}()
	}
	wg.Wait()
	t.Logf("relay1=%d건, relay2=%d건", published[0], published[1])

	recs := consumeAll(t, topic)
	seen := map[string]int{} // message_id -> 등장 횟수
	for _, r := range recs {
		seen[header(r.Headers, "message_id")]++
	}
	if len(recs) != events || len(seen) != events {
		t.Fatalf("이벤트 %d개가 정확히 한 번씩 발행돼야 한다: records=%d, distinct=%d", events, len(recs), len(seen))
	}
	if published[0] == 0 || published[1] == 0 {
		t.Fatalf("두 릴레이가 일을 나눠 가져야 한다 (SKIP LOCKED): %v", published)
	}
	if n := count(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("미발행 행이 남으면 안 된다: %d", n)
	}
}

func Test_crash_after_publish_before_mark__event_published_twice(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	topic := newTopic(t)
	svc := &PaymentService{DB: testPool, Gateway: NewFakePaymentGateway()}
	relay := &OutboxRelay{DB: testPool, Publisher: newKafkaPublisher(t, topic)}

	if _, err := svc.Pay(ctx, "order-1", 10000); err != nil {
		t.Fatal(err)
	}

	// 1회차: Kafka 발행은 성공, published_at 갱신 전에 "죽음" → tx 롤백
	relay.CrashAfterPublish.Store(true)
	if _, err := relay.RelayOnce(ctx, 10); !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("크래시 기대: %v", err)
	}
	// 2회차: 재시작한 릴레이는 published_at IS NULL 인 행을 보고 다시 보낸다
	relay.CrashAfterPublish.Store(false)
	if n, err := relay.RelayOnce(ctx, 10); err != nil || n != 1 {
		t.Fatalf("재발행 1건 기대: n=%d err=%v", n, err)
	}

	recs := consumeAll(t, topic)
	if len(recs) != 2 {
		t.Fatalf("같은 이벤트가 2번 발행돼야 한다 (at-least-once): %d건", len(recs))
	}
	id1, id2 := header(recs[0].Headers, "message_id"), header(recs[1].Headers, "message_id")
	if id1 == "" || id1 != id2 {
		t.Fatalf("두 메시지의 message_id 가 같아야 한다 → 컨슈머가 이걸로 중복 제거 (05 단계): %q vs %q", id1, id2)
	}
	t.Logf("중복 메시지 message_id=%s (2회 수신)", id1)
}
