package dualwrite

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// 테스트 함수 이름은 docs/SPEC.md 의 Kotlin 테스트 이름과 맞췄다 (Test_ 접두사만 추가).
// Go 테스트는 func TestXxx(t *testing.T) 형태면 자동으로 발견된다. t 는 JUnit의 Assertions + 테스트 컨텍스트 역할.

func Test_commit_then_publish__broker_failure__payment_saved_but_event_lost(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	topic := newTopic(t)
	publisher := &ToggleablePublisher{Inner: newKafkaPublisher(t, topic)}
	svc := &DualWritePaymentService{DB: testPool, Gateway: NewFakePaymentGateway(), Publisher: publisher}

	publisher.SetDown(true) // 브로커 장애 발생

	_, err := svc.CommitThenPublish(ctx, "order-1", 10000)
	if !errors.Is(err, ErrBrokerDown) {
		t.Fatalf("브로커 장애 에러를 기대했지만 %v", err)
	}

	// DB 에는 결제가 커밋되어 있다
	if _, found, err := FindPaymentByOrderID(ctx, testPool, "order-1"); err != nil || !found {
		t.Fatalf("결제가 저장돼 있어야 한다 (found=%v, err=%v)", found, err)
	}
	// 그런데 Kafka 에는 이벤트가 한 건도 없다 → 다운스트림(배송/포인트)은 이 결제를 영원히 모른다
	if recs := consumeAll(t, topic); len(recs) != 0 {
		t.Fatalf("이벤트가 없어야 하는데 %d건 있음", len(recs))
	}
}

func Test_publish_then_commit__db_failure__ghost_event_published(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	topic := newTopic(t)
	svc := &DualWritePaymentService{DB: testPool, Gateway: NewFakePaymentGateway(), Publisher: newKafkaPublisher(t, topic)}

	svc.FailBeforeCommit = true // DB 커밋 실패 발생

	_, err := svc.PublishThenCommit(ctx, "order-2", 10000)
	if !errors.Is(err, ErrDBCommitFailed) {
		t.Fatalf("DB 커밋 실패 에러를 기대했지만 %v", err)
	}

	// DB 에는 결제가 없다
	if _, found, _ := FindPaymentByOrderID(ctx, testPool, "order-2"); found {
		t.Fatal("결제가 없어야 한다")
	}
	// 그런데 Kafka 에는 "결제 완료" 이벤트가 이미 있다 → 유령 이벤트
	recs := consumeAll(t, topic)
	if len(recs) != 1 {
		t.Fatalf("유령 이벤트 1건을 기대했지만 %d건", len(recs))
	}
	var ev PaymentCompleted
	if err := json.Unmarshal(recs[0].Value, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.OrderID != "order-2" || string(recs[0].Key) != "order-2" {
		t.Fatalf("예상과 다른 이벤트: %+v", ev)
	}
	t.Logf("유령 이벤트: paymentId=%s 인 결제는 DB 어디에도 없다", ev.PaymentID)
}

func Test_publish_inside_tx__rollback__event_already_sent(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	topic := newTopic(t)
	svc := &DualWritePaymentService{DB: testPool, Gateway: NewFakePaymentGateway(), Publisher: newKafkaPublisher(t, topic)}

	svc.FailBeforeCommit = true // 발행 이후 트랜잭션이 롤백되는 상황

	if _, err := svc.PublishInsideTx(ctx, "order-3", 10000); !errors.Is(err, ErrDBCommitFailed) {
		t.Fatalf("롤백 에러를 기대했지만 %v", err)
	}

	// 트랜잭션은 롤백되어 결제가 없다
	if _, found, _ := FindPaymentByOrderID(ctx, testPool, "order-3"); found {
		t.Fatal("롤백됐으므로 결제가 없어야 한다")
	}
	// 하지만 "트랜잭션 안에서" 보낸 메시지는 롤백과 무관하게 이미 브로커에 있다
	if recs := consumeAll(t, topic); len(recs) != 1 {
		t.Fatalf("이미 발행된 이벤트 1건을 기대했지만 %d건", len(recs))
	}
}
