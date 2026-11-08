package outboxcdc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PaymentService 는 03 과 똑같이 "한 트랜잭션에서 payments + outbox insert" 까지만 한다.
// 03 과 다른 점: 애플리케이션에 릴레이(폴링 루프)가 없다.
// Debezium 이 Postgres WAL(변경 로그)을 읽어서 outbox INSERT 를 감지하고 Kafka 로 보낸다.
type PaymentService struct {
	DB      *pgxpool.Pool
	Gateway *FakePaymentGateway
}

func (s *PaymentService) Pay(ctx context.Context, orderID string, amount int64) (Payment, error) {
	pgTxID, err := s.Gateway.Charge(ctx, "order-"+orderID, orderID, amount)
	if err != nil {
		return Payment{}, err
	}
	p := Payment{ID: uuid.NewString(), OrderID: orderID, Amount: amount, Status: "COMPLETED", PgTxID: pgTxID}
	payload, err := json.Marshal(PaymentCompleted{
		PaymentID: p.ID, OrderID: p.OrderID, Amount: p.Amount, PgTxID: p.PgTxID, OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return Payment{}, err
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Payment{}, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`INSERT INTO payments (id, order_id, amount, status, pg_tx_id) VALUES ($1, $2, $3, $4, $5)`,
		p.ID, p.OrderID, p.Amount, p.Status, p.PgTxID); err != nil {
		return Payment{}, err
	}
	// Debezium Event Router 가 이 행을 이렇게 바꿔서 보낸다 (outbox-connector.json 참고):
	//   aggregatetype='payment' → 토픽 "payment.events"  (route.topic.replacement = ${routedByValue}.events)
	//   aggregateid             → 메시지 key (orderId → 주문 단위 순서 보장)
	//   id                      → 헤더 message_id,  type → 헤더 event_type
	//   payload                 → 메시지 value (JSON 그대로)
	// WAL 에는 "커밋된" 변경만 나타나므로, 롤백된 결제의 이벤트는 절대 나가지 않는다.
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload) VALUES ($1, 'payment', $2, 'PaymentCompleted', $3)`,
		uuid.NewString(), p.OrderID, payload); err != nil {
		return Payment{}, err
	}

	// (선택) 운영에서는 outbox 가 무한히 커지지 않도록, INSERT 직후 같은 tx 에서 DELETE 하는 패턴도 쓴다.
	// Debezium 은 WAL 의 INSERT 이벤트만 보면 되므로 행이 바로 지워져도 발행된다. (README 의 "outbox 정리" 참고)

	if err := tx.Commit(ctx); err != nil {
		return Payment{}, err
	}
	return p, nil
}
