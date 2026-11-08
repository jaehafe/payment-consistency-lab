package outboxpolling

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrDBCommitFailed 는 커밋 직전 DB 실패를 흉내 내는 에러.
var ErrDBCommitFailed = errors.New("db commit failed (simulated)")

// PaymentService 는 결제를 저장하면서 "보낼 이벤트"를 같은 DB 의 outbox 테이블에 함께 적는다.
// Kafka 로는 직접 보내지 않는다 → 발행은 OutboxRelay 의 몫.
type PaymentService struct {
	DB      *pgxpool.Pool
	Gateway *FakePaymentGateway

	FailBeforeCommit bool // 실패 주입: true 면 커밋 대신 롤백
}

// Pay : PG 승인 → tx { payments insert + outbox insert } 커밋.
func (s *PaymentService) Pay(ctx context.Context, orderID string, amount int64) (Payment, error) {
	// PG 호출은 트랜잭션 밖에서 (외부 호출 동안 DB 커넥션을 쥐고 있지 않기 위해. 자세한 이유는 02 단계 참고)
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

	// ---- 트랜잭션 경계 시작 ----
	// payments 와 outbox 는 같은 DB 이므로 하나의 로컬 트랜잭션으로 묶을 수 있다.
	// → "결제는 있는데 이벤트가 없음" / "이벤트는 있는데 결제가 없음" 이 원천적으로 불가능해진다.
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Payment{}, err
	}
	defer tx.Rollback(ctx) // 중간에 return 하면 롤백. Commit 성공 후에는 아무 일도 안 한다.

	if _, err := tx.Exec(ctx,
		`INSERT INTO payments (id, order_id, amount, status, pg_tx_id) VALUES ($1, $2, $3, $4, $5)`,
		p.ID, p.OrderID, p.Amount, p.Status, p.PgTxID); err != nil {
		return Payment{}, err
	}
	// outbox.id 는 Kafka 헤더 message_id 로 쓰인다. 재발행돼도 같은 id → 컨슈머가 중복을 걸러낼 수 있다 (05 단계).
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload) VALUES ($1, 'payment', $2, 'PaymentCompleted', $3)`,
		uuid.NewString(), p.OrderID, payload); err != nil {
		return Payment{}, err
	}

	if s.FailBeforeCommit {
		return Payment{}, ErrDBCommitFailed // defer 된 Rollback 이 두 INSERT 를 함께 되돌린다
	}
	if err := tx.Commit(ctx); err != nil {
		return Payment{}, err
	}
	// ---- 트랜잭션 경계 끝 ----
	return p, nil
}
