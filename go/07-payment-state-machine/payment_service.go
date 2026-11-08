package statemachine

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PaymentService 는 결제 승인을 상태 머신으로 진행한다.
type PaymentService struct {
	Repo    *Repository
	Gateway *FakePaymentGateway
}

// Pay 는 REQUESTED → PENDING → (APPROVED | FAILED | UNKNOWN) 으로 진행한다.
// 반환 error 는 DB 장애 같은 "시스템" 에러만. 결제 결과(승인/거절/모름)는 Payment.Status 로 표현한다.
func (s *PaymentService) Pay(ctx context.Context, orderID string, amount int64) (Payment, error) {
	id := uuid.NewString()
	key := "pay-" + id // PG 에 보낼 멱등키. 재시도/조회 모두 이 key 로 한다.

	// 1) REQUESTED 로 생성 (+ 최초 이력)
	err := withTx(ctx, s.Repo.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO payments (id, order_id, amount, status, idempotency_key) VALUES ($1, $2, $3, $4, $5)`,
			id, orderID, amount, string(Requested), key); err != nil {
			return err
		}
		return insertHistory(ctx, tx, id, nil, Requested, "created")
	})
	if err != nil {
		return Payment{}, err
	}

	// 2) PG 호출 직전에 PENDING 으로. "PG 에 뭔가 보냈을 수도 있다" 는 사실을 먼저 기록해 둔다.
	if err := s.Repo.Transition(ctx, id, Requested, Pending, "calling pg"); err != nil {
		return Payment{}, err
	}

	// 3) PG 호출 (트랜잭션 밖: 외부 호출 동안 DB 잠금/커넥션을 쥐지 않는다)
	pgTxID, chargeErr := s.Gateway.Charge(ctx, key, orderID, amount)

	// 4) 결과에 따라 전이. 전이와 부가 컬럼 갱신을 한 트랜잭션으로 묶는다.
	err = withTx(ctx, s.Repo.DB, func(tx pgx.Tx) error {
		switch {
		case chargeErr == nil:
			if err := TransitionTx(ctx, tx, id, Pending, Approved, "pg approved"); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE payments SET pg_tx_id = $2 WHERE id = $1`, id, pgTxID)
			return err

		case errors.Is(chargeErr, ErrPgDeclined):
			// PG 가 "거절"이라고 명확히 답했다 → 확정 실패
			if err := TransitionTx(ctx, tx, id, Pending, Failed, chargeErr.Error()); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE payments SET last_error = $2 WHERE id = $1`, id, chargeErr.Error())
			return err

		default:
			// ★ 타임아웃 등 "결과를 모르는" 에러 → FAILED 가 아니라 UNKNOWN.
			// 실패로 단정하고 사용자에게 재결제를 유도하면, 실제로는 승인됐던 첫 결제와 합쳐 이중 결제가 된다.
			// UNKNOWN 으로 두고 janitor 가 PG 조회 API 로 확인한다.
			if err := TransitionTx(ctx, tx, id, Pending, Unknown, chargeErr.Error()); err != nil {
				return err
			}
			_, err := tx.Exec(ctx,
				`UPDATE payments SET last_error = $2, attempt_count = 0, next_retry_at = now() WHERE id = $1`,
				id, chargeErr.Error())
			return err
		}
	})
	if err != nil {
		return Payment{}, err
	}
	return s.Repo.Get(ctx, id)
}
