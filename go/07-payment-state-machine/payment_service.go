package statemachine

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PaymentService 는 결제 승인을 상태 머신으로 진행한다.
type PaymentService struct {
	Repo    *Repository
	Gateway *FakePaymentGateway

	// CancelRetryDelay: 늦은 승인을 망취소하다 실패했을 때 janitor 가
	// 처음 재시도하기까지의 지연 (오케스트레이터와 같은 의미). 0 이면 10초.
	CancelRetryDelay time.Duration
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

	// 4) 결과 기록
	return s.recordChargeResult(ctx, id, pgTxID, chargeErr)
}

// recordChargeResult 는 PG 응답을 PENDING → (APPROVED | FAILED | UNKNOWN) 으로 기록한다.
// 전이와 부가 컬럼 갱신을 한 트랜잭션으로 묶는다.
func (s *PaymentService) recordChargeResult(ctx context.Context, id, pgTxID string, chargeErr error) (Payment, error) {
	err := withTx(ctx, s.Repo.DB, func(tx pgx.Tx) error {
		switch {
		case chargeErr == nil:
			return approveTx(ctx, tx, id, Pending, pgTxID, "pg approved")

		case errors.Is(chargeErr, ErrPgDeclined):
			// PG 가 "거절"이라고 명확히 답했다 → 확정 실패
			return failTx(ctx, tx, id, Pending, chargeErr.Error())

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
	if errors.Is(err, ErrStaleStatus) {
		// PG 응답이 PendingAfter 보다 늦게 와서 janitor 가 PENDING 을
		// 먼저 정리했다. janitor 가 본 PG 기록은 "그때" 의 것이라
		// 요청이 지금 아는 결과와 다를 수 있다 → 맞춰 본다.
		return s.reconcileAfterStale(ctx, id, pgTxID, chargeErr)
	}
	if err != nil {
		return Payment{}, err
	}
	return s.Repo.Get(ctx, id)
}

// maxStaleRereads: 맞추는 전이도 janitor 와 또 부딪힐 수 있다 (UNKNOWN 을
// ReconcileUnknown 이 동시에 확정하는 경우). 다시 읽고 다시 판단하되, 무한히
// 돌지 않게 상한을 둔다. 상태는 앞으로만 가므로(종결 상태가 있다) 몇 번이면 끝난다.
const maxStaleRereads = 3

// reconcileAfterStale 은 janitor 가 먼저 정한 상태와 요청이 PG 에서 받은 결과를 맞춘다.
//
//	요청이 아는 것   저장된 상태          처리
//	승인(pgTxID)    APPROVED            그대로 (같은 결론)
//	승인(pgTxID)    UNKNOWN             UNKNOWN → APPROVED (+pg_tx_id)
//	승인(pgTxID)    FAILED              ★ 늦은 승인 → 망취소: FAILED → CANCEL_REQUESTED → PG 취소
//	거절            UNKNOWN             UNKNOWN → FAILED
//	거절            FAILED              그대로
//	모름(타임아웃)    아무거나             그대로 (janitor 가 더 많이 안다)
//	그 외 (MANUAL_REVIEW, CANCEL_*, 모순)  그대로 + 로그
func (s *PaymentService) reconcileAfterStale(ctx context.Context, id, pgTxID string, chargeErr error) (Payment, error) {
	approved := chargeErr == nil
	declined := errors.Is(chargeErr, ErrPgDeclined)
	for range maxStaleRereads {
		p, err := s.Repo.Get(ctx, id)
		if err != nil {
			return Payment{}, err
		}
		switch {
		case !approved && !declined:
			return p, nil

		case approved && p.Status == Unknown:
			err = withTx(ctx, s.Repo.DB, func(tx pgx.Tx) error {
				return approveTx(ctx, tx, id, Unknown, pgTxID,
					"pg approved (janitor had moved it to UNKNOWN)")
			})

		case declined && p.Status == Unknown:
			err = withTx(ctx, s.Repo.DB, func(tx pgx.Tx) error {
				return failTx(ctx, tx, id, Unknown, chargeErr.Error())
			})

		case approved && p.Status == Failed:
			return s.netCancel(ctx, id, pgTxID)

		default:
			if (approved && p.Status != Approved) || (declined && p.Status != Failed) {
				// MANUAL_REVIEW(사람 몫) 이거나 PG 와 모순되는 기록. 자동으로 고치지 않고 남긴다.
				log.Printf("WARN payment %s: request learned pg=%s (tx=%q) but stored status is %s; leaving it",
					id, chargeResult(chargeErr), pgTxID, p.Status)
			}
			return p, nil
		}
		if errors.Is(err, ErrStaleStatus) {
			continue // 그 사이 janitor 가 또 바꿨다 → 다시 읽는다
		}
		if err != nil {
			return Payment{}, err
		}
		return s.Repo.Get(ctx, id)
	}
	log.Printf("WARN payment %s: still racing after %d re-reads; returning stored status", id, maxStaleRereads)
	return s.Repo.Get(ctx, id)
}

// netCancel 은 "janitor 가 FAILED 로 확정한 뒤 PG 승인이 도착한" 결제를 취소한다.
//
// FAILED 는 이미 밖으로 나간 결론이다 (고객은 재결제했을 수 있다). 그래서
// FAILED → APPROVED 로 되살리지 않고, 돈을 돌려주는 쪽(취소)으로 간다.
// PG 연동에서 말하는 망취소(net-cancel): 승인은 됐는데 가맹점이 그 결과를
// 반영하지 못했으면 승인을 바로 취소한다.
//
// 주문 보상과 같은 순서: CANCEL_REQUESTED 를 먼저 커밋 → PG 취소. 취소가
// 실패하거나 그 사이 죽어도 CANCEL_REQUESTED 가 남아 janitor 가 재시도한다.
// "돈은 빠졌는데 FAILED, 후속 조치 없음" 이 되지 않는다.
func (s *PaymentService) netCancel(ctx context.Context, id, pgTxID string) (Payment, error) {
	log.Printf("ERROR payment %s: pg approved (tx=%s) after janitor recorded FAILED; net-cancelling", id, pgTxID)
	reason := "late pg approval after FAILED: net-cancel"
	err := withTx(ctx, s.Repo.DB, func(tx pgx.Tx) error {
		if err := TransitionTx(ctx, tx, id, Failed, CancelRequested, reason); err != nil {
			return err
		}
		// next_retry_at = 지금 + 지연: 이 요청이 바로 취소를 부르는 동안
		// janitor 가 같은 건을 집어 경쟁하지 않게 (오케스트레이터 보상과 같다)
		_, err := tx.Exec(ctx, `
			UPDATE payments
			   SET pg_tx_id = $2, attempt_count = 0, next_retry_at = $3, last_error = $4
			 WHERE id = $1`,
			id, pgTxID, time.Now().Add(s.cancelRetryDelay()), reason)
		return err
	})
	if err != nil {
		return Payment{}, err
	}

	if cancelErr := s.Gateway.Cancel(ctx, pgTxID); cancelErr != nil {
		log.Printf("ERROR payment %s: net-cancel failed, left CANCEL_REQUESTED for janitor: %v", id, cancelErr)
		if _, err := s.Repo.DB.Exec(ctx, `
			UPDATE payments
			   SET attempt_count = 1, next_retry_at = $3, last_error = $2
			 WHERE id = $1 AND status = $4`,
			id, cancelErr.Error(), time.Now().Add(s.cancelRetryDelay()),
			string(CancelRequested)); err != nil {
			return Payment{}, err
		}
		return s.Repo.Get(ctx, id)
	}
	err = s.Repo.Transition(ctx, id, CancelRequested, Canceled, "net-cancel: pg cancel succeeded")
	if err != nil && !errors.Is(err, ErrStaleStatus) { // Stale: janitor 가 먼저 CANCELED 로
		return Payment{}, err
	}
	return s.Repo.Get(ctx, id)
}

func (s *PaymentService) cancelRetryDelay() time.Duration {
	if s.CancelRetryDelay <= 0 {
		return 10 * time.Second
	}
	return s.CancelRetryDelay
}

// approveTx: from → APPROVED + pg_tx_id (UNKNOWN 재시도 예약은 지운다).
func approveTx(ctx context.Context, tx pgx.Tx, id string, from Status, pgTxID, reason string) error {
	if err := TransitionTx(ctx, tx, id, from, Approved, reason); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE payments SET pg_tx_id = $2, next_retry_at = NULL WHERE id = $1`, id, pgTxID)
	return err
}

// failTx: from → FAILED + last_error (UNKNOWN 재시도 예약은 지운다).
func failTx(ctx context.Context, tx pgx.Tx, id string, from Status, reason string) error {
	if err := TransitionTx(ctx, tx, id, from, Failed, reason); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE payments SET last_error = $2, next_retry_at = NULL WHERE id = $1`, id, reason)
	return err
}

func chargeResult(chargeErr error) string {
	switch {
	case chargeErr == nil:
		return "approved"
	case errors.Is(chargeErr, ErrPgDeclined):
		return "declined"
	default:
		return "unknown"
	}
}
