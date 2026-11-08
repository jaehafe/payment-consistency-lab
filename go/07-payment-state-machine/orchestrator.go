package statemachine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// InventoryClient 는 재고 서비스. 결제 승인 "이후" 단계의 예.
type InventoryClient interface {
	Reserve(ctx context.Context, orderID, sku string, qty int) error
}

// FakeInventory 는 실패 주입이 가능한 재고 서비스.
type FakeInventory struct {
	Fail atomic.Bool
}

var ErrOutOfStock = errors.New("inventory: out of stock (simulated)")

func (f *FakeInventory) Reserve(ctx context.Context, orderID, sku string, qty int) error {
	if f.Fail.Load() {
		return ErrOutOfStock
	}
	return nil
}

// Order 는 체크아웃 입력.
type Order struct {
	ID     string
	Amount int64
	SKU    string
	Qty    int
}

var (
	ErrPaymentNotApproved  = errors.New("payment not approved")
	ErrCheckoutCompensated = errors.New("checkout failed; payment canceled (compensated)")
	ErrCompensationPending = errors.New("checkout failed; pg cancel failed, left CANCEL_REQUESTED for janitor")
)

// CheckoutOrchestrator 는 "결제 승인 → 재고 확보" 를 순서대로 실행하고,
// 뒤 단계가 실패하면 앞 단계를 되돌리는 보상(compensation)을 실행한다 (Saga 오케스트레이션의 가장 작은 형태).
//
// 왜 보상인가? 결제 승인(PG)과 재고(다른 서비스)는 하나의 DB 트랜잭션으로 묶을 수 없다.
// 그래서 "롤백" 대신 "반대 동작(PG 취소)" 을 명시적으로 실행한다.
type CheckoutOrchestrator struct {
	Payments  *PaymentService
	Inventory InventoryClient

	// CancelRetryDelay: 보상 취소를 janitor 가 처음 재시도하기까지의
	// 지연 (Kotlin 은 lab.janitor.base-backoff). 0 이면 10초.
	CancelRetryDelay time.Duration
}

func (o *CheckoutOrchestrator) cancelRetryDelay() time.Duration {
	if o.CancelRetryDelay <= 0 {
		return 10 * time.Second
	}
	return o.CancelRetryDelay
}

func (o *CheckoutOrchestrator) Checkout(ctx context.Context, order Order) (Payment, error) {
	p, err := o.Payments.Pay(ctx, order.ID, order.Amount)
	if err != nil {
		return Payment{}, err
	}
	if p.Status != Approved {
		// FAILED: 할 일 없음. UNKNOWN: janitor 가 확정할 때까지 다음 단계로 가지 않는다.
		return p, fmt.Errorf("%w: %s", ErrPaymentNotApproved, p.Status)
	}

	reserveErr := o.Inventory.Reserve(ctx, order.ID, order.SKU, order.Qty)
	if reserveErr == nil {
		return p, nil
	}

	// ---- 보상 시작 ----
	repo := o.Payments.Repo
	// 1) PG 취소를 "부르기 전에" CANCEL_REQUESTED 로 기록한다.
	//    취소 호출 중 프로세스가 죽어도, 이 기록이 남아 있으면 janitor 가 이어서 취소한다. (기록 먼저, 외부 호출 나중)
	//    next_retry_at 은 "지금 + 지연": 지금 이 요청이 취소를 부르는
	//    중인데 janitor 가 바로 같은 건을 집어 가 경쟁하지 않도록.
	err = withTx(ctx, repo.DB, func(tx pgx.Tx) error {
		if err := TransitionTx(ctx, tx, p.ID, Approved, CancelRequested, "compensate: "+reserveErr.Error()); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			UPDATE payments
			   SET attempt_count = 0, next_retry_at = $3,
			       last_error = $2
			 WHERE id = $1`,
			p.ID, reserveErr.Error(),
			time.Now().Add(o.cancelRetryDelay()))
		return err
	})
	if err != nil {
		return p, err
	}

	// 2) PG 취소
	if cancelErr := o.Payments.Gateway.Cancel(ctx, p.PgTxID); cancelErr != nil {
		// 취소 1회 실패 = 시도 1회. janitor 는 지연 뒤부터 백오프로 이어 간다.
		// status 조건(CAS)을 걸어 그 사이 상태가 바뀌었으면 건드리지 않는다.
		_, _ = repo.DB.Exec(ctx, `
			UPDATE payments
			   SET attempt_count = 1, next_retry_at = $3,
			       last_error = $2
			 WHERE id = $1 AND status = $4`,
			p.ID, cancelErr.Error(),
			time.Now().Add(o.cancelRetryDelay()),
			string(CancelRequested))
		p, _ = repo.Get(ctx, p.ID)
		return p, fmt.Errorf("%w: %v", ErrCompensationPending, cancelErr)
	}

	// 3) 취소 성공 → CANCELED
	if err := repo.Transition(ctx, p.ID, CancelRequested, Canceled, "compensated: pg cancel succeeded"); err != nil {
		return p, err
	}
	p, _ = repo.Get(ctx, p.ID)
	return p, fmt.Errorf("%w: %v", ErrCheckoutCompensated, reserveErr)
}
