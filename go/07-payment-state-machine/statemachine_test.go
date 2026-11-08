package statemachine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

// fixture 는 테스트 하나에 필요한 객체들.
type fixture struct {
	repo    *Repository
	gateway *FakePaymentGateway
	svc     *PaymentService
	janitor *Janitor
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	exec(t, `TRUNCATE payments, payment_status_history`)
	repo := &Repository{DB: testPool}
	gw := NewFakePaymentGateway()
	return fixture{
		repo:    repo,
		gateway: gw,
		svc:     &PaymentService{Repo: repo, Gateway: gw},
		janitor: &Janitor{DB: testPool, Gateway: gw, MaxAttempts: 3, BaseBackoff: time.Minute, NotFoundGrace: 5 * time.Minute},
	}
}

func (f fixture) mustGet(t *testing.T, id string) Payment {
	t.Helper()
	p, err := f.repo.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// insertPayment 는 원하는 상태의 결제를 바로 만든다 (전이 테스트용).
func insertPayment(t *testing.T, id string, status Status) {
	t.Helper()
	exec(t, `INSERT INTO payments (id, order_id, amount, status, idempotency_key) VALUES ($1::uuid, $1::text, 10000, $2, 'key-' || $1::text)`,
		id, string(status))
}

func Test_invalid_transition__rejected(t *testing.T) {
	f := newFixture(t)
	const id = "00000000-0000-0000-0000-000000000001"
	insertPayment(t, id, Requested)

	// REQUESTED 에서 PG 호출도 없이 APPROVED 로 건너뛸 수 없다
	err := f.repo.Transition(context.Background(), id, Requested, Approved, "skip")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("ErrInvalidTransition 기대: %v", err)
	}
	// 최종 상태(FAILED)에서 APPROVED 로 되살릴 수 없다
	if err := f.repo.Transition(context.Background(), id, Failed, Approved, "revive"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("ErrInvalidTransition 기대: %v", err)
	}
	if p := f.mustGet(t, id); p.Status != Requested {
		t.Fatalf("상태는 그대로여야 한다: %s", p.Status)
	}
	if n := queryInt(t, `SELECT count(*) FROM payment_status_history`); n != 0 {
		t.Fatalf("거부된 전이는 이력도 없다: %d", n)
	}
}

func Test_every_transition_recorded_in_history(t *testing.T) {
	f := newFixture(t)
	p, err := f.svc.Pay(context.Background(), "order-1", 10000)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != Approved || p.PgTxID == "" {
		t.Fatalf("승인 기대: %+v", p)
	}
	hist, err := f.repo.History(context.Background(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []HistoryEntry{
		{From: "", To: Requested},
		{From: Requested, To: Pending},
		{From: Pending, To: Approved},
	}
	// reason 은 비교에서 빼고 from/to 만 본다
	got := make([]HistoryEntry, len(hist))
	for i, h := range hist {
		got[i] = HistoryEntry{From: h.From, To: h.To}
	}
	if !slices.Equal(got, want) { // 구조체 슬라이스 비교 (Kotlin의 list == list)
		t.Fatalf("이력이 다르다:\n got=%v\nwant=%v", got, want)
	}
}

func Test_concurrent_transition__only_one_wins(t *testing.T) {
	f := newFixture(t)
	const id = "00000000-0000-0000-0000-000000000002"
	insertPayment(t, id, Pending)

	// 10개 goroutine 이 동시에 PENDING 에서 전이를 시도: 절반은 APPROVED(PG 콜백), 절반은 UNKNOWN(타임아웃 처리기)
	const n = 10
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			to := Approved
			if i%2 == 1 {
				to = Unknown
			}
			<-start
			errs[i] = f.repo.Transition(context.Background(), id, Pending, to, fmt.Sprintf("worker-%d", i))
		}()
	}
	close(start)
	wg.Wait()

	wins, stale := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrStaleStatus):
			stale++
		default:
			t.Fatalf("예상 못한 에러: %v", err)
		}
	}
	if wins != 1 || stale != n-1 {
		t.Fatalf("compare-and-set 으로 1개만 이겨야 한다: wins=%d stale=%d", wins, stale)
	}
	if h := queryInt(t, `SELECT count(*) FROM payment_status_history WHERE payment_id = $1`, id); h != 1 {
		t.Fatalf("이력도 1줄: %d", h)
	}
}

func Test_pg_timeout_after_charge__unknown__janitor_confirms_approved(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	f.gateway.TimeoutAfterCharge.Store(true) // PG 는 승인했지만 응답 유실
	p, err := f.svc.Pay(ctx, "order-1", 10000)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != Unknown {
		t.Fatalf("타임아웃은 FAILED 가 아니라 UNKNOWN: %s", p.Status)
	}
	if f.gateway.ChargeCount() != 1 {
		t.Fatal("PG 에서는 실제로 승인됐다")
	}

	f.gateway.TimeoutAfterCharge.Store(false)
	stats, err := f.janitor.ReconcileUnknown(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	p = f.mustGet(t, p.ID)
	if stats.Approved != 1 || p.Status != Approved || p.PgTxID == "" {
		t.Fatalf("janitor 가 PG 조회로 APPROVED 확정해야 한다: stats=%+v payment=%+v", stats, p)
	}
	if f.gateway.ChargeCount() != 1 {
		t.Fatal("재결제 없이 확인만 했어야 한다")
	}
}

func Test_pg_timeout_before_charge__unknown__janitor_marks_failed(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	f.gateway.TimeoutBeforeCharge.Store(true) // 요청이 PG 에 도달하지 못함
	p, err := f.svc.Pay(ctx, "order-1", 10000)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != Unknown {
		t.Fatalf("호출자 입장에선 똑같이 타임아웃 → UNKNOWN: %s", p.Status)
	}

	// 1회차 (직후): PG 에 기록이 없지만 아직 grace 기간 → 결론 보류, 재시도 예약
	stats, err := f.janitor.ReconcileUnknown(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rescheduled != 1 || f.mustGet(t, p.ID).Status != Unknown {
		t.Fatalf("grace 기간 내 NOT_FOUND 는 보류: %+v", stats)
	}

	// 2회차 (10분 뒤): 여전히 NOT_FOUND → 요청이 도달하지 않은 것으로 확정 → FAILED
	stats, err = f.janitor.ReconcileUnknown(ctx, time.Now().Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failed != 1 || f.mustGet(t, p.ID).Status != Failed {
		t.Fatalf("grace 이후 NOT_FOUND → FAILED: %+v", stats)
	}
	if f.gateway.ChargeCount() != 0 {
		t.Fatal("돈은 빠지지 않았다")
	}
}

func Test_unknown_exceeds_max_attempts__moved_to_manual_review(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	f.gateway.TimeoutAfterCharge.Store(true)
	p, err := f.svc.Pay(ctx, "order-1", 10000)
	if err != nil {
		t.Fatal(err)
	}
	f.gateway.FailStatusQuery.Store(true) // PG 조회 API 까지 장애 → 계속 결론을 못 냄

	now := time.Now()
	// MaxAttempts=3, BaseBackoff=1분 → 1차 후 +1분, 2차 후 +2분, 3차에 포기
	for attempt := 1; attempt <= 3; attempt++ {
		now = now.Add(time.Hour) // 재시도 시각을 확실히 지나도록 시간을 넉넉히 진행
		if _, err := f.janitor.ReconcileUnknown(ctx, now); err != nil {
			t.Fatal(err)
		}
		got := f.mustGet(t, p.ID)
		if got.AttemptCount != attempt {
			t.Fatalf("시도 횟수 %d 기대: %d", attempt, got.AttemptCount)
		}
		if attempt < 3 {
			// 지수 백오프 확인: next_retry_at = now + 1분 × 2^(attempt-1)
			wantNext := now.Add(time.Minute << (attempt - 1))
			var next time.Time
			if err := testPool.QueryRow(ctx, `SELECT next_retry_at FROM payments WHERE id = $1`, p.ID).Scan(&next); err != nil {
				t.Fatal(err)
			}
			if next.Sub(wantNext).Abs() > time.Millisecond {
				t.Fatalf("%d차 후 다음 재시도 %v 기대: %v", attempt, wantNext, next)
			}
		}
	}
	got := f.mustGet(t, p.ID)
	if got.Status != ManualReview {
		t.Fatalf("최대 시도 초과 → MANUAL_REVIEW: %s", got.Status)
	}
	// MANUAL_REVIEW 는 janitor 가 더 이상 건드리지 않는다
	stats, _ := f.janitor.ReconcileUnknown(ctx, now.Add(24*time.Hour))
	if stats != (JanitorStats{}) {
		t.Fatalf("보류 건은 자동 처리 대상이 아니다: %+v", stats)
	}
}

func Test_downstream_failure__compensated_by_pg_cancel(t *testing.T) {
	f := newFixture(t)
	inv := &FakeInventory{}
	inv.Fail.Store(true) // 결제 승인 후 재고 확보 실패
	orch := &CheckoutOrchestrator{Payments: f.svc, Inventory: inv}

	p, err := orch.Checkout(context.Background(), Order{ID: "order-1", Amount: 10000, SKU: "sku-1", Qty: 1})
	if !errors.Is(err, ErrCheckoutCompensated) {
		t.Fatalf("보상 완료 에러 기대: %v", err)
	}
	if p.Status != Canceled {
		t.Fatalf("CANCELED 기대: %s", p.Status)
	}
	if !f.gateway.IsCanceled(p.PgTxID) || f.gateway.CancelCount() != 1 {
		t.Fatal("PG 에서 실제로 취소돼야 한다")
	}
	hist, _ := f.repo.History(context.Background(), p.ID)
	last2 := hist[len(hist)-2:] // 슬라이스의 마지막 2개 (Kotlin의 takeLast(2))
	if last2[0].To != CancelRequested || last2[1].To != Canceled {
		t.Fatalf("APPROVED → CANCEL_REQUESTED → CANCELED 이력 기대: %v", hist)
	}
}

func Test_cancel_failure__left_cancel_requested__janitor_retries_cancel(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	inv := &FakeInventory{}
	inv.Fail.Store(true)
	f.gateway.FailCancel.Store(true) // 보상(취소)마저 실패
	orch := &CheckoutOrchestrator{Payments: f.svc, Inventory: inv}

	p, err := orch.Checkout(ctx, Order{ID: "order-1", Amount: 10000, SKU: "sku-1", Qty: 1})
	if !errors.Is(err, ErrCompensationPending) {
		t.Fatalf("보상 보류 에러 기대: %v", err)
	}
	if p.Status != CancelRequested || f.gateway.IsCanceled(p.PgTxID) {
		t.Fatalf("CANCEL_REQUESTED 로 남고 PG 는 아직 승인 상태: %+v", p)
	}

	// 취소 API 가 아직 장애 → janitor 재시도도 실패, 백오프 후 재예약
	stats, err := f.janitor.RetryCancels(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rescheduled != 1 {
		t.Fatalf("재예약 기대: %+v", stats)
	}

	// 장애 복구 후 janitor 가 다시 시도 → CANCELED
	f.gateway.FailCancel.Store(false)
	stats, err = f.janitor.RetryCancels(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Canceled != 1 || f.mustGet(t, p.ID).Status != Canceled || !f.gateway.IsCanceled(p.PgTxID) {
		t.Fatalf("janitor 재시도로 취소 완료돼야 한다: %+v", stats)
	}
}
