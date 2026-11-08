package statemachine

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// 오래된 PENDING 정리 (Janitor.SweepPending).
// "REQUESTED → PENDING 커밋 후, PG 응답을 기록하기 전에 프로세스가
// 죽은" 결제를 행을 직접 넣어 재현한다. (Kotlin 의 PendingSweepTest)
//
// fixture: PendingAfter 2분, NotFoundGrace 5분, MaxAttempts 3,
// BaseBackoff 1분.

type pendingRow struct{ id, key string }

// insertPending 은 PENDING 까지 커밋하고 죽은 결제를 넣는다
// (next_retry_at 없음). created 를 직접 정해 시계를 조종한다.
func insertPending(t *testing.T, created time.Time) pendingRow {
	t.Helper()
	id := uuid.NewString()
	r := pendingRow{id: id, key: "key-" + id}
	exec(t, `INSERT INTO payments
	           (id, order_id, amount, status, idempotency_key, created_at)
	         VALUES ($1::uuid, $1::text, 10000, 'PENDING',
	                 $2, $3)`,
		id, r.key, created)
	return r
}

// nowMicros: Postgres timestamptz 는 마이크로초까지라 비교용으로 자른다.
// (Truncate 는 monotonic 시계 값도 떼어 낸다 → Equal 비교가 안전)
func nowMicros() time.Time { return time.Now().Truncate(time.Microsecond) }

// retryState 는 (attempt_count, next_retry_at) 을 읽는다.
// next_retry_at 이 NULL 이면 nil (*time.Time = Kotlin 의 Instant?).
func retryState(t *testing.T, id string) (int, *time.Time) {
	t.Helper()
	var n int
	var next *time.Time
	err := testPool.QueryRow(context.Background(),
		`SELECT attempt_count, next_retry_at FROM payments WHERE id = $1`,
		id).Scan(&n, &next)
	if err != nil {
		t.Fatal(err)
	}
	return n, next
}

// transitions 는 이력을 "FROM->TO" 문자열 목록으로 돌려준다.
func transitions(t *testing.T, f fixture, id string) []string {
	t.Helper()
	hist, err := f.repo.History(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, h := range hist {
		out = append(out, string(h.From)+"->"+string(h.To))
	}
	return out
}

func Test_stuck_pending__pg_approved__janitor_marks_approved(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	// PG 는 승인했는데, 응답을 기록하기 전에 프로세스가 죽었다
	created := nowMicros()
	p := insertPending(t, created)
	pgTxID, err := f.gateway.Charge(ctx, p.key, p.id, 10000)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := f.janitor.SweepPending(ctx, created.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	got := f.mustGet(t, p.id)
	if stats.Approved != 1 || got.Status != Approved {
		t.Fatalf("APPROVED 기대: stats=%+v payment=%+v", stats, got)
	}
	if got.PgTxID != pgTxID {
		t.Fatalf("pg_tx_id 기대 %s, 실제 %s", pgTxID, got.PgTxID)
	}
	if _, next := retryState(t, p.id); next != nil {
		t.Fatalf("next_retry_at 은 비워야 한다: %v", next)
	}
	if h := transitions(t, f, p.id); !slices.Equal(h,
		[]string{"PENDING->APPROVED"}) {
		t.Fatalf("이력: %v", h)
	}
	if f.gateway.ChargeCount() != 1 {
		t.Fatal("재결제 없이 확인만 했어야 한다")
	}
}

func Test_stuck_pending__no_pg_record__failed_after_grace(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	// PENDING 커밋 직후, PG 를 부르기 전에 죽었다 → PG 기록 없음
	created := nowMicros()
	p := insertPending(t, created)

	// 3분 뒤: threshold(2분)는 지났지만 grace(5분) 안.
	// UNKNOWN 규칙과 같이 아직 FAILED 로 단정하지 않는다
	// → UNKNOWN 으로 넘기고, 시도로 세지 않는다.
	stats, err := f.janitor.SweepPending(ctx, created.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if stats.ToUnknown != 1 || f.mustGet(t, p.id).Status != Unknown {
		t.Fatalf("grace 안 NOT_FOUND → UNKNOWN 기대: %+v", stats)
	}
	n, next := retryState(t, p.id)
	graceEnd := created.Add(5 * time.Minute)
	if n != 0 || next == nil || !next.Equal(graceEnd) {
		t.Fatalf("attempt 0, next=grace 끝 기대: n=%d next=%v", n, next)
	}

	// grace 가 끝나는 시각에 UNKNOWN 흐름이 FAILED 로 확정
	if _, err := f.janitor.ReconcileUnknown(ctx, *next); err != nil {
		t.Fatal(err)
	}
	if s := f.mustGet(t, p.id).Status; s != Failed {
		t.Fatalf("FAILED 기대: %s", s)
	}
	if h := transitions(t, f, p.id); !slices.Equal(h,
		[]string{"PENDING->UNKNOWN", "UNKNOWN->FAILED"}) {
		t.Fatalf("이력: %v", h)
	}
}

func Test_stuck_pending__no_pg_record_past_grace__failed_directly(
	t *testing.T,
) {
	f := newFixture(t)
	created := nowMicros()
	p := insertPending(t, created)

	stats, err := f.janitor.SweepPending(context.Background(),
		created.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failed != 1 || f.mustGet(t, p.id).Status != Failed {
		t.Fatalf("FAILED 기대: %+v", stats)
	}
	if h := transitions(t, f, p.id); !slices.Equal(h,
		[]string{"PENDING->FAILED"}) {
		t.Fatalf("이력: %v", h)
	}
}

func Test_pending_younger_than_threshold__not_touched(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	now := nowMicros()
	// 대조군: threshold 를 넘긴 PENDING (집혀야 한다)
	old := insertPending(t, now.Add(-3*time.Minute))
	// 1초 차이로 threshold 안: 요청이 아직 PG 응답을 기다리는 중일 수
	// 있다. PG 에 승인 기록이 있어도 건드리지 않는다.
	young := insertPending(t, now.Add(-2*time.Minute+time.Second))
	if _, err := f.gateway.Charge(ctx, young.key, young.id, 10000); err != nil {
		t.Fatal(err)
	}
	var queried []string
	f.gateway.OnGetStatus = func(key string) {
		queried = append(queried, key)
	}

	stats, err := f.janitor.SweepPending(ctx, now)
	if err != nil {
		t.Fatal(err)
	}

	// 대조군은 grace(5분) 안의 NOT_FOUND → UNKNOWN 으로 넘어간다
	if stats.ToUnknown != 1 || !slices.Equal(queried, []string{old.key}) {
		t.Fatalf("오래된 건만 집어야 한다: stats=%+v queried=%v",
			stats, queried)
	}
	if s := f.mustGet(t, young.id).Status; s != Pending {
		t.Fatalf("젊은 PENDING 은 그대로: %s", s)
	}
	if n, next := retryState(t, young.id); n != 0 || next != nil {
		t.Fatalf("attempt/next_retry_at 도 그대로: n=%d next=%v", n, next)
	}
	if h := transitions(t, f, young.id); len(h) != 0 {
		t.Fatalf("이력 없음 기대: %v", h)
	}
}

func Test_stuck_pending__pg_query_unavailable__handed_to_unknown_flow(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	created := nowMicros()
	p := insertPending(t, created)
	if _, err := f.gateway.Charge(ctx, p.key, p.id, 10000); err != nil {
		t.Fatal(err)
	}
	f.gateway.FailStatusQuery.Store(true) // 조회 API 장애

	at := created.Add(3 * time.Minute)
	stats, err := f.janitor.SweepPending(ctx, at)
	if err != nil {
		t.Fatal(err)
	}

	// 판단 불가 = "결과를 모름" = UNKNOWN. 재시도/보류는 UNKNOWN
	// 흐름 하나가 맡는다. 이번 확인은 1회로 센다.
	if stats.ToUnknown != 1 || f.mustGet(t, p.id).Status != Unknown {
		t.Fatalf("UNKNOWN 기대: %+v", stats)
	}
	n, next := retryState(t, p.id)
	if n != 1 || next == nil || !next.Equal(at.Add(time.Minute)) {
		t.Fatalf("attempt 1, 백오프 1회차(1분) 기대: n=%d next=%v", n, next)
	}

	f.gateway.FailStatusQuery.Store(false)
	if _, err := f.janitor.ReconcileUnknown(ctx, *next); err != nil {
		t.Fatal(err)
	}
	if s := f.mustGet(t, p.id).Status; s != Approved {
		t.Fatalf("APPROVED 기대: %s", s)
	}
	if h := transitions(t, f, p.id); !slices.Equal(h,
		[]string{"PENDING->UNKNOWN", "UNKNOWN->APPROVED"}) {
		t.Fatalf("이력: %v", h)
	}
}

func Test_pending_race__janitor_settles_first__request_gets_no_error(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	// Pay 가 PG 응답을 받기 직전에 janitor 가 같은 결제를 확정한다
	// (PendingAfter 보다 늦은 PG 응답을, janitor 에 넘기는 now 를
	// 당겨서 재현).
	var swept JanitorStats
	var sweepErr error
	f.gateway.OnCharge = func(string) {
		swept, sweepErr = f.janitor.SweepPending(ctx,
			time.Now().Add(3*time.Minute))
	}

	p, err := f.svc.Pay(ctx, "order-1", 10000)

	if err != nil {
		t.Fatalf("요청 쪽에 에러가 새면 안 된다: %v", err)
	}
	if sweepErr != nil || swept.Approved != 1 {
		t.Fatalf("janitor 가 진행 중인 PENDING 을 확정했어야 한다: %+v %v",
			swept, sweepErr)
	}
	if p.Status != Approved || p.PgTxID == "" {
		t.Fatalf("APPROVED 기대: %+v", p)
	}
	hist, err := f.repo.History(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	var fromPending []HistoryEntry
	for _, h := range hist {
		if h.From == Pending {
			fromPending = append(fromPending, h)
		}
	}
	if len(fromPending) != 1 ||
		!strings.HasPrefix(fromPending[0].Reason, "janitor") {
		t.Fatalf("PENDING 에서 나간 전이는 janitor 의 1건뿐: %v", fromPending)
	}
	// 같은 결론(APPROVED)이면 요청은 아무 전이도 더 하지 않는다
	if h := transitions(t, f, p.ID); !slices.Equal(h, []string{
		"->REQUESTED", "REQUESTED->PENDING", "PENDING->APPROVED",
	}) {
		t.Fatalf("이력: %v", h)
	}
	if f.gateway.ChargeCount() != 1 || f.gateway.CancelCount() != 0 {
		t.Fatal("PG 승인은 1번, 취소 없음")
	}
}

func Test_pending_race__request_waits_for_janitor_lock__then_adopts_settled(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	created := nowMicros()
	p := insertPending(t, created)
	pgTxID, err := f.gateway.Charge(ctx, p.key, p.id, 10000)
	if err != nil {
		t.Fatal(err)
	}
	// janitor 가 PG 에 묻는 사이 요청의 PG 응답(승인)이 도착한다.
	// Go janitor 는 행 잠금을 쥔 채 PG 를 부르므로, 요청의 CAS 는
	// janitor 가 커밋할 때까지 "기다린다". 훅 안에서 요청 backend 가
	// 정말 Lock 대기에 들어간 것을 pg_stat_activity 로 확인한 뒤에야
	// janitor 를 진행시킨다 (안 기다리고 먼저 끝났다면 이 테스트는 실패).
	// (Kotlin 은 claim 을 먼저 커밋해 잠금이 없다 → 요청이 이길 수
	// 있고 janitor 가 Stale 을 받는다)
	type result struct {
		p   Payment
		err error
	}
	reqDone := make(chan result, 1) // 버퍼 1: 받는 쪽이 없어도 안 막힘
	sawLockWait := false
	f.gateway.OnGetStatus = func(key string) {
		if key != p.key {
			return
		}
		go func() { // 요청 goroutine: Pay 의 4) 단계
			got, err := f.svc.recordChargeResult(ctx, p.id, pgTxID, nil)
			reqDone <- result{got, err}
		}()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			// 이 DB 에서 행 잠금을 기다리는 backend 수. 테스트 DB 는
			// 이 패키지만 쓰므로 1 이면 요청 goroutine 이다.
			err := testPool.QueryRow(ctx, `
				SELECT count(*) FROM pg_stat_activity
				 WHERE datname = current_database()
				   AND wait_event_type = 'Lock'`).Scan(&n)
			if err == nil && n > 0 {
				sawLockWait = true
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	stats, err := f.janitor.SweepPending(ctx, created.Add(time.Hour))
	if err != nil || stats.Approved != 1 {
		t.Fatalf("janitor 가 확정: %+v %v", stats, err)
	}
	if !sawLockWait {
		t.Fatal("요청이 janitor 의 행 잠금을 기다리는 것을 보지 못했다")
	}
	select {
	case r := <-reqDone:
		// CAS 0행 → 다시 읽으니 같은 결론(APPROVED) → 에러 없이 그 상태
		if r.err != nil {
			t.Fatalf("요청 쪽에 에러가 새면 안 된다: %v", r.err)
		}
		if r.p.Status != Approved || r.p.PgTxID != pgTxID {
			t.Fatalf("janitor 가 확정한 APPROVED 를 돌려받아야 한다: %+v", r.p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("요청 쪽 전이가 끝나지 않음")
	}
	if h := transitions(t, f, p.id); !slices.Equal(h,
		[]string{"PENDING->APPROVED"}) {
		t.Fatalf("이력은 janitor 의 1건: %v", h)
	}
}

func Test_sweep_pending__zero_threshold__rejected(t *testing.T) {
	f := newFixture(t)
	insertPending(t, nowMicros())
	f.janitor.PendingAfter = 0 // 설정 누락: 진행 중인 요청까지 집게 된다

	if _, err := f.janitor.SweepPending(context.Background(),
		time.Now()); err == nil {
		t.Fatal("PendingAfter <= 0 은 거부해야 한다")
	}
	if n := queryInt(t,
		`SELECT count(*) FROM payments WHERE status <> 'PENDING'`); n != 0 {
		t.Fatalf("아무것도 건드리지 않아야 한다: %d", n)
	}
}

// ---- 요청이 늦게 돌아왔는데 janitor 가 이미 다르게 정한 경우 ----
// 요청이 PENDING 을 커밋하고 PG 앞에서 멈춘다(OnBeforeCharge). 그
// 사이 janitor 가 PG 에 물으면 NOT_FOUND 다. 그 뒤 요청이 깨어나
// PG 에서 결과를 받는다. PENDING → * CAS 는 0행이 되지만, 요청이
// 아는 PG 결과를 버리면 안 된다.

func Test_late_pg_response__janitor_moved_to_unknown__request_approved__approved(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	// +3분: pending-after(2분)는 지났고 grace(5분) 안 → NOT_FOUND → UNKNOWN
	var swept JanitorStats
	f.gateway.OnBeforeCharge = func(string) {
		swept, _ = f.janitor.SweepPending(ctx, time.Now().Add(3*time.Minute))
	}

	p, err := f.svc.Pay(ctx, "order-1", 10000)

	if err != nil {
		t.Fatal(err)
	}
	if swept.ToUnknown != 1 {
		t.Fatalf("janitor 가 먼저 UNKNOWN 으로 넘겼어야 한다: %+v", swept)
	}
	// 요청은 PG 승인(pgTxID)을 안다 → UNKNOWN 으로 두면 안 된다
	if p.Status != Approved || p.PgTxID == "" {
		t.Fatalf("APPROVED + pg_tx_id 기대: %+v", p)
	}
	if _, next := retryState(t, p.ID); next != nil {
		t.Fatalf("UNKNOWN 재시도 예약은 지워야 한다: %v", next)
	}
	if h := transitions(t, f, p.ID); !slices.Equal(h, []string{
		"->REQUESTED", "REQUESTED->PENDING",
		"PENDING->UNKNOWN", "UNKNOWN->APPROVED",
	}) {
		t.Fatalf("이력: %v", h)
	}
	if f.gateway.ChargeCount() != 1 || f.gateway.CancelCount() != 0 {
		t.Fatal("승인 1번, 취소 없음")
	}
}

func Test_late_pg_response__janitor_moved_to_unknown__request_declined__failed(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	f.gateway.DeclineCharge.Store(true)
	f.gateway.OnBeforeCharge = func(string) {
		_, _ = f.janitor.SweepPending(ctx, time.Now().Add(3*time.Minute))
	}

	p, err := f.svc.Pay(ctx, "order-1", 10000)

	if err != nil {
		t.Fatal(err)
	}
	// PG 가 명확히 거절했다 → UNKNOWN 이 아니라 FAILED
	if p.Status != Failed {
		t.Fatalf("FAILED 기대: %+v", p)
	}
	if _, next := retryState(t, p.ID); next != nil {
		t.Fatalf("UNKNOWN 재시도 예약은 지워야 한다: %v", next)
	}
	if h := transitions(t, f, p.ID); !slices.Equal(h, []string{
		"->REQUESTED", "REQUESTED->PENDING",
		"PENDING->UNKNOWN", "UNKNOWN->FAILED",
	}) {
		t.Fatalf("이력: %v", h)
	}
}

// 가장 위험한 경우: janitor 는 grace 가 지나도록 PG 기록이 없어
// FAILED 로 확정했는데, 요청이 그 뒤 PG 에 닿아 승인됐다(돈이 빠짐).
// FAILED 로 두면 고객은 재결제 → 이중 결제. 망취소(net-cancel)한다.
func Test_late_pg_response__janitor_failed__request_approved__net_canceled(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	var swept JanitorStats
	f.gateway.OnBeforeCharge = func(string) {
		swept, _ = f.janitor.SweepPending(ctx, time.Now().Add(time.Hour))
	}

	p, err := f.svc.Pay(ctx, "order-1", 10000)

	if err != nil {
		t.Fatal(err)
	}
	if swept.Failed != 1 {
		t.Fatalf("janitor 가 먼저 FAILED 로 확정했어야 한다: %+v", swept)
	}
	if p.Status != Canceled || p.PgTxID == "" {
		t.Fatalf("늦은 승인은 취소돼야 한다 (CANCELED + pg_tx_id): %+v", p)
	}
	if !f.gateway.IsCanceled(p.PgTxID) || f.gateway.CancelCount() != 1 {
		t.Fatal("PG 에서 실제로 취소돼야 한다")
	}
	if h := transitions(t, f, p.ID); !slices.Equal(h, []string{
		"->REQUESTED", "REQUESTED->PENDING", "PENDING->FAILED",
		"FAILED->CANCEL_REQUESTED", "CANCEL_REQUESTED->CANCELED",
	}) {
		t.Fatalf("이력: %v", h)
	}
}

func Test_late_pg_response__janitor_failed__net_cancel_fails__janitor_retries(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	f.gateway.FailCancel.Store(true) // 망취소마저 실패
	f.gateway.OnBeforeCharge = func(string) {
		_, _ = f.janitor.SweepPending(ctx, time.Now().Add(time.Hour))
	}

	p, err := f.svc.Pay(ctx, "order-1", 10000)

	if err != nil {
		t.Fatal(err)
	}
	// "돈은 빠졌는데 FAILED, 후속 조치 없음" 이 되면 안 된다
	// → CANCEL_REQUESTED 로 남아 janitor 의 취소 재시도 대상이 된다
	if p.Status != CancelRequested || p.PgTxID == "" {
		t.Fatalf("CANCEL_REQUESTED + pg_tx_id 기대: %+v", p)
	}
	n, next := retryState(t, p.ID)
	if n != 1 || next == nil {
		t.Fatalf("취소 1회 실패, 재시도 예약 기대: n=%d next=%v", n, next)
	}

	f.gateway.FailCancel.Store(false)
	stats, err := f.janitor.RetryCancels(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if stats.Canceled != 1 || f.mustGet(t, p.ID).Status != Canceled ||
		!f.gateway.IsCanceled(p.PgTxID) {
		t.Fatalf("janitor 재시도로 취소 완료: %+v", stats)
	}
}

// 요청도 결과를 모르면(타임아웃) janitor 가 정한 상태가 더 많이 안다.
func Test_late_pg_response__janitor_failed__request_timed_out__keeps_stored(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	f.gateway.TimeoutBeforeCharge.Store(true)
	f.gateway.OnBeforeCharge = func(string) {
		_, _ = f.janitor.SweepPending(ctx, time.Now().Add(time.Hour))
	}

	p, err := f.svc.Pay(ctx, "order-1", 10000)

	if err != nil {
		t.Fatal(err)
	}
	if p.Status != Failed {
		t.Fatalf("저장된 FAILED 그대로: %+v", p)
	}
	if h := transitions(t, f, p.ID); !slices.Equal(h, []string{
		"->REQUESTED", "REQUESTED->PENDING", "PENDING->FAILED",
	}) {
		t.Fatalf("이력: %v", h)
	}
}

// 조회가 멈추면(PG 무응답) 행 잠금을 쥔 채 무한정 기다리면 안 된다.
// PgCallTimeout 이 지나면 판단 불가로 보고 UNKNOWN 으로 넘긴다.
func Test_sweep_pending__pg_status_query_hangs__timeout_is_inconclusive(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	f.janitor.PgCallTimeout = 50 * time.Millisecond
	created := nowMicros()
	p := insertPending(t, created)
	f.gateway.OnGetStatus = func(string) {
		time.Sleep(500 * time.Millisecond) // 제한 시간보다 한참 길게
	}

	start := time.Now()
	stats, err := f.janitor.SweepPending(ctx, created.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if stats.ToUnknown != 1 || f.mustGet(t, p.id).Status != Unknown {
		t.Fatalf("타임아웃 → 판단 불가 → UNKNOWN 기대: %+v", stats)
	}
	if got := f.mustGet(t, p.id).LastError; !strings.Contains(got,
		context.DeadlineExceeded.Error()) {
		t.Fatalf("last_error 에 타임아웃이 남아야 한다: %q", got)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("너무 오래 걸림: %v", d)
	}
}
