package statemachine

import (
	"context"
	"testing"
	"time"
)

// Run(스케줄 루프) 자체를 검증한다. 테스트는 ReconcileUnknown 을
// 직접 부르지 않고, Run 이 스스로 주기 실행해서 확정하는지만 본다.
// (Kotlin 의 JanitorSchedulerTest 와 같은 시나리오)

func Test_run__reconciles_due_unknown_on_schedule(t *testing.T) {
	f := newFixture(t)
	const id = "00000000-0000-0000-0000-0000000000a1"
	insertPayment(t, id, Unknown) // idempotency_key = 'key-' || id
	exec(t, `UPDATE payments SET next_retry_at = now() - interval '1 minute'
	          WHERE id = $1`, id)
	// PG 에는 승인 기록이 있다 (= 승인 후 응답 유실)
	pgTxID, err := f.gateway.Charge(context.Background(),
		"key-"+id, id, 10000)
	if err != nil {
		t.Fatal(err)
	}

	// context.WithCancel: cancel() 을 부르면 ctx.Done() 채널이 닫힌다
	// (Kotlin 의 Job.cancel() 과 비슷)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 실패로 일찍 끝나도 goroutine 이 남지 않게
	done := make(chan struct{})
	go func() {
		f.janitor.Run(ctx, 50*time.Millisecond)
		close(done) // Run 이 끝났다는 신호
	}()

	// 고정 sleep 없이: 조건이 맞을 때까지 폴링, 상한 10초
	waitUntil(t, 10*time.Second, func() bool {
		return f.mustGet(t, id).Status == Approved
	})
	if p := f.mustGet(t, id); p.PgTxID != pgTxID {
		t.Fatalf("pg_tx_id 기대 %s, 실제 %s", pgTxID, p.PgTxID)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ctx 취소 후에도 Run 이 끝나지 않음")
	}
}

func Test_run__returns_promptly_when_ctx_cancelled(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		// 주기를 1시간으로 줘도, 취소되면 기다리지 않고 바로 끝나야 한다
		f.janitor.Run(ctx, time.Hour)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ctx 취소 후에도 Run 이 끝나지 않음 (interval 을 다 기다림?)")
	}
}

// waitUntil 은 cond 가 true 가 될 때까지 짧게 폴링한다.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond) // 폴링 간격 (결과 대기용 고정 sleep 아님)
	}
	t.Fatalf("%s 안에 조건이 만족되지 않음", timeout)
}
