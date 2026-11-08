package balance

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// 시나리오: 잔액 1,000원 계좌에 1,000원 인출 요청 10건이 "동시에" 들어온다. 정답은 성공 1건, 잔액 0.
const (
	initialBalance = 1000
	withdrawAmount = 1000
	concurrency    = 10
	// 나이브/낙관적/직렬화 전략에서 "읽기 → 쓰기" 사이 틈. 10개 goroutine 이 모두 읽은 뒤에 첫 쓰기가 일어나게 한다.
	raceGap = 200 * time.Millisecond
)

func resetAccounts(t *testing.T, balances map[string]int64) {
	t.Helper()
	exec(t, `TRUNCATE accounts, withdrawals`)
	for id, b := range balances {
		exec(t, `INSERT INTO accounts (id, balance) VALUES ($1, $2)`, id, b)
	}
}

// runConcurrently 는 fn 을 n 개의 goroutine 에서 "동시에" 실행한다.
// start 채널을 close 하는 순간 모든 goroutine 이 한꺼번에 출발한다 (start barrier).
// 결과는 i 번째 칸에 저장하므로 잠금이 필요 없다.
func runConcurrently[T any](n int, fn func(i int) T) []T { // [T any] = 제네릭 (Kotlin의 fun <T> runConcurrently)
	results := make([]T, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = fn(i)
		}()
	}
	close(start)
	wg.Wait()
	return results
}

// withdrawOutcome 은 goroutine 한 개의 결과.
type withdrawOutcome struct {
	res Result
	err error
}

// runWithdrawScenario 는 공통 시나리오를 돌리고 (성공 수, 잔액부족 수)를 돌려준다.
func runWithdrawScenario(t *testing.T, w Withdrawer) (successes, insufficient int) {
	t.Helper()
	resetAccounts(t, map[string]int64{"acc-1": initialBalance})
	outcomes := runConcurrently(concurrency, func(int) withdrawOutcome {
		res, err := w.Withdraw(context.Background(), "acc-1", withdrawAmount)
		return withdrawOutcome{res, err}
	})
	for _, o := range outcomes {
		switch {
		case o.err != nil:
			t.Fatalf("예상 못한 에러: %v", o.err)
		case o.res == Success:
			successes++
		case o.res == Insufficient:
			insufficient++
		}
	}
	return successes, insufficient
}

// assertExactlyOne 은 "정확히 1건 성공, 잔액 0, withdrawals 합계 = 1,000" 을 확인한다.
func assertExactlyOne(t *testing.T, successes, insufficient int) {
	t.Helper()
	if successes != 1 || insufficient != concurrency-1 {
		t.Fatalf("성공 1 / 잔액부족 %d 기대: 성공=%d, 부족=%d", concurrency-1, successes, insufficient)
	}
	if b := queryInt(t, `SELECT balance FROM accounts WHERE id = 'acc-1'`); b != 0 {
		t.Fatalf("잔액 0 기대: %d", b)
	}
	if sum := queryInt(t, `SELECT coalesce(sum(amount), 0) FROM withdrawals`); sum != withdrawAmount {
		t.Fatalf("withdrawals 합계 %d 기대: %d", withdrawAmount, sum)
	}
}

func Test_naive__concurrent_withdrawals__double_spending(t *testing.T) {
	successes, _ := runWithdrawScenario(t, &NaiveWithdraw{DB: testPool, ReadWriteDelay: raceGap})

	balance := queryInt(t, `SELECT balance FROM accounts WHERE id = 'acc-1'`)
	withdrawn := queryInt(t, `SELECT coalesce(sum(amount), 0) FROM withdrawals`)
	t.Logf("성공 %d건, 실제 인출 합계 %d원, 남은 잔액 %d원 (1,000원 계좌!)", successes, withdrawn, balance)

	// raceGap(200ms) 동안 10건이 모두 "1,000" 을 읽고 나서야 첫 쓰기가 일어나므로
	// 결과가 결정적이다: 10건 모두 성공, 각자 0 을 덮어써서 잔액 0.
	if successes != concurrency {
		t.Fatalf("10건 모두 성공해야 한다 (이중 지불): %d", successes)
	}
	if withdrawn != concurrency*withdrawAmount || balance != 0 {
		t.Fatalf("인출 10,000 / 잔액 0 기대: withdrawn=%d balance=%d",
			withdrawn, balance)
	}
	// 돈의 보존 법칙이 깨졌다: 초기 잔액 - 인출 합계(-9,000) ≠ 현재 잔액(0)
	if initialBalance-withdrawn == balance {
		t.Fatalf("잔액과 인출 합계가 불일치해야 한다: balance=%d withdrawn=%d", balance, withdrawn)
	}
}

func Test_pessimistic__concurrent_withdrawals__exactly_one_succeeds(t *testing.T) {
	s, i := runWithdrawScenario(t, &PessimisticWithdraw{DB: testPool, ReadWriteDelay: 20 * time.Millisecond})
	assertExactlyOne(t, s, i)
}

func Test_optimistic__concurrent_withdrawals__exactly_one_succeeds_with_retries(t *testing.T) {
	w := &OptimisticWithdraw{
		DB: testPool, MaxAttempts: 5, ReadWriteDelay: raceGap,
	}
	s, i := runWithdrawScenario(t, w)
	assertExactlyOne(t, s, i)

	// 모두 version 0 을 읽었으므로 1명만 이기고 나머지 9명은 0행 갱신
	// → 재시도 1번 → 잔액 0 을 읽고 "부족". 그래서 재시도는 정확히 9회.
	t.Logf("재시도 %d회", w.Retries.Load())
	if w.Retries.Load() != concurrency-1 {
		t.Fatalf("충돌한 %d건이 1번씩 재시도해야 한다: %d",
			concurrency-1, w.Retries.Load())
	}
}

func Test_serializable__concurrent_withdrawals__exactly_one_succeeds_with_retries(t *testing.T) {
	w := &SerializableWithdraw{
		DB: testPool, MaxAttempts: 5, ReadWriteDelay: raceGap,
	}
	s, i := runWithdrawScenario(t, w)
	assertExactlyOne(t, s, i)

	// 승자가 커밋하면 나머지 9건의 UPDATE 가 40001 → 재시도 1번 → 잔액 0 → "부족".
	t.Logf("40001 로 인한 재시도 %d회", w.Retries.Load())
	if w.Retries.Load() != concurrency-1 {
		t.Fatalf("충돌한 %d건이 40001 후 1번씩 재시도해야 한다: %d",
			concurrency-1, w.Retries.Load())
	}
}

func Test_conditional_update__concurrent_withdrawals__exactly_one_succeeds(t *testing.T) {
	s, i := runWithdrawScenario(t, &ConditionalUpdateWithdraw{DB: testPool})
	assertExactlyOne(t, s, i)
}

func Test_transfer__opposite_directions_concurrently__no_deadlock(t *testing.T) {
	resetAccounts(t, map[string]int64{"A": 10000, "B": 10000})
	svc := &TransferService{DB: testPool, LockGap: 10 * time.Millisecond}

	const n = 20 // 짝수 번째는 A→B, 홀수 번째는 B→A
	errs := runConcurrently(n, func(i int) error {
		from, to := "A", "B"
		if i%2 == 1 {
			from, to = "B", "A" // Go는 여러 변수를 한 번에 대입할 수 있다 (swap 도 이렇게)
		}
		res, err := svc.Transfer(context.Background(), from, to, 100)
		if err == nil && res != Success {
			return errors.New(string(res))
		}
		return err
	})
	for _, err := range errs {
		if err != nil {
			t.Fatalf("정렬된 잠금 순서에서는 에러(데드락)가 없어야 한다: %v (SQLSTATE=%s)", err, SQLState(err))
		}
	}
	if total := queryInt(t, `SELECT sum(balance) FROM accounts`); total != 20000 {
		t.Fatalf("총액 보존: 20000 기대, %d", total)
	}
	if a := queryInt(t, `SELECT balance FROM accounts WHERE id = 'A'`); a != 10000 {
		t.Fatalf("같은 수만큼 오갔으므로 A=10000: %d", a)
	}
}

func Test_transfer__unordered_locking__deadlock_detected(t *testing.T) {
	resetAccounts(t, map[string]int64{"A": 10000, "B": 10000})
	// 각자 첫 행을 잠근 뒤 300ms 기다렸다가 두 번째 행을 잠근다 → A→B 는 A를 쥐고 B를, B→A 는 B를 쥐고 A를 기다린다.
	svc := &TransferService{DB: testPool, LockGap: 300 * time.Millisecond}

	errs := runConcurrently(2, func(i int) error {
		var err error
		if i == 0 {
			_, err = svc.TransferUnordered(context.Background(), "A", "B", 100)
		} else {
			_, err = svc.TransferUnordered(context.Background(), "B", "A", 100)
		}
		return err
	})

	deadlocks := 0
	for _, err := range errs {
		if SQLState(err) == SQLStateDeadlockDetected {
			deadlocks++
			t.Logf("Postgres 가 데드락을 감지하고 한쪽을 죽임: %v", err)
		} else if err != nil {
			t.Fatalf("예상 못한 에러: %v", err)
		}
	}
	// Postgres 는 deadlock_timeout(기본 1초) 후 순환을 감지하고 "한쪽만" 실패시킨다. 나머지는 성공.
	if deadlocks != 1 {
		t.Fatalf("40P01 이 정확히 1건 나야 한다: %d", deadlocks)
	}
	if total := queryInt(t, `SELECT sum(balance) FROM accounts`); total != 20000 {
		t.Fatalf("죽은 쪽은 롤백되므로 총액은 보존: %d", total)
	}
}
