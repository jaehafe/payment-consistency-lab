package balance

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Result 는 인출 결과. `type Result string` 은 string 을 바탕으로 한 새 타입 (Kotlin의 value class 와 비슷).
type Result string

const (
	Success      Result = "SUCCESS"
	Insufficient Result = "INSUFFICIENT" // 잔액 부족 (에러가 아니라 정상적인 비즈니스 결과)
)

// ErrAccountNotFound 는 계좌가 없을 때.
var ErrAccountNotFound = errors.New("account not found")

// Withdrawer 는 5가지 전략이 모두 만족하는 인터페이스. 테스트가 같은 시나리오를 전략만 바꿔 돌린다.
type Withdrawer interface {
	Withdraw(ctx context.Context, accountID string, amount int64) (Result, error)
}

// Postgres SQLSTATE 코드. https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	SQLStateSerializationFailure = "40001" // SERIALIZABLE 충돌 → 재시도하면 된다
	SQLStateDeadlockDetected     = "40P01" // 데드락 → Postgres 가 한쪽 트랜잭션을 강제로 실패시킴
)

// SQLState 는 에러에서 Postgres SQLSTATE 코드를 꺼낸다. Postgres 에러가 아니면 "".
//
// errors.As(err, &target): err (또는 그것이 감싼 원인 체인) 중에 *pgconn.PgError 타입이 있으면 target 에 넣고 true.
// Kotlin으로 치면 `(e as? PSQLException)?.sqlState` 를 cause 체인을 따라가며 하는 것.
// errors.Is 는 "같은 값인가"를, errors.As 는 "이 타입인가(그리고 꺼내줘)"를 본다.
func SQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// recordWithdrawal 은 성공한 인출을 withdrawals 에 남긴다 (모든 전략 공통).
func recordWithdrawal(ctx context.Context, tx pgx.Tx, accountID string, amount int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO withdrawals (account_id, amount) VALUES ($1, $2)`, accountID, amount)
	return err
}

// ---------------------------------------------------------------------------
// 1) NaiveWithdraw: 읽고 → 앱에서 검사 → "읽은 값 - amount" 로 덮어쓰기.  ❌ 이중 지불
// ---------------------------------------------------------------------------

type NaiveWithdraw struct {
	DB *pgxpool.Pool
	// ReadWriteDelay: SELECT 와 UPDATE 사이에 일부러 기다리는 시간.
	// 실제로는 GC 멈춤, 외부 API 호출, 느린 네트워크 등으로 생기는 틈. 테스트에서 레이스를 "결정적으로" 재현하는 데 쓴다.
	ReadWriteDelay time.Duration
}

func (w *NaiveWithdraw) Withdraw(ctx context.Context, accountID string, amount int64) (Result, error) {
	tx, err := w.DB.Begin(ctx) // 기본 격리 수준 READ COMMITTED
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var balance int64
	if err := tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, accountID).Scan(&balance); err != nil {
		return "", notFound(err)
	}
	if balance < amount {
		return Insufficient, nil
	}

	time.Sleep(w.ReadWriteDelay) // ← 이 틈에 다른 요청들도 "잔액 1,000" 을 읽는다

	// 문제: 내가 읽은 값(1,000) 기준으로 덮어쓴다. 그 사이 다른 트랜잭션이 이미 빼갔어도 모른다 (lost update).
	// UPDATE 는 행 잠금을 잡지만, 앞 트랜잭션 커밋을 기다렸다가 "내 계산 결과"로 다시 덮어쓸 뿐이다.
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = $2 WHERE id = $1`, accountID, balance-amount); err != nil {
		return "", err
	}
	if err := recordWithdrawal(ctx, tx, accountID, amount); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return Success, nil
}

// ---------------------------------------------------------------------------
// 2) PessimisticWithdraw: SELECT ... FOR UPDATE 로 읽는 순간 잠근다.  ✅
// ---------------------------------------------------------------------------

type PessimisticWithdraw struct {
	DB             *pgxpool.Pool
	ReadWriteDelay time.Duration // 나이브와 같은 틈을 줘도 안전하다는 걸 보이기 위해
}

func (w *PessimisticWithdraw) Withdraw(ctx context.Context, accountID string, amount int64) (Result, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var balance int64
	// FOR UPDATE: 이 행을 커밋/롤백까지 잠근다. 다른 요청의 FOR UPDATE 는 여기서 줄을 서고,
	// 앞 트랜잭션이 커밋한 "최신 값"을 읽게 된다. (JPA의 @Lock(PESSIMISTIC_WRITE))
	if err := tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1 FOR UPDATE`, accountID).Scan(&balance); err != nil {
		return "", notFound(err)
	}
	if balance < amount {
		return Insufficient, nil
	}
	time.Sleep(w.ReadWriteDelay)

	// 나이브와 똑같이 "읽은 값 - amount" 를 절대값으로 쓴다 (Kotlin 은 balance - ?).
	// 일부러 그렇게 했다: 안전하게 만드는 건 오직 FOR UPDATE 라는 걸 보이려고.
	// FOR UPDATE 를 지우면 이 함수는 나이브와 같아져 10건 성공·잔액 0 (lost update).
	// (Kotlin 은 지우면 10건 성공·잔액 -9,000 — 초과 인출이지만 합계는 맞음)
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = $2 WHERE id = $1`, accountID, balance-amount); err != nil {
		return "", err
	}
	if err := recordWithdrawal(ctx, tx, accountID, amount); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return Success, nil
}

// defaultMaxAttempts 는 MaxAttempts 를 안 정했을 때(0) 쓰는 값. Kotlin 과 같다.
const defaultMaxAttempts = 20

// attemptsOrDefault: 구조체 zero value(MaxAttempts=0)여도 동작하게 한다.
func attemptsOrDefault(n int) int {
	if n <= 0 {
		return defaultMaxAttempts
	}
	return n
}

// ---------------------------------------------------------------------------
// 3) OptimisticWithdraw: version 컬럼으로 "내가 읽은 뒤 아무도 안 바꿨을 때만" 쓴다.  ✅ (재시도 필요)
// ---------------------------------------------------------------------------

type OptimisticWithdraw struct {
	DB *pgxpool.Pool
	// MaxAttempts: 첫 시도를 "포함한" 총 시도 횟수 (Kotlin maxAttempts 와 같은 의미).
	// 3 이면 첫 시도 1 + 재시도 2. 0 이면 defaultMaxAttempts.
	MaxAttempts    int
	ReadWriteDelay time.Duration
	// Retries: 지금까지 재시도한 총 횟수 (테스트/관측용).
	// atomic.Int64 는 여러 goroutine 이 동시에 Add 해도 안전한 정수 (Kotlin의 AtomicLong).
	Retries atomic.Int64
}

// ErrTooManyRetries 는 재시도 한도를 넘었을 때.
var ErrTooManyRetries = errors.New("too many retries")

func (w *OptimisticWithdraw) Withdraw(ctx context.Context, accountID string, amount int64) (Result, error) {
	for attempt := 0; attempt < attemptsOrDefault(w.MaxAttempts); attempt++ {
		if attempt > 0 {
			w.Retries.Add(1)
		}
		res, retry, err := w.try(ctx, accountID, amount)
		if err != nil || !retry {
			return res, err
		}
		// retry == true: 다른 트랜잭션이 먼저 바꿨다 → 처음부터 다시 읽는다
	}
	return "", ErrTooManyRetries
}

// try 는 반환값이 3개: (결과, 재시도 필요 여부, 에러). Go는 이렇게 여러 값을 자연스럽게 돌려준다.
func (w *OptimisticWithdraw) try(ctx context.Context, accountID string, amount int64) (Result, bool, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)

	var balance, version int64
	// 잠그지 않고 읽는다 (FOR UPDATE 없음) → 읽기끼리 서로 막지 않는다
	if err := tx.QueryRow(ctx, `SELECT balance, version FROM accounts WHERE id = $1`, accountID).Scan(&balance, &version); err != nil {
		return "", false, notFound(err)
	}
	if balance < amount {
		return Insufficient, false, nil
	}
	time.Sleep(w.ReadWriteDelay)

	// WHERE version = 내가 읽은 version: 그 사이 누가 바꿨다면 version 이 올라가 있어서 0행이 갱신된다.
	// (JPA의 @Version 이 내부적으로 하는 일과 정확히 같다)
	tag, err := tx.Exec(ctx,
		`UPDATE accounts SET balance = $2, version = version + 1 WHERE id = $1 AND version = $3`,
		accountID, balance-amount, version)
	if err != nil {
		return "", false, err
	}
	if tag.RowsAffected() == 0 {
		return "", true, nil // 충돌 → 재시도
	}
	if err := recordWithdrawal(ctx, tx, accountID, amount); err != nil {
		return "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, err
	}
	return Success, false, nil
}

// ---------------------------------------------------------------------------
// 4) SerializableWithdraw: 격리 수준을 SERIALIZABLE 로. 충돌하면 DB가 40001 로 실패시킨다.  ✅ (재시도 필요)
// ---------------------------------------------------------------------------

type SerializableWithdraw struct {
	DB             *pgxpool.Pool
	MaxAttempts    int // 첫 시도 포함 총 시도 횟수. 0 이면 defaultMaxAttempts
	ReadWriteDelay time.Duration
	Retries        atomic.Int64
}

func (w *SerializableWithdraw) Withdraw(ctx context.Context, accountID string, amount int64) (Result, error) {
	for attempt := 0; attempt < attemptsOrDefault(w.MaxAttempts); attempt++ {
		if attempt > 0 {
			w.Retries.Add(1)
		}
		res, err := w.try(ctx, accountID, amount)
		if SQLState(err) == SQLStateSerializationFailure {
			continue // "동시에 하면 직렬 실행과 결과가 달라질 수 있었다" → 처음부터 다시
		}
		return res, err
	}
	return "", ErrTooManyRetries
}

func (w *SerializableWithdraw) try(ctx context.Context, accountID string, amount int64) (Result, error) {
	// pgx.TxOptions 로 격리 수준을 지정한다. Spring의 @Transactional(isolation = SERIALIZABLE).
	tx, err := w.DB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	// 코드는 나이브와 똑같다! 정합성은 DB가 책임진다.
	var balance int64
	if err := tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, accountID).Scan(&balance); err != nil {
		return "", notFound(err)
	}
	if balance < amount {
		return Insufficient, nil
	}
	time.Sleep(w.ReadWriteDelay)

	// 다른 SERIALIZABLE 트랜잭션이 이 행을 먼저 바꾸고 커밋했다면 여기서(또는 Commit 에서) 40001 이 난다.
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = $2 WHERE id = $1`, accountID, balance-amount); err != nil {
		return "", err
	}
	if err := recordWithdrawal(ctx, tx, accountID, amount); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return Success, nil
}

// ---------------------------------------------------------------------------
// 5) ConditionalUpdateWithdraw: 검사와 차감을 SQL 한 문장으로.  ✅ 실무에서 가장 흔한 답
// ---------------------------------------------------------------------------

type ConditionalUpdateWithdraw struct {
	DB *pgxpool.Pool
}

func (w *ConditionalUpdateWithdraw) Withdraw(ctx context.Context, accountID string, amount int64) (Result, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	// "읽고 → 검사 → 쓰기" 를 앱으로 가져오지 않는다. balance = balance - $2 는 UPDATE 시점의 최신 값으로 계산되고,
	// WHERE balance >= $2 도 그 최신 값으로 다시 평가된다 (READ COMMITTED 의 UPDATE 재확인 동작).
	// 그래서 레이스가 생길 틈 자체가 없다. 잠금은 이 한 문장 동안만.
	tag, err := tx.Exec(ctx,
		`UPDATE accounts SET balance = balance - $2 WHERE id = $1 AND balance >= $2`, accountID, amount)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return Insufficient, nil // (계좌가 없어도 0행이다. 구분이 필요하면 RETURNING 이나 별도 조회)
	}
	if err := recordWithdrawal(ctx, tx, accountID, amount); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return Success, nil
}

// notFound 는 "행 없음"을 ErrAccountNotFound 로 바꾼다.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrAccountNotFound
	}
	return err
}
