package idempotentconsumer

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ---------------------------------------------------------------------------
// 에러 분류: 실패한 메시지를 DLT 로 보낼지, 몇 번 더 해 볼지, 끝없이 기다릴지.
// Spring Kafka 의 ExceptionClassifier(not-retryable 목록) +
// FailedRecordTracker.setBackOffFunction(예외별 BackOff) 을 옮긴 것.
// ---------------------------------------------------------------------------

// errorClass 는 처리 실패의 종류.
type errorClass int

const (
	// classRetryable: 다시 하면 될 수도, 안 될 수도 있다.
	// MaxAttempts 번까지 세고 넘으면 DLT. 깨진 JSON, 검증 실패,
	// 그리고 "모르는 에러" 전부 (기본값 — 아래 classify 설명).
	classRetryable errorClass = iota
	// classNotRetryable: 몇 번을 다시 해도 결과가 같다 → 1번 만에 DLT.
	// (Spring ExceptionClassifier 의 not-retryable)
	classNotRetryable
	// classTransient: 메시지가 아니라 인프라(DB·네트워크)가 아프다.
	// 메시지는 멀쩡하므로 DLT 로 보내지 않는다. 시도 횟수도 세지 않고,
	// Run 이 지수 백오프로 끝없이 재시도한다 (장애가 풀릴 때까지 파티션 정지).
	classTransient
)

func (c errorClass) String() string {
	switch c {
	case classNotRetryable:
		return "not-retryable"
	case classTransient:
		return "transient"
	default:
		return "retryable"
	}
}

// classify 는 err 체인 전체(errors.Is/As 는 %w 로 감싼 원인까지 본다 —
// Spring 의 setTraverseCauses(true) 와 같다)를 보고 종류를 정한다.
//
// 판정 순서:
//  1. not-retryable 이 먼저 — 메시지 자체가 틀렸으면 인프라 상태와 무관하다.
//  2. transient — 아래 isTransient 목록.
//  3. 나머지는 retryable (MaxAttempts 까지 세고 DLT).
//
// "모르는 에러 = retryable" 인 이유 (Spring ExceptionClassifier 의 기본값
// defaultValue=true 와 같은 선택):
//   - 모르는 에러를 transient 로 두면, 특정 payload 에서만 터지는 버그
//     (nil 역참조, 범위 밖 값, 제약 위반)가 파티션을 영원히 멈춘다.
//     DLT 가 막으려던 바로 그 상황이다.
//   - retryable 로 두면 최악이 "분류 못 한 인프라 에러로 멀쩡한 메시지가 DLT
//     에 간다" 인데, DLT 에는 원래 key·value·헤더와 위치가 그대로 남으니
//     유실은 아니다 (손으로 되돌릴 수 있다). 그 경우는 isTransient 에 추가한다.
func classify(err error) errorClass {
	switch {
	case errors.Is(err, ErrMissingMessageID):
		return classNotRetryable
	case isTransient(err):
		return classTransient
	default:
		return classRetryable
	}
}

// isTransient 는 "메시지 탓이 아니라 인프라 탓" 인 에러인가.
func isTransient(err error) bool {
	// 처리 도중 타임아웃 / 종료 신호. 메시지와는 무관하다.
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return true
	}
	// Postgres 에 연결 자체를 못 함 (DB 재시작, 네트워크 단절).
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	// pgx 가 "서버에 아무것도 안 보냈다" / "타임아웃" 이라고 표시한 에러.
	if pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	// 서버가 보낸 에러는 SQLSTATE 로 판단한다.
	// 제약 위반(23xxx), 문법(42xxx) 같은 건 결정적 → retryable.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return transientSQLState(pgErr.Code)
	}
	// TCP 수준 에러 (connection refused/reset, i/o timeout).
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// 쿼리 도중 연결이 끊김. (json.Unmarshal 은 io.EOF 를 돌려주지 않는다)
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// transientSQLState 는 "기다리면 풀리는" Postgres 에러 코드인가.
// https://www.postgresql.org/docs/16/errcodes-appendix.html
func transientSQLState(code string) bool {
	switch {
	case strings.HasPrefix(code, "08"): // connection_exception
		return true
	case strings.HasPrefix(code, "53"): // insufficient_resources (too_many_connections 등)
		return true
	}
	switch code {
	case "57P01", // admin_shutdown
		"57P02", // crash_shutdown
		"57P03", // cannot_connect_now (기동 중)
		"57014", // query_canceled (statement_timeout)
		"40001", // serialization_failure
		"40P01": // deadlock_detected
		return true
	}
	return false
}

// DefaultMaxRetryDelay 는 재시도 간격의 상한.
const DefaultMaxRetryDelay = 30 * time.Second

// retryBackoff 는 연속 failures 번째 실패 뒤 기다릴 시간.
// base, 2·base, 4·base, ... 로 늘다가 max 에서 멈춘다 (Spring ExponentialBackOff,
// multiplier 2, maxInterval). failures 는 1 부터.
func retryBackoff(base, max time.Duration, failures int) time.Duration {
	d := base
	for i := 1; i < failures; i++ {
		d *= 2
		if d >= max || d <= 0 { // d <= 0: 오버플로 방어
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}
