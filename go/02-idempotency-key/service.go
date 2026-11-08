package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// recovery_point 값. 요청 처리를 여러 "원자적 단계(atomic phase)"로 나누고,
// 어디까지 끝났는지를 DB에 기록해두면 재시도 때 그 지점부터 이어서 실행할 수 있다.
const (
	RecoveryStarted   = "started"    // 키만 만들어짐. PG 호출 전(또는 PG 결과를 모름)
	RecoveryPgCharged = "pg_charged" // PG 승인 + payments 저장 완료
	RecoveryFinished  = "finished"   // 응답까지 저장 완료 → 이후엔 저장된 응답만 돌려줌
)

// 서비스가 돌려주는 에러. 핸들러가 HTTP 상태 코드로 바꾼다.
var (
	ErrKeyReusedWithDifferentBody = errors.New("idempotency key reused with a different request body") // → 422
	ErrRequestInProgress          = errors.New("a request with this idempotency key is in progress")   // → 409
)

// PaymentRequest 는 POST /payments 바디.
type PaymentRequest struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

// PaymentResponse 는 201 응답 바디.
type PaymentResponse struct {
	PaymentID string `json:"paymentId"`
	OrderID   string `json:"orderId"`
	Amount    int64  `json:"amount"`
	PgTxID    string `json:"pgTxId"`
	Status    string `json:"status"`
}

// Result 는 HTTP 로 그대로 내보낼 응답 (상태 코드 + JSON 바디).
type Result struct {
	StatusCode int
	Body       []byte
}

// PaymentService 는 Brandur 의 rocket-rides-atomic 방식으로 멱등 결제를 처리한다.
type PaymentService struct {
	DB          *pgxpool.Pool
	Gateway     *FakePaymentGateway
	LockTimeout time.Duration // 이 시간보다 오래된 잠금은 "주인이 죽은 잠금"으로 보고 가져갈 수 있다
}

// keyRow 는 idempotency_keys 의 한 행 중 필요한 부분.
type keyRow struct {
	RequestHash   string
	RecoveryPoint string
	LockActive    bool
	ResponseCode  *int    // * 포인터 = NULL 가능 (Kotlin의 Int?)
	ResponseBody  []byte  // JSONB 를 바이트로 받는다
	PgTxID        *string // String?
}

// Pay 는 멱등키 하나에 대한 결제 요청을 처리한다.
// 같은 key 로 몇 번을 호출해도 PG 승인은 최대 1번, 응답은 항상 같다.
func (s *PaymentService) Pay(ctx context.Context, key, path string, rawBody []byte, req PaymentRequest) (Result, error) {
	hash := sha256Hex(rawBody)

	// ---- Phase 1 (tx): 키를 만들거나 찾아서 잠근다 ----
	row, err := s.lockKey(ctx, key, path, hash)
	if err != nil {
		return Result{}, err
	}
	if row.RecoveryPoint == RecoveryFinished {
		// 이미 끝난 요청: 저장해둔 응답을 그대로 돌려준다. PG 는 다시 호출하지 않는다.
		return Result{StatusCode: *row.ResponseCode, Body: row.ResponseBody}, nil
	}

	// 여기부터는 이 요청이 잠금을 쥐고 있다. 어떤 이유로든 실패하면 잠금을 풀어서 클라이언트가 재시도할 수 있게 한다.
	result, err := s.resume(ctx, key, row, req)
	if err != nil {
		// 원래 요청 ctx 가 취소됐을 수도 있으니, 잠금 해제는 새 ctx 로 한다.
		// context.WithoutCancel: 값은 물려받되 취소 신호는 끊는다.
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = s.DB.Exec(unlockCtx, `UPDATE idempotency_keys SET locked_at = NULL WHERE idempotency_key = $1`, key)
		return Result{}, err
	}
	return result, nil
}

// resume 는 recovery_point 부터 이어서 실행한다. 재시도라면 이미 끝난 단계는 건너뛴다.
func (s *PaymentService) resume(ctx context.Context, key string, row keyRow, req PaymentRequest) (Result, error) {
	// Go의 for 는 while 역할도 한다. switch 의 case 는 자동으로 break 된다 (Kotlin의 when 과 같음).
	for {
		switch row.RecoveryPoint {
		case RecoveryStarted:
			// ---- 외부 호출 (트랜잭션 밖) ----
			// 왜 트랜잭션 밖인가?
			//  1) PG 에서 일어난 승인은 DB 롤백으로 되돌릴 수 없다. tx 안에 넣어도 원자성은 생기지 않는다.
			//  2) 느린 외부 호출 동안 DB 커넥션/행 잠금을 쥐고 있으면 풀이 고갈된다.
			// 대신 "PG 의 멱등성"에 기댄다: 같은 key 로 다시 부르면 PG 가 새로 승인하지 않고 이전 결과를 준다.
			// 그래서 타임아웃 후 재시도해도 이중 결제가 안 된다.
			pgTxID, err := s.Gateway.Charge(ctx, key, req.OrderID, req.Amount)
			if err != nil {
				return Result{}, fmt.Errorf("pg charge: %w", err)
			}

			// ---- Phase 2 (tx): payments insert + recovery_point=pg_charged ----
			if err := s.savePayment(ctx, key, req, pgTxID); err != nil {
				return Result{}, err
			}
			row.RecoveryPoint = RecoveryPgCharged

		case RecoveryPgCharged:
			// ---- Phase 3 (tx): 응답 저장 + finished + 잠금 해제 ----
			return s.finish(ctx, key, req.OrderID)

		default:
			return Result{}, fmt.Errorf("unknown recovery point %q", row.RecoveryPoint)
		}
	}
}

// lockKey = Phase 1.
func (s *PaymentService) lockKey(ctx context.Context, key, path, hash string) (keyRow, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return keyRow{}, err
	}
	// Begin 직후 defer Rollback 은 pgx 의 관용구. Commit 이 성공한 뒤의 Rollback 은 아무것도 하지 않는다.
	defer tx.Rollback(ctx)

	// 동시에 같은 key 로 두 요청이 와도 UNIQUE 제약 때문에 한 행만 생긴다.
	// 늦게 온 쪽의 INSERT 는 먼저 온 tx 가 커밋될 때까지 기다렸다가 DO NOTHING 이 된다.
	if _, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (idempotency_key, request_path, request_hash, recovery_point)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING`, key, path, hash, RecoveryStarted); err != nil {
		return keyRow{}, err
	}

	// FOR UPDATE: 이 행을 tx 가 끝날 때까지 잠근다. 동시에 들어온 요청들은 여기서 한 줄로 선다.
	// 잠금 유효 여부는 DB 시계(now())로 계산해서 서버 간 시계 차이를 피한다.
	var row keyRow
	err = tx.QueryRow(ctx, `
		SELECT request_hash, recovery_point,
		       (locked_at IS NOT NULL AND locked_at > now() - make_interval(secs => $2)) AS lock_active,
		       response_code, response_body, pg_tx_id
		  FROM idempotency_keys
		 WHERE idempotency_key = $1
		   FOR UPDATE`, key, s.lockTimeout().Seconds()).
		Scan(&row.RequestHash, &row.RecoveryPoint, &row.LockActive, &row.ResponseCode, &row.ResponseBody, &row.PgTxID)
	if err != nil {
		return keyRow{}, err
	}

	switch {
	case row.RequestHash != hash:
		return keyRow{}, ErrKeyReusedWithDifferentBody // 같은 key 를 다른 요청에 재사용 → 클라이언트 버그
	case row.RecoveryPoint == RecoveryFinished:
		return row, nil // 잠글 필요 없음. 저장된 응답을 돌려준다.
	case row.LockActive:
		return keyRow{}, ErrRequestInProgress // 다른 요청이 처리 중
	}

	// 잠금 획득. 이 시점부터 다른 요청은 409 를 받는다.
	if _, err := tx.Exec(ctx,
		`UPDATE idempotency_keys SET locked_at = now(), last_run_at = now() WHERE idempotency_key = $1`, key); err != nil {
		return keyRow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return keyRow{}, err
	}
	return row, nil
}

// savePayment = Phase 2. payments 저장과 recovery_point 전진을 "한 트랜잭션"으로 묶는다.
// 둘 중 하나만 반영되는 일이 없으므로, 재시도 시 recovery_point 만 보고 어디서부터 할지 정확히 알 수 있다.
func (s *PaymentService) savePayment(ctx context.Context, key string, req PaymentRequest, pgTxID string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`INSERT INTO payments (id, order_id, amount, status, pg_tx_id) VALUES ($1, $2, $3, 'COMPLETED', $4)`,
		uuid.NewString(), req.OrderID, req.Amount, pgTxID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE idempotency_keys SET recovery_point = $2, pg_tx_id = $3 WHERE idempotency_key = $1`,
		key, RecoveryPgCharged, pgTxID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// finish = Phase 3. 응답을 만들어 저장하고 잠금을 푼다.
func (s *PaymentService) finish(ctx context.Context, key, orderID string) (Result, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(ctx)

	var resp PaymentResponse
	if err := tx.QueryRow(ctx,
		`SELECT id::text, order_id, amount, coalesce(pg_tx_id, ''), status FROM payments WHERE order_id = $1`, orderID).
		Scan(&resp.PaymentID, &resp.OrderID, &resp.Amount, &resp.PgTxID, &resp.Status); err != nil {
		return Result{}, err
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return Result{}, err
	}
	const code = 201
	// RETURNING 으로 "저장된" 바디를 다시 받아 응답한다. JSONB 는 키 순서/공백을 정규화하므로,
	// 이렇게 해야 첫 응답과 재시도 응답이 바이트 단위까지 똑같다.
	var stored []byte
	if err := tx.QueryRow(ctx, `
		UPDATE idempotency_keys
		   SET recovery_point = $2, response_code = $3, response_body = $4, locked_at = NULL
		 WHERE idempotency_key = $1
		RETURNING response_body`, key, RecoveryFinished, code, body).Scan(&stored); err != nil {
		return Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	return Result{StatusCode: code, Body: stored}, nil
}

func (s *PaymentService) lockTimeout() time.Duration {
	if s.LockTimeout <= 0 {
		return 30 * time.Second
	}
	return s.LockTimeout
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]) // sum[:] : 고정 길이 배열을 슬라이스(가변 길이 뷰)로 바꾼다
}

// CountPayments 는 테스트 검증용.
func CountPayments(ctx context.Context, db *pgxpool.Pool, orderID string) (int, error) {
	var n int
	err := db.QueryRow(ctx, `SELECT count(*) FROM payments WHERE order_id = $1`, orderID).Scan(&n)
	return n, err
}
