package observability

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 결제 상태 (07 의 상태 머신을 간소화)
const (
	StatusPending  = "PENDING"
	StatusApproved = "APPROVED"
	StatusFailed   = "FAILED"
	StatusUnknown  = "UNKNOWN"
)

// Payment 는 payments 한 행.
type Payment struct {
	ID      string `json:"paymentId"`
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
	Status  string `json:"status"`
	PgTxID  string `json:"pgTxId,omitempty"` // omitempty: 빈 값이면 JSON 에서 생략
}

// PaymentService 는 결제 처리 + 관측(로그, 메트릭).
type PaymentService struct {
	DB        *pgxpool.Pool
	Gateway   *FakePaymentGateway
	Metrics   *Metrics
	Log       *slog.Logger
	PgTimeout time.Duration
}

// Pay 는 결제를 처리하고 (결제, 결과 라벨, 에러)를 돌려준다. 결과 라벨은 payment_requests_total 의 result 값.
func (s *PaymentService) Pay(ctx context.Context, key, orderID string, amount int64) (Payment, string, error) {
	p, result, err := s.pay(ctx, key, orderID, amount)
	if err == nil {
		s.Metrics.PaymentRequests.WithLabelValues(result).Inc()
	}
	return p, result, err
}

func (s *PaymentService) pay(ctx context.Context, key, orderID string, amount int64) (Payment, string, error) {
	ctx = AddLogAttrs(ctx, slog.String("idempotency_key", key), slog.String("order_id", orderID))
	s.Log.InfoContext(ctx, "payment requested", "amount", amount)

	// 1) 멱등키로 결제 행 선점. 이미 있으면 replay(완료) 또는 conflict(처리 중).
	p := Payment{ID: uuid.NewString(), OrderID: orderID, Amount: amount, Status: StatusPending}
	tag, err := s.DB.Exec(ctx, `
		INSERT INTO payments (id, order_id, amount, status, idempotency_key) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO NOTHING`, p.ID, orderID, amount, p.Status, key)
	if err != nil {
		return Payment{}, "", err
	}
	if tag.RowsAffected() == 0 {
		return s.replay(ctx, key)
	}
	ctx = AddLogAttrs(ctx, slog.String("payment_id", p.ID))

	// 2) PG 호출. 시간을 재서 히스토그램에 기록 (성공이든 실패든 타임아웃이든 전부).
	pgCtx, cancel := context.WithTimeout(ctx, s.pgTimeout())
	defer cancel()
	start := time.Now()
	pgTxID, pgErr := s.Gateway.Charge(pgCtx, key, orderID, amount)
	elapsed := time.Since(start)
	s.Metrics.PgCallDuration.Observe(elapsed.Seconds())

	var result string
	switch {
	case pgErr == nil:
		p.Status, p.PgTxID, result = StatusApproved, pgTxID, ResultApproved
	case errors.Is(pgErr, ErrPgDeclined):
		p.Status, result = StatusFailed, ResultFailed
	default:
		// 타임아웃: 승인됐을 수도 있다 → UNKNOWN (07 의 janitor 가 PG 조회로 확정)
		p.Status, result = StatusUnknown, ResultUnknown
	}
	// 로그는 key-value 쌍으로 남긴다. JSON 핸들러가 {"pg_duration_ms":123,...} 로 출력한다.
	level := slog.LevelInfo
	if pgErr != nil {
		level = slog.LevelWarn
	}
	s.Log.Log(ctx, level, "pg call finished",
		"pg_duration_ms", elapsed.Milliseconds(), "status", p.Status, "error", errString(pgErr))

	if _, err := s.DB.Exec(ctx, `
		UPDATE payments SET status = $2, pg_tx_id = nullif($3, ''), last_error = nullif($4, ''), updated_at = now()
		 WHERE id = $1`, p.ID, p.Status, p.PgTxID, errString(pgErr)); err != nil {
		return Payment{}, "", err
	}
	s.Log.InfoContext(ctx, "payment completed", "status", p.Status)
	return p, result, nil
}

// replay 는 같은 멱등키의 기존 결제를 돌려준다.
func (s *PaymentService) replay(ctx context.Context, key string) (Payment, string, error) {
	var p Payment
	err := s.DB.QueryRow(ctx,
		`SELECT id::text, order_id, amount, status, coalesce(pg_tx_id, '') FROM payments WHERE idempotency_key = $1`, key).
		Scan(&p.ID, &p.OrderID, &p.Amount, &p.Status, &p.PgTxID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, "", errors.New("idempotency key vanished")
	}
	if err != nil {
		return Payment{}, "", err
	}
	ctx = AddLogAttrs(ctx, slog.String("payment_id", p.ID))

	if p.Status == StatusPending {
		s.Log.WarnContext(ctx, "duplicate request while in progress")
		return p, ResultConflict, nil
	}
	s.Metrics.IdempotencyReplays.Inc()
	s.Log.InfoContext(ctx, "idempotent replay", "status", p.Status)
	return p, ResultReplayed, nil
}

func (s *PaymentService) pgTimeout() time.Duration {
	if s.PgTimeout <= 0 {
		return 2 * time.Second
	}
	return s.PgTimeout
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
