package ledger

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultFeeRateBps 는 기본 수수료율. bps(basis point) = 1/10000. 300bps = 3%.
// 비율을 정수(bps)로 들고 다니면 float 없이 계산할 수 있다.
const DefaultFeeRateBps = 300

// CalculateFee 는 원 단위 HALF_UP 반올림 수수료.
//
//	fee = (amount × rateBps + 5000) / 10000
//
// 정수 나눗셈은 버림이므로, 나누기 전에 절반(5000)을 더하면 반올림이 된다. 예) 1,050원 × 3% = 31.5 → 32원.
// float 는 틀릴 수 있다: 5,750원 × 2.2% 는 126.5 → 127원이어야 하는데,
// float64 로 5750 * 0.022 = 126.49999999999999 → math.Round 는 126원.
// → 금액에 float 금지.
// (0.022 를 2진수로 정확히 표현할 수 없어서. 1050 * 0.03 은 우연히 정확히 31.5 가 나온다)
func CalculateFee(amount, rateBps int64) int64 {
	return (amount*rateBps + 5000) / 10000
}

// Transaction 은 결제/환불 원천 거래.
type Transaction struct {
	ID         string
	MerchantID string
	Type       string // PAYMENT | REFUND
	PgTxID     string
	Amount     int64
	Fee        int64
	OccurredAt time.Time
}

// TransactionService 는 원천 거래를 기록하고 원장에 분개한다.
type TransactionService struct {
	DB         *pgxpool.Pool
	FeeRateBps int64
}

func (s *TransactionService) rate() int64 {
	if s.FeeRateBps == 0 {
		return DefaultFeeRateBps
	}
	return s.FeeRateBps
}

// RecordPayment 는 결제 승인 1건을 기록한다:
//
//	Dr pg:receivable          amount        (PG 가 우리에게 줄 돈이 생김)
//	  Cr merchant:{id}:payable  amount - fee  (그중 가맹점 몫은 우리가 줄 돈)
//	  Cr platform:fee_revenue   fee           (나머지는 우리 수익)
func (s *TransactionService) RecordPayment(ctx context.Context, merchantID, pgTxID string, amount int64, occurredAt time.Time) (Transaction, error) {
	t := Transaction{ID: uuid.NewString(), MerchantID: merchantID, Type: RefPayment, PgTxID: pgTxID,
		Amount: amount, Fee: CalculateFee(amount, s.rate()), OccurredAt: occurredAt}
	return t, s.record(ctx, t, PaymentEntry(t))
}

// RecordRefund 는 환불을 기록한다. 결제 분개의 역분개(차변/대변을 뒤집음)이고, 환불 금액에 대한 수수료도 돌려준다.
//
//	Dr merchant:{id}:payable  amount - fee
//	Dr platform:fee_revenue   fee
//	  Cr pg:receivable          amount
func (s *TransactionService) RecordRefund(ctx context.Context, merchantID, pgTxID string, amount int64, occurredAt time.Time) (Transaction, error) {
	t := Transaction{ID: uuid.NewString(), MerchantID: merchantID, Type: RefRefund, PgTxID: pgTxID,
		Amount: amount, Fee: CalculateFee(amount, s.rate()), OccurredAt: occurredAt}
	return t, s.record(ctx, t, RefundEntry(t))
}

// record 는 원천 거래 insert 와 분개 전기를 한 트랜잭션으로 묶는다 (거래는 있는데 원장엔 없는 상태 방지).
//
// 순서가 중요하다: 분개를 먼저 전기하고, "새로 전기됐을 때만" 원천 거래를 넣는다.
// 분개의 UNIQUE(reference_type, reference_id) 가 멱등성의 기준점이다.
// 거래를 먼저 넣고 PostTx 의 false(이미 전기됨)를 무시하면, 같은 결제가 두 번 올 때
// 원장은 1번인데 transactions 는 2행 → 그날 정산 gross 가 2배가 된다.
// (Kotlin 의 `if (posted.created) insertTransaction(...)` 과 같다)
func (s *TransactionService) record(ctx context.Context, t Transaction, e JournalEntry) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	posted, err := PostTx(ctx, tx, e)
	if err != nil {
		return err
	}
	if !posted {
		return nil // 이미 기록된 거래 (중복 메시지) → 아무것도 하지 않음
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO transactions (id, merchant_id, type, pg_tx_id, amount, fee, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		t.ID, t.MerchantID, t.Type, t.PgTxID, t.Amount, t.Fee, t.OccurredAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// PaymentEntry 는 결제 승인 분개. reference_id 는 PG 거래 ID → 같은 PG 거래는 한 번만 전기된다.
func PaymentEntry(t Transaction) JournalEntry {
	return JournalEntry{
		ReferenceType: RefPayment, ReferenceID: t.PgTxID, Description: "payment approved",
		Lines: []Line{
			Dr(AccountPgReceivable, t.Amount),
			Cr(MerchantPayable(t.MerchantID), t.Amount-t.Fee),
			Cr(AccountFeeRevenue, t.Fee),
		},
	}
}

// RefundEntry 는 PaymentEntry 의 차변/대변을 정확히 뒤집은 것.
func RefundEntry(t Transaction) JournalEntry {
	return JournalEntry{
		ReferenceType: RefRefund, ReferenceID: t.PgTxID, Description: "refund",
		Lines: []Line{
			Dr(MerchantPayable(t.MerchantID), t.Amount-t.Fee),
			Dr(AccountFeeRevenue, t.Fee),
			Cr(AccountPgReceivable, t.Amount),
		},
	}
}
