package ledger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 계정 ID. 복식부기에서는 "돈이 어디서 와서 어디로 갔는지"를 항상 두 쪽 이상에 기록한다.
const (
	AccountPgReceivable = "pg:receivable"        // 자산: PG 사가 우리에게 줄 돈
	AccountBankCash     = "bank:cash"            // 자산: 은행 잔고
	AccountFeeRevenue   = "platform:fee_revenue" // 수익: 우리가 번 수수료
)

// MerchantPayable 은 가맹점별 부채 계정: 우리가 가맹점에 줘야 할 돈.
func MerchantPayable(merchantID string) string {
	return "merchant:" + merchantID + ":payable"
}

// 참조 타입 (journal_entries.reference_type)
const (
	RefPayment   = "PAYMENT"
	RefRefund    = "REFUND"
	RefPgDeposit = "PG_DEPOSIT"
	RefPayout    = "PAYOUT"
)

// Line 은 분개의 한 줄. Debit 과 Credit 중 정확히 하나만 0보다 크다.
type Line struct {
	AccountID string
	Debit     int64 // 차변
	Credit    int64 // 대변
}

// Dr / Cr 은 Line 을 읽기 좋게 만드는 헬퍼: Dr("bank:cash", 1000) = 차변 1000
func Dr(account string, amount int64) Line { return Line{AccountID: account, Debit: amount} }
func Cr(account string, amount int64) Line { return Line{AccountID: account, Credit: amount} }

// JournalEntry 는 하나의 거래에 대한 분개 (여러 줄).
type JournalEntry struct {
	ReferenceType string
	ReferenceID   string
	Description   string
	Lines         []Line
}

var ErrUnbalancedEntry = errors.New("unbalanced journal entry: debits != credits")

// ErrUnknownAccount: 계정과목에 없는 계정 ID.
// (Kotlin 의 IllegalArgumentException("알 수 없는 계정"))
var ErrUnknownAccount = errors.New("unknown account")

// Validate 는 분개 규칙을 검사한다. 가장 중요한 규칙: 차변 합계 == 대변 합계.
// 이 규칙 덕분에 원장 전체에서 "잔액 합 = 0" 이 항상 유지된다 (돈이 생기거나 사라지지 않는다).
func (e JournalEntry) Validate() error {
	if len(e.Lines) < 2 {
		return fmt.Errorf("%w: need at least 2 lines", ErrUnbalancedEntry)
	}
	var debits, credits int64
	for _, l := range e.Lines {
		if l.Debit < 0 || l.Credit < 0 || (l.Debit == 0) == (l.Credit == 0) {
			return fmt.Errorf("invalid line %+v: exactly one of debit/credit must be positive", l)
		}
		debits += l.Debit
		credits += l.Credit
	}
	if debits != credits {
		return fmt.Errorf("%w: debit=%d credit=%d", ErrUnbalancedEntry, debits, credits)
	}
	return nil
}

// LedgerService 는 분개를 원장에 전기(post)한다.
type LedgerService struct {
	DB *pgxpool.Pool
}

// Post 는 분개를 자체 트랜잭션으로 전기한다. 이미 전기된 참조면 (false, nil) — 멱등.
func (s *LedgerService) Post(ctx context.Context, e JournalEntry) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	posted, err := PostTx(ctx, tx, e)
	if err != nil {
		return false, err
	}
	return posted, tx.Commit(ctx)
}

// PostTx 는 호출자의 트랜잭션 안에서 전기한다 (정산 지급처럼 다른 갱신과 원자적으로 묶을 때).
func PostTx(ctx context.Context, tx pgx.Tx, e JournalEntry) (bool, error) {
	if err := e.Validate(); err != nil {
		return false, err
	}

	// 1) 분개 헤더. UNIQUE(reference_type, reference_id) 덕분에 같은 거래를 두 번 전기할 수 없다.
	//    이벤트가 중복 도착해도(05 단계의 at-least-once) 원장은 한 번만 반영된다.
	entryID := uuid.NewString()
	tag, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, reference_type, reference_id, description) VALUES ($1, $2, $3, $4)
		ON CONFLICT (reference_type, reference_id) DO NOTHING`,
		entryID, e.ReferenceType, e.ReferenceID, e.Description)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil // 이미 전기됨
	}

	// 2) 계정 잔액 갱신은 "계정 ID 순서"로 한다 → 동시에 여러 분개가 같은 계정들을 건드려도 데드락이 없다 (06 단계의 정렬 잠금).
	//    원본 e.Lines 순서는 건드리지 않도록 복사(slices.Clone) 후 정렬한다.
	lines := slices.Clone(e.Lines)
	slices.SortFunc(lines, func(a, b Line) int { return strings.Compare(a.AccountID, b.AccountID) })

	for _, l := range lines {
		if err := ensureAccount(ctx, tx, l.AccountID); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ledger_lines (entry_id, account_id, debit, credit) VALUES ($1, $2, $3, $4)`,
			entryID, l.AccountID, l.Debit, l.Credit); err != nil {
			return false, err
		}
		// debit-positive 부호 규칙: balance += 차변 - 대변.
		// 자산은 보통 +, 부채/수익은 보통 - 가 된다. 이렇게 하면 모든 계정 잔액의 합이 항상 0 (시산표 검증).
		if _, err := tx.Exec(ctx,
			`UPDATE ledger_accounts SET balance = balance + $2 - $3 WHERE id = $1`,
			l.AccountID, l.Debit, l.Credit); err != nil {
			return false, err
		}
	}
	return true, nil
}

// ensureAccount 는 가맹점 계정을 처음 쓸 때 만든다.
func ensureAccount(ctx context.Context, tx pgx.Tx, id string) error {
	typ, err := accountTypeOf(id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_accounts (id, type) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING`, id, typ)
	return err
}

// accountTypeOf 는 계정 ID 로 유형을 정한다. 모르는 ID 는 에러.
// "나머지는 다 ASSET" 으로 두면 오타 난 계정("bank:cahs")이 조용히 생기고,
// 시산표 합은 여전히 0 이라 아무도 모른다.
func accountTypeOf(id string) (string, error) {
	switch {
	case id == AccountPgReceivable, id == AccountBankCash:
		return "ASSET", nil
	case id == AccountFeeRevenue:
		return "REVENUE", nil
	case strings.HasPrefix(id, "merchant:") &&
		strings.HasSuffix(id, ":payable"):
		return "LIABILITY", nil
	default:
		// %w 로 감싸면 호출자가 errors.Is(err, ErrUnknownAccount) 로 판별 가능
		return "", fmt.Errorf("%w: %s", ErrUnknownAccount, id)
	}
}

// Balance 는 계정 잔액 (테스트/리포트용).
func (s *LedgerService) Balance(ctx context.Context, accountID string) (int64, error) {
	var b int64
	err := s.DB.QueryRow(ctx, `SELECT balance FROM ledger_accounts WHERE id = $1`, accountID).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return b, err
}
