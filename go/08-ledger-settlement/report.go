package ledger

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MerchantDaily 는 가맹점별 일 정산 요약.
type MerchantDaily struct {
	MerchantID              string
	Gross, Refund, Fee, Net int64
	Status                  string
}

// TrialBalanceLine 은 시산표 한 줄: 계정별 차변 합계, 대변 합계, 잔액.
type TrialBalanceLine struct {
	AccountID   string
	Type        string
	DebitTotal  int64
	CreditTotal int64
	Balance     int64
}

// DailyReport 는 일일 리포트.
type DailyReport struct {
	Date         string
	Merchants    []MerchantDaily
	TrialBalance []TrialBalanceLine
	// 아래 세 값이 시산표 검증: TotalDebit == TotalCredit, SumOfBalances == 0 이어야 원장이 건강하다.
	TotalDebit    int64
	TotalCredit   int64
	SumOfBalances int64
}

// ReportQueries 는 리포트용 조회 모음.
type ReportQueries struct {
	DB *pgxpool.Pool
}

// DailyReport 는 date(KST 거래일)의 가맹점별 정산과 현재 시점의 시산표를 만든다.
func (q *ReportQueries) DailyReport(ctx context.Context, date time.Time) (DailyReport, error) {
	rep := DailyReport{Date: date.Format("2006-01-02")}

	rows, err := q.DB.Query(ctx, `
		SELECT merchant_id, gross_amount, refund_amount, fee_amount, net_amount, status
		  FROM settlements WHERE settlement_date = $1::date ORDER BY merchant_id`, rep.Date)
	if err != nil {
		return rep, err
	}
	rep.Merchants, err = pgx.CollectRows(rows, pgx.RowToStructByPos[MerchantDaily])
	if err != nil {
		return rep, err
	}

	// 시산표(trial balance): 모든 계정의 차변/대변 합계. 복식부기가 제대로 됐다면 두 합계가 같다.
	rows, err = q.DB.Query(ctx, `
		SELECT a.id, a.type,
		       coalesce(sum(l.debit), 0)::bigint  AS debit_total,
		       coalesce(sum(l.credit), 0)::bigint AS credit_total,
		       a.balance
		  FROM ledger_accounts a
		  LEFT JOIN ledger_lines l ON l.account_id = a.id
		 GROUP BY a.id, a.type, a.balance
		 ORDER BY a.id`)
	if err != nil {
		return rep, err
	}
	rep.TrialBalance, err = pgx.CollectRows(rows, pgx.RowToStructByPos[TrialBalanceLine])
	if err != nil {
		return rep, err
	}
	for _, l := range rep.TrialBalance {
		rep.TotalDebit += l.DebitTotal
		rep.TotalCredit += l.CreditTotal
		rep.SumOfBalances += l.Balance
	}
	return rep, nil
}
