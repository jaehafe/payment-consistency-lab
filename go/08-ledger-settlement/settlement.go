package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"
	_ "time/tzdata" // 타임존 DB 를 바이너리에 포함한다. OS 에 tzdata 가 없는 최소 컨테이너에서도 LoadLocation 이 동작하게.

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// KST 는 한국 표준시. time.LoadLocation 은 (값, 에러)를 돌려주는데, 패키지 초기화 시점에 한 번만 읽는다.
// Kotlin으로 치면 ZoneId.of("Asia/Seoul").
var KST = mustLoadLocation("Asia/Seoul")

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err) // 프로그램 시작 시 설정 오류는 바로 죽이는 게 낫다 (Kotlin의 error(...) / check(...))
	}
	return loc
}

// KSTDayRange 는 KST 거래일 하루의 [시작, 끝) 시각을 돌려준다. date 는 연/월/일만 쓴다 (Kotlin의 LocalDate 역할).
//
// 왜 중요한가? DB 에는 UTC 로 저장된다. "11월 8일 거래" 를 UTC 날짜로 자르면
// KST 11/8 00:00~08:59 거래(= UTC 11/7 15:00~23:59)가 11/7 정산에 들어가 버린다.
func KSTDayRange(date time.Time) (time.Time, time.Time) {
	y, m, d := date.Date()
	start := time.Date(y, m, d, 0, 0, 0, 0, KST) // KST 자정 = UTC 전날 15:00
	return start, start.AddDate(0, 0, 1)
}

// Settlement 는 settlements 한 행.
type Settlement struct {
	ID           string
	MerchantID   string
	Date         string // YYYY-MM-DD
	GrossAmount  int64
	RefundAmount int64
	FeeAmount    int64
	NetAmount    int64
	Status       string
}

// SettlementService 는 일 정산, PG 입금, 가맹점 지급을 처리한다.
type SettlementService struct {
	DB *pgxpool.Pool
}

// Settle 은 KST 거래일의 미정산 거래를 가맹점별로 집계해 settlements 를 만든다.
// 같은 날짜로 다시 실행해도 (1) 이미 정산된 거래는 settlement_id 가 있어 제외되고
// (2) UNIQUE(merchant_id, settlement_date) 로 같은 정산이 두 번 생기지 않는다.
func (s *SettlementService) Settle(ctx context.Context, date time.Time) ([]Settlement, error) {
	start, end := KSTDayRange(date)
	day := date.Format("2006-01-02") // Go 날짜 포맷은 "2006-01-02 15:04:05" 라는 기준 시각으로 쓴다 (yyyy-MM-dd 대신)

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 1) 대상 거래를 잠근다. 정산이 동시에 두 번 돌아도 같은 거래를 두 번 집계하지 않게.
	if _, err := tx.Exec(ctx, `
		SELECT id FROM transactions
		 WHERE occurred_at >= $1 AND occurred_at < $2 AND settlement_id IS NULL
		   FOR UPDATE`, start, end); err != nil {
		return nil, err
	}

	// 2) 가맹점별 집계. FILTER (WHERE ...) 는 조건부 합계 (SUM(CASE WHEN ...) 의 깔끔한 버전).
	rows, err := tx.Query(ctx, `
		SELECT merchant_id,
		       coalesce(sum(amount) FILTER (WHERE type = 'PAYMENT'), 0) AS gross,
		       coalesce(sum(amount) FILTER (WHERE type = 'REFUND'), 0)  AS refund,
		       coalesce(sum(fee) FILTER (WHERE type = 'PAYMENT'), 0)
		     - coalesce(sum(fee) FILTER (WHERE type = 'REFUND'), 0)     AS net_fee
		  FROM transactions
		 WHERE occurred_at >= $1 AND occurred_at < $2 AND settlement_id IS NULL
		 GROUP BY merchant_id
		 ORDER BY merchant_id`, start, end)
	if err != nil {
		return nil, err
	}
	aggs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Settlement, error) {
		st := Settlement{Date: day, Status: "CALCULATED"}
		err := r.Scan(&st.MerchantID, &st.GrossAmount, &st.RefundAmount, &st.FeeAmount)
		st.NetAmount = st.GrossAmount - st.RefundAmount - st.FeeAmount
		return st, err
	})
	if err != nil {
		return nil, err
	}

	var created []Settlement
	for _, st := range aggs {
		st.ID = uuid.NewString()
		tag, err := tx.Exec(ctx, `
			INSERT INTO settlements (id, merchant_id, settlement_date, gross_amount, refund_amount, fee_amount, net_amount, status)
			VALUES ($1, $2, $3::date, $4, $5, $6, $7, $8)
			ON CONFLICT (merchant_id, settlement_date) DO NOTHING`,
			st.ID, st.MerchantID, day, st.GrossAmount, st.RefundAmount, st.FeeAmount, st.NetAmount, st.Status)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			continue // 이 가맹점/날짜는 이미 정산됨 (늦게 들어온 거래는 README 의 한계 참고)
		}
		// 3) 거래에 정산 ID 를 찍어 "정산 완료" 표시
		if _, err := tx.Exec(ctx, `
			UPDATE transactions SET settlement_id = $1
			 WHERE merchant_id = $2 AND occurred_at >= $3 AND occurred_at < $4 AND settlement_id IS NULL`,
			st.ID, st.MerchantID, start, end); err != nil {
			return nil, err
		}
		created = append(created, st)
	}
	return created, tx.Commit(ctx)
}

// RecordPgDeposit 은 PG 사가 정산금을 우리 은행 계좌로 입금한 것을 기록한다:
//
//	Dr bank:cash  /  Cr pg:receivable   (받을 돈이 실제 현금으로 바뀜)
func (s *SettlementService) RecordPgDeposit(ctx context.Context, depositID string, amount int64) error {
	_, err := (&LedgerService{DB: s.DB}).Post(ctx, JournalEntry{
		ReferenceType: RefPgDeposit, ReferenceID: depositID, Description: "pg settlement deposit",
		Lines: []Line{Dr(AccountBankCash, amount), Cr(AccountPgReceivable, amount)},
	})
	return err
}

var ErrAlreadyPaid = errors.New("settlement already paid")

// Payout 은 가맹점에 정산금을 지급(출금)한다:
//
//	Dr merchant:{id}:payable  /  Cr bank:cash   (줄 돈이 줄고, 현금이 나감)
//
// 분개 전기와 settlements.status=PAID 를 한 트랜잭션으로 → "돈은 나갔는데 PAID 아님" 이 없다.
func (s *SettlementService) Payout(ctx context.Context, settlementID string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var merchantID, status string
	var net int64
	if err := tx.QueryRow(ctx,
		`SELECT merchant_id, net_amount, status FROM settlements WHERE id = $1 FOR UPDATE`, settlementID).
		Scan(&merchantID, &net, &status); err != nil {
		return err
	}
	if status == "PAID" {
		return ErrAlreadyPaid
	}
	if net > 0 { // 환불이 더 많아 net <= 0 이면 지급할 돈이 없다 (실무에선 다음 정산에서 상계)
		if _, err := PostTx(ctx, tx, JournalEntry{
			ReferenceType: RefPayout, ReferenceID: settlementID, Description: fmt.Sprintf("payout to %s", merchantID),
			Lines: []Line{Dr(MerchantPayable(merchantID), net), Cr(AccountBankCash, net)},
		}); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE settlements SET status = 'PAID' WHERE id = $1`, settlementID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
