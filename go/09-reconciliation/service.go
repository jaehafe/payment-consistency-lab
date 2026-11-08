package recon

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Kind 는 대사 결과 종류.
type Kind string

const (
	Matched             Kind = "MATCHED" // 일치 (recon_results 에는 저장하지 않음)
	MissingInInternal   Kind = "MISSING_IN_INTERNAL"
	MissingInPG         Kind = "MISSING_IN_PG"
	AmountMismatch      Kind = "AMOUNT_MISMATCH"
	DuplicateInPG       Kind = "DUPLICATE_IN_PG"
	DuplicateInInternal Kind = "DUPLICATE_IN_INTERNAL"
)

// AllDiscrepancyKinds 는 리포트에 항상 표시할 순서.
var AllDiscrepancyKinds = []Kind{MissingInInternal, MissingInPG, AmountMismatch, DuplicateInPG, DuplicateInInternal}

// Discrepancy 는 불일치 한 건. 금액 NULL 가능 → 포인터 (*int64 = Kotlin의 Long?)
type Discrepancy struct {
	Kind           Kind
	PgTxID         string
	InternalAmount *int64
	PgAmount       *int64
	DiffAmount     int64 // pg - internal
}

// ReconReport 는 대사 1회 실행 결과.
type ReconReport struct {
	RunID         string
	BusinessDate  string
	Matched       int
	Counts        map[Kind]int
	TotalDiff     int64 // 차액 합계 (pg - internal)
	TotalAbsDiff  int64 // |차액| 합계: +/- 가 상쇄되어 0 처럼 보이는 착시를 막기 위해
	Discrepancies []Discrepancy
}

// ReconciliationService 는 우리 DB(internal_transactions)와 PG 정산 파일(pg_settlement_records)을 맞춰본다.
type ReconciliationService struct {
	DB *pgxpool.Pool
}

// Run 은 businessDate(KST) 의 대사를 실행하고 결과를 recon_results 에 저장한다.
func (s *ReconciliationService) Run(ctx context.Context, businessDate time.Time) (ReconReport, error) {
	day := businessDate.Format("2006-01-02")
	rep := ReconReport{RunID: uuid.NewString(), BusinessDate: day, Counts: map[Kind]int{}}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return rep, err
	}
	defer tx.Rollback(ctx)

	// recon_queries.sql 의 이름 붙은 쿼리 3개를 차례로 실행한다. 컬럼 모양이 같아서 같은 코드로 읽는다.
	for _, name := range []string{"compare_by_pg_tx_id", "duplicate_in_pg", "duplicate_in_internal"} {
		rows, err := tx.Query(ctx, query(name), day)
		if err != nil {
			return rep, fmt.Errorf("%s: %w", name, err)
		}
		found, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Discrepancy])
		if err != nil {
			return rep, fmt.Errorf("%s: %w", name, err)
		}
		for _, d := range found {
			if d.Kind == Matched {
				rep.Matched++
				continue
			}
			rep.Counts[d.Kind]++
			rep.TotalDiff += d.DiffAmount
			rep.TotalAbsDiff += abs(d.DiffAmount)
			rep.Discrepancies = append(rep.Discrepancies, d)
			if _, err := tx.Exec(ctx, `
				INSERT INTO recon_results (run_id, business_date, kind, pg_tx_id, internal_amount, pg_amount, diff_amount)
				VALUES ($1, $2::date, $3, $4, $5, $6, $7)`,
				rep.RunID, day, string(d.Kind), d.PgTxID, d.InternalAmount, d.PgAmount, d.DiffAmount); err != nil {
				return rep, err
			}
		}
	}
	return rep, tx.Commit(ctx)
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// Markdown 은 사람이 읽을 리포트를 만든다 (슬랙/메일에 그대로 붙일 수 있게).
//
// func (r ReconReport) 처럼 포인터(*)가 아닌 "값 리시버"는 구조체 복사본으로 동작한다 → 원본을 바꾸지 않는 읽기 전용 메서드에 쓴다.
func (r ReconReport) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# 대사 리포트 %s\n\n", r.BusinessDate) // Fprintf: 지정한 곳(b)에 포맷 출력
	fmt.Fprintf(&b, "- run_id: `%s`\n", r.RunID)
	fmt.Fprintf(&b, "- 일치: %d건\n", r.Matched)
	fmt.Fprintf(&b, "- 불일치: %d건 (차액 합계 %s원, 절대값 합계 %s원)\n\n", len(r.Discrepancies), won(r.TotalDiff), won(r.TotalAbsDiff))

	b.WriteString("| 종류 | 건수 |\n|---|---:|\n")
	for _, k := range AllDiscrepancyKinds {
		fmt.Fprintf(&b, "| %s | %d |\n", k, r.Counts[k]) // 없는 key 는 0 (map 의 제로 값)
	}

	if len(r.Discrepancies) > 0 {
		b.WriteString("\n| 종류 | pg_tx_id | 내부 금액 | PG 금액 | 차액 |\n|---|---|---:|---:|---:|\n")
		for _, d := range r.Discrepancies {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", d.Kind, d.PgTxID, optWon(d.InternalAmount), optWon(d.PgAmount), won(d.DiffAmount))
		}
	}
	return b.String()
}

// won 은 정수 금액을 1,000 단위 콤마로 표시한다.
func won(v int64) string {
	s := fmt.Sprintf("%d", abs(v))
	var out []byte
	for i := range len(s) { // Go 1.22+: 정수 range (0..len-1)
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	if v < 0 {
		return "-" + string(out)
	}
	return string(out)
}

func optWon(v *int64) string {
	if v == nil {
		return "-"
	}
	return won(*v) // *v : 포인터가 가리키는 값을 꺼낸다 (역참조)
}
