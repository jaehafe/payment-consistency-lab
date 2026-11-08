package recon

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var businessDate = time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC) // KST 거래일 2026-11-08 (연/월/일만 사용)

const csvHead = "pg_tx_id,order_id,type,amount,fee,transacted_at\n"

type fixture struct {
	importer *Importer
	svc      *ReconciliationService
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	exec(t, `TRUNCATE internal_transactions, pg_settlement_records, recon_files, recon_results`)
	return fixture{importer: &Importer{DB: testPool}, svc: &ReconciliationService{DB: testPool}}
}

// internal 은 우리 DB 에 거래를 넣는다. at 은 KST "HH:MM".
func internal(t *testing.T, pgTxID, typ string, amount int64, hhmm string) {
	t.Helper()
	at, err := time.Parse(time.RFC3339, "2026-11-08T"+hhmm+":00+09:00")
	if err != nil {
		t.Fatal(err)
	}
	exec(t, `INSERT INTO internal_transactions (id, pg_tx_id, order_id, type, amount, occurred_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		uuid.NewString(), pgTxID, "order-"+pgTxID, typ, amount, at)
}

func (f fixture) importCSV(t *testing.T, name, body string) ImportResult {
	t.Helper()
	res, err := f.importer.Import(context.Background(), name, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (f fixture) run(t *testing.T) ReconReport {
	t.Helper()
	rep, err := f.svc.Run(context.Background(), businessDate)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// only 는 리포트에 해당 종류가 정확히 1건 있고 다른 불일치는 없는지 확인하고 그 1건을 돌려준다.
func only(t *testing.T, rep ReconReport, kind Kind) Discrepancy {
	t.Helper()
	if len(rep.Discrepancies) != 1 || rep.Discrepancies[0].Kind != kind {
		t.Fatalf("%s 1건만 기대: %+v", kind, rep.Discrepancies)
	}
	return rep.Discrepancies[0]
}

func Test_matched_records__no_discrepancy(t *testing.T) {
	f := newFixture(t)
	internal(t, "pg-1", "PAYMENT", 10000, "09:00")
	internal(t, "pg-2", "REFUND", 3000, "10:00")
	f.importCSV(t, "a.csv", csvHead+
		"pg-1,order-pg-1,PAYMENT,10000,300,2026-11-08T09:00:01+09:00\n"+
		"pg-2,order-pg-2,REFUND,3000,90,2026-11-08T10:00:01+09:00\n")

	rep := f.run(t)
	if rep.Matched != 2 || len(rep.Discrepancies) != 0 {
		t.Fatalf("2건 일치, 불일치 0: %+v", rep)
	}
	if n := queryInt(t, `SELECT count(*) FROM recon_results`); n != 0 {
		t.Fatalf("일치 건은 저장하지 않는다: %d", n)
	}
}

func Test_missing_in_internal__detected(t *testing.T) {
	f := newFixture(t)
	// PG 는 승인했는데 우리 DB 엔 기록이 없다 (07 의 UNKNOWN 이 처리 안 된 경우 등)
	f.importCSV(t, "a.csv", csvHead+"pg-1,order-1,PAYMENT,10000,300,2026-11-08T09:00:00+09:00\n")

	d := only(t, f.run(t), MissingInInternal)
	if d.PgTxID != "pg-1" || d.InternalAmount != nil || *d.PgAmount != 10000 || d.DiffAmount != 10000 {
		t.Fatalf("내부 누락 10000: %+v", d)
	}
}

func Test_missing_in_pg__detected(t *testing.T) {
	f := newFixture(t)
	internal(t, "pg-1", "PAYMENT", 10000, "09:00")
	f.importCSV(t, "a.csv", csvHead) // PG 파일엔 없음

	d := only(t, f.run(t), MissingInPG)
	if d.PgTxID != "pg-1" || *d.InternalAmount != 10000 || d.PgAmount != nil || d.DiffAmount != -10000 {
		t.Fatalf("PG 누락 -10000: %+v", d)
	}
}

func Test_amount_mismatch__detected_with_diff(t *testing.T) {
	f := newFixture(t)
	internal(t, "pg-1", "PAYMENT", 10000, "09:00")
	f.importCSV(t, "a.csv", csvHead+"pg-1,order-1,PAYMENT,9500,285,2026-11-08T09:00:00+09:00\n")

	d := only(t, f.run(t), AmountMismatch)
	if *d.InternalAmount != 10000 || *d.PgAmount != 9500 || d.DiffAmount != -500 {
		t.Fatalf("금액 차이 -500: %+v", d)
	}
}

func Test_duplicate_in_pg_file__detected(t *testing.T) {
	f := newFixture(t)
	internal(t, "pg-1", "PAYMENT", 10000, "09:00")
	f.importCSV(t, "a.csv", csvHead+
		"pg-1,order-1,PAYMENT,10000,300,2026-11-08T09:00:00+09:00\n"+
		"pg-1,order-1,PAYMENT,10000,300,2026-11-08T09:00:00+09:00\n")

	rep := f.run(t)
	d := only(t, rep, DuplicateInPG)
	if d.DiffAmount != 10000 {
		t.Fatalf("초과분 10000: %+v", d)
	}
	if rep.Matched != 1 {
		t.Fatalf("중복과 별개로 금액 비교는 일치(1건만 비교): %d", rep.Matched)
	}
}

func Test_duplicate_internal__detected(t *testing.T) {
	f := newFixture(t)
	// 같은 결제 이벤트를 두 번 처리해서 내부에 두 줄 (05 의 멱등 컨슈머가 없을 때)
	internal(t, "pg-1", "PAYMENT", 10000, "09:00")
	internal(t, "pg-1", "PAYMENT", 10000, "09:00")
	f.importCSV(t, "a.csv", csvHead+"pg-1,order-1,PAYMENT,10000,300,2026-11-08T09:00:00+09:00\n")

	d := only(t, f.run(t), DuplicateInInternal)
	if d.DiffAmount != -10000 {
		t.Fatalf("내부 초과분 -10000: %+v", d)
	}
}

func Test_same_file_imported_twice__ignored(t *testing.T) {
	f := newFixture(t)
	content, err := os.ReadFile("testdata/pg_settlement_2026-11-08.csv")
	if err != nil {
		t.Fatal(err)
	}
	first := f.importCSV(t, "pg_settlement_2026-11-08.csv", string(content))
	// 이름을 바꿔서 다시 보내도 내용(해시)이 같으면 무시
	second := f.importCSV(t, "pg_settlement_2026-11-08 (1).csv", string(content))

	if first.Skipped || first.Rows != 7 || !second.Skipped {
		t.Fatalf("첫 import 7행, 두 번째는 skip: %+v / %+v", first, second)
	}
	if n := queryInt(t, `SELECT count(*) FROM pg_settlement_records`); n != 7 {
		t.Fatalf("레코드 7행: %d", n)
	}
	if n := queryInt(t, `SELECT count(*) FROM recon_files`); n != 1 {
		t.Fatalf("파일 이력 1행: %d", n)
	}
}

func Test_report__summarizes_counts_and_diff_amount(t *testing.T) {
	f := newFixture(t)
	content, err := os.ReadFile("testdata/pg_settlement_2026-11-08.csv")
	if err != nil {
		t.Fatal(err)
	}
	f.importCSV(t, "pg_settlement_2026-11-08.csv", string(content))

	internal(t, "pg-001", "PAYMENT", 10000, "09:00") // 일치
	internal(t, "pg-002", "PAYMENT", 25000, "10:15") // 일치 …
	internal(t, "pg-002", "PAYMENT", 25000, "10:15") // … 인데 내부 중복 (-25000)
	internal(t, "pg-003", "PAYMENT", 7000, "11:30")  // 금액 차이 (PG 8000, +1000)
	internal(t, "pg-004", "REFUND", 5000, "13:45")   // 일치
	internal(t, "pg-005", "PAYMENT", 12000, "15:00") // 일치, 단 PG 파일 중복 (+12000)
	internal(t, "pg-006", "PAYMENT", 4000, "16:00")  // PG 누락 (-4000)
	// pg-007 은 내부 누락 (+3000)
	internal(t, "pg-999", "PAYMENT", 1000, "08:00")
	exec(t, `UPDATE internal_transactions SET occurred_at = '2026-11-07T23:00:00+09:00' WHERE pg_tx_id = 'pg-999'`) // 전날 거래 → 대상 아님

	rep := f.run(t)
	md := rep.Markdown()
	t.Log("\n" + md)

	if rep.Matched != 4 {
		t.Fatalf("일치 4건(pg-001, 002, 004, 005): %d", rep.Matched)
	}
	want := map[Kind]int{MissingInInternal: 1, MissingInPG: 1, AmountMismatch: 1, DuplicateInPG: 1, DuplicateInInternal: 1}
	for k, n := range want {
		if rep.Counts[k] != n {
			t.Fatalf("%s %d건 기대: %d", k, n, rep.Counts[k])
		}
	}
	if rep.TotalDiff != 3000-4000+1000+12000-25000 || rep.TotalAbsDiff != 45000 {
		t.Fatalf("차액 합계 -13000 / 절대값 45000: %d / %d", rep.TotalDiff, rep.TotalAbsDiff)
	}
	if n := queryInt(t, `SELECT count(*) FROM recon_results WHERE run_id = $1`, rep.RunID); n != 5 {
		t.Fatalf("recon_results 5행: %d", n)
	}
	for _, s := range []string{"# 대사 리포트 2026-11-08", "일치: 4건", "-13,000", "| AMOUNT_MISMATCH | 1 |", "pg-007"} {
		if !strings.Contains(md, s) {
			t.Fatalf("리포트에 %q 가 있어야 한다", s)
		}
	}
}
