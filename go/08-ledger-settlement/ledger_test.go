package ledger

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

type fixture struct {
	ledger  *LedgerService
	txs     *TransactionService
	settle  *SettlementService
	reports *ReportQueries
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	exec(t, `TRUNCATE ledger_lines, journal_entries, transactions, settlements`)
	exec(t, `DELETE FROM ledger_accounts WHERE id LIKE 'merchant:%'`)
	exec(t, `UPDATE ledger_accounts SET balance = 0`)
	return fixture{
		ledger:  &LedgerService{DB: testPool},
		txs:     &TransactionService{DB: testPool},
		settle:  &SettlementService{DB: testPool},
		reports: &ReportQueries{DB: testPool},
	}
}

func (f fixture) balance(t *testing.T, account string) int64 {
	t.Helper()
	b, err := f.ledger.Balance(context.Background(), account)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// kst 는 KST 기준 시각을 만든다.
func kst(y int, m time.Month, d, hh, mm int) time.Time {
	return time.Date(y, m, d, hh, mm, 0, 0, KST)
}

var nov8 = time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC) // 정산 "날짜" (연/월/일만 의미 있음)

func Test_payment_posting__debits_equal_credits(t *testing.T) {
	f := newFixture(t)
	if _, err := f.txs.RecordPayment(context.Background(), "m1", "pg-1", 10000, kst(2026, 11, 8, 10, 0)); err != nil {
		t.Fatal(err)
	}
	// 분개 한 건 안에서 차변 합 = 대변 합
	debit := queryInt(t, `SELECT sum(debit) FROM ledger_lines`)
	credit := queryInt(t, `SELECT sum(credit) FROM ledger_lines`)
	if debit != 10000 || credit != 10000 {
		t.Fatalf("차변=대변=10000 기대: %d / %d", debit, credit)
	}
	// 계정별: PG 미수금 +10000, 가맹점 부채 -9700, 수수료 수익 -300 (debit-positive 부호 규칙)
	if f.balance(t, AccountPgReceivable) != 10000 || f.balance(t, MerchantPayable("m1")) != -9700 || f.balance(t, AccountFeeRevenue) != -300 {
		t.Fatalf("계정 잔액이 다르다: recv=%d payable=%d fee=%d",
			f.balance(t, AccountPgReceivable), f.balance(t, MerchantPayable("m1")), f.balance(t, AccountFeeRevenue))
	}
}

func Test_unbalanced_entry__rejected(t *testing.T) {
	f := newFixture(t)
	_, err := f.ledger.Post(context.Background(), JournalEntry{
		ReferenceType: RefPayment, ReferenceID: "bad-1",
		Lines: []Line{Dr(AccountPgReceivable, 10000), Cr(MerchantPayable("m1"), 9000)}, // 1,000원이 증발
	})
	if !errors.Is(err, ErrUnbalancedEntry) {
		t.Fatalf("ErrUnbalancedEntry 기대: %v", err)
	}
	if n := queryInt(t, `SELECT count(*) FROM journal_entries`); n != 0 {
		t.Fatalf("아무것도 기록되지 않아야 한다: %d", n)
	}
}

func Test_fee_rounding__half_up_in_won(t *testing.T) {
	// 테이블 기반 테스트: Go에서 흔한 패턴. 입력/기대값을 표로 두고 반복한다 (JUnit의 @ParameterizedTest).
	cases := []struct {
		amount, want int64
	}{
		{10000, 300}, // 정확히 3%
		{1050, 32},   // 31.5  → 32 (0.5 는 올림)
		{1049, 31},   // 31.47 → 31
		{1017, 31},   // 30.51 → 31
		{1016, 30},   // 30.48 → 30
		{50, 2},      // 1.5   → 2
		{16, 0},      // 0.48  → 0
	}
	for _, c := range cases {
		if got := CalculateFee(c.amount, DefaultFeeRateBps); got != c.want {
			t.Errorf("CalculateFee(%d) = %d, want %d", c.amount, got, c.want) // Errorf: 실패 기록 후 계속 진행
		}
	}

	// float 가 실제로 틀리는 예: 5,750원 × 2.2% = 126.5 → 127 이어야 한다.
	// 변수에 담아야 런타임 float64 연산이 된다.
	// (5750 * 0.022 처럼 상수끼리 쓰면 Go 는 컴파일 시점에 정확히 계산한다)
	amount, rate := 5750.0, 0.022
	if f := math.Round(amount * rate); f != 126 {
		t.Fatalf("float64: 126.49999999999999 → 126 기대: %v", f)
	}
	if got := CalculateFee(5750, 220); got != 127 {
		t.Fatalf("정수식은 127: %d", got)
	}
}

func Test_same_reference_posted_twice__single_entry(t *testing.T) {
	f := newFixture(t)
	tx := Transaction{MerchantID: "m1", PgTxID: "pg-1", Amount: 10000, Fee: 300}
	first, err := f.ledger.Post(context.Background(), PaymentEntry(tx))
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.ledger.Post(context.Background(), PaymentEntry(tx)) // 같은 이벤트가 중복 도착
	if err != nil {
		t.Fatal(err)
	}
	if !first || second {
		t.Fatalf("첫 번째만 전기돼야 한다: first=%v second=%v", first, second)
	}
	if n := queryInt(t, `SELECT count(*) FROM journal_entries`); n != 1 {
		t.Fatalf("분개 1건 기대: %d", n)
	}
	if f.balance(t, AccountPgReceivable) != 10000 {
		t.Fatal("잔액도 한 번만 반영돼야 한다")
	}
}

func Test_refund__reverses_payment_entries(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, kst(2026, 11, 8, 10, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.txs.RecordRefund(ctx, "m1", "pg-1-refund", 10000, kst(2026, 11, 8, 11, 0)); err != nil {
		t.Fatal(err)
	}
	// 전액 환불 → 모든 계정이 원래대로 0
	for _, acc := range []string{AccountPgReceivable, MerchantPayable("m1"), AccountFeeRevenue} {
		if b := f.balance(t, acc); b != 0 {
			t.Fatalf("%s 잔액 0 기대: %d", acc, b)
		}
	}
	// 환불 분개의 각 줄은 결제 분개의 같은 계정 줄과 차변/대변이 뒤바뀌어 있다
	rows := queryInt(t, `
		SELECT count(*) FROM ledger_lines p
		  JOIN journal_entries pe ON pe.id = p.entry_id AND pe.reference_type = 'PAYMENT'
		  JOIN ledger_lines r ON r.account_id = p.account_id AND r.debit = p.credit AND r.credit = p.debit
		  JOIN journal_entries re ON re.id = r.entry_id AND re.reference_type = 'REFUND'`)
	if rows != 3 {
		t.Fatalf("3줄 모두 역분개 기대: %d", rows)
	}
}

func Test_settle__net_equals_gross_minus_refund_minus_fee(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, kst(2026, 11, 8, 9, 0)))  // fee 300
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-2", 5050, kst(2026, 11, 8, 12, 0)))  // fee 152 (151.5 → 152)
	must(t)(f.txs.RecordRefund(ctx, "m1", "pg-1-r", 2000, kst(2026, 11, 8, 15, 0))) // fee 60 환급
	must(t)(f.txs.RecordPayment(ctx, "m2", "pg-3", 3000, kst(2026, 11, 8, 18, 0)))  // 다른 가맹점

	sts, err := f.settle.Settle(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	if len(sts) != 2 {
		t.Fatalf("가맹점 2곳 정산 기대: %d", len(sts))
	}
	m1 := sts[0]
	want := Settlement{GrossAmount: 15050, RefundAmount: 2000, FeeAmount: 300 + 152 - 60}
	if m1.MerchantID != "m1" || m1.GrossAmount != want.GrossAmount || m1.RefundAmount != want.RefundAmount || m1.FeeAmount != want.FeeAmount {
		t.Fatalf("m1 집계가 다르다: %+v", m1)
	}
	if m1.NetAmount != m1.GrossAmount-m1.RefundAmount-m1.FeeAmount || m1.NetAmount != 12658 {
		t.Fatalf("net = gross - refund - fee = 12658 기대: %d", m1.NetAmount)
	}
	// 정산 금액은 원장의 가맹점 부채 잔액과 정확히 같아야 한다 (원장과 정산의 대사)
	if -f.balance(t, MerchantPayable("m1")) != m1.NetAmount {
		t.Fatalf("원장 부채 %d 와 정산 net %d 가 같아야 한다", -f.balance(t, MerchantPayable("m1")), m1.NetAmount)
	}
}

func Test_settle__uses_kst_day_boundary(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	// UTC 11/7 15:30 = KST 11/8 00:30 → 11/8 거래
	utc := time.Date(2026, 11, 7, 15, 30, 0, 0, time.UTC)
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, utc))
	// UTC 11/7 14:59 = KST 11/7 23:59 → 11/7 거래
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-2", 7000, time.Date(2026, 11, 7, 14, 59, 0, 0, time.UTC)))

	nov7, err := f.settle.Settle(ctx, time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(nov7) != 1 || nov7[0].GrossAmount != 7000 {
		t.Fatalf("11/7 정산에는 KST 23:59 거래(7000)만: %+v", nov7)
	}
	nov8s, err := f.settle.Settle(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	if len(nov8s) != 1 || nov8s[0].GrossAmount != 10000 {
		t.Fatalf("UTC 15:30 거래는 KST 다음날(11/8) 정산: %+v", nov8s)
	}
}

func Test_settle_twice_same_date__no_duplicate_settlement(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, kst(2026, 11, 8, 10, 0)))

	first, err := f.settle.Settle(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.settle.Settle(ctx, nov8) // 배치 재실행 (장애 후 재시도 등)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 0 {
		t.Fatalf("두 번째 실행은 아무것도 만들지 않아야 한다: %d / %d", len(first), len(second))
	}
	if n := queryInt(t, `SELECT count(*) FROM settlements`); n != 1 {
		t.Fatalf("settlements 1행: %d", n)
	}
}

func Test_payout__merchant_payable_zero_after_payout(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, kst(2026, 11, 8, 10, 0)))
	sts, err := f.settle.Settle(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	// PG 사가 10,000원 입금 → 가맹점에 9,700원 지급
	if err := f.settle.RecordPgDeposit(ctx, "dep-2026-11-09", 10000); err != nil {
		t.Fatal(err)
	}
	if err := f.settle.Payout(ctx, sts[0].ID); err != nil {
		t.Fatal(err)
	}

	if b := f.balance(t, MerchantPayable("m1")); b != 0 {
		t.Fatalf("지급 후 가맹점 부채 0: %d", b)
	}
	if f.balance(t, AccountPgReceivable) != 0 {
		t.Fatal("PG 미수금도 입금으로 0")
	}
	// 은행에 남은 300원 = 우리 수수료 수익
	if cash, fee := f.balance(t, AccountBankCash), f.balance(t, AccountFeeRevenue); cash != 300 || fee != -300 {
		t.Fatalf("현금 300 = 수수료 수익 300: cash=%d fee=%d", cash, fee)
	}
	if st := queryInt(t, `SELECT count(*) FROM settlements WHERE status = 'PAID'`); st != 1 {
		t.Fatal("정산 상태 PAID")
	}
	if err := f.settle.Payout(ctx, sts[0].ID); !errors.Is(err, ErrAlreadyPaid) {
		t.Fatalf("이중 지급 방지: %v", err)
	}
}

func Test_trial_balance__sum_of_balances_is_zero(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, kst(2026, 11, 8, 10, 0)))
	must(t)(f.txs.RecordPayment(ctx, "m2", "pg-2", 3333, kst(2026, 11, 8, 11, 0)))
	must(t)(f.txs.RecordRefund(ctx, "m1", "pg-1-r", 1234, kst(2026, 11, 8, 12, 0)))
	sts, err := f.settle.Settle(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.settle.RecordPgDeposit(ctx, "dep-1", 10000+3333-1234); err != nil {
		t.Fatal(err)
	}
	for _, st := range sts {
		if err := f.settle.Payout(ctx, st.ID); err != nil {
			t.Fatal(err)
		}
	}

	rep, err := f.reports.DailyReport(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range rep.TrialBalance {
		t.Logf("%-24s %-9s Dr %7d  Cr %7d  잔액 %7d", l.AccountID, l.Type, l.DebitTotal, l.CreditTotal, l.Balance)
	}
	if rep.SumOfBalances != 0 || rep.TotalDebit != rep.TotalCredit {
		t.Fatalf("시산표: 잔액 합 0, 차변 합 = 대변 합 이어야 한다: %+v", rep)
	}
	// 잔액 캐시(balance) = 라인으로 다시 계산한 값(차변 합 - 대변 합)
	for _, l := range rep.TrialBalance {
		if l.Balance != l.DebitTotal-l.CreditTotal {
			t.Fatalf("%s: 캐시 잔액 %d != 라인 합 %d",
				l.AccountID, l.Balance, l.DebitTotal-l.CreditTotal)
		}
	}
	if len(rep.Merchants) != 2 || rep.Merchants[0].Status != "PAID" {
		t.Fatalf("가맹점별 리포트 2줄, PAID: %+v", rep.Merchants)
	}
}

// D1: 같은 결제가 두 번 기록돼도(05 의 중복 메시지) 원천 거래는 1행.
// 원천 거래가 2행이 되면 원장은 맞는데 정산 gross 가 2배가 된다.
func Test_same_payment_recorded_twice__single_transaction_row(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	at := kst(2026, 11, 8, 10, 0)
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, at))
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000, at))

	if n := queryInt(t, `SELECT count(*) FROM transactions`); n != 1 {
		t.Fatalf("transactions 1행 기대: %d", n)
	}
	sts, err := f.settle.Settle(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	if len(sts) != 1 || sts[0].GrossAmount != 10000 {
		t.Fatalf("정산 gross 10000 (2배 아님): %+v", sts)
	}
}

// D2: 정산이 집계한 뒤 settlement_id 를 찍기 전에 같은 날 거래가
// 커밋되면, 그 거래는 집계에 없으므로 "정산됨" 표시도 없어야 한다.
func Test_late_transaction_during_settle__not_marked_settled(
	t *testing.T,
) {
	f := newFixture(t)
	ctx := context.Background()
	must(t)(f.txs.RecordPayment(ctx, "m1", "pg-1", 10000,
		kst(2026, 11, 8, 10, 0)))

	// 집계 직후 다른 커넥션(풀)에서 늦은 거래를 커밋한다.
	f.settle.afterAggregate = func() {
		must(t)(f.txs.RecordPayment(ctx, "m1", "pg-late", 5000,
			kst(2026, 11, 8, 23, 0)))
	}
	sts, err := f.settle.Settle(ctx, nov8)
	if err != nil {
		t.Fatal(err)
	}
	if len(sts) != 1 || sts[0].GrossAmount != 10000 {
		t.Fatalf("집계는 늦은 거래 전 10000: %+v", sts)
	}
	late := queryInt(t, `SELECT count(*) FROM transactions
		WHERE pg_tx_id = 'pg-late' AND settlement_id IS NOT NULL`)
	if late != 0 {
		t.Fatal("집계 안 된 늦은 거래가 정산됨으로 표시됐다")
	}
	// 정산됨 표시된 거래의 합 = 정산 gross (정산과 거래의 대사)
	marked := queryInt(t, `SELECT coalesce(sum(amount), 0)
		FROM transactions WHERE settlement_id = $1`, sts[0].ID)
	if marked != sts[0].GrossAmount {
		t.Fatalf("표시된 거래 합 %d != gross %d",
			marked, sts[0].GrossAmount)
	}
}

// D4: 모르는 계정 ID 는 조용히 ASSET 으로 만들지 않고 거부한다.
// (오타 난 계정에 돈이 쌓이면 시산표 합은 0 이라 알아채기 어렵다)
func Test_unknown_account__rejected(t *testing.T) {
	f := newFixture(t)
	_, err := f.ledger.Post(context.Background(), JournalEntry{
		ReferenceType: RefPgDeposit, ReferenceID: "dep-typo",
		Lines: []Line{
			Dr("bank:cahs", 1000), // 오타
			Cr(AccountPgReceivable, 1000),
		},
	})
	if !errors.Is(err, ErrUnknownAccount) {
		t.Fatalf("ErrUnknownAccount 기대: %v", err)
	}
	if n := queryInt(t, `SELECT count(*) FROM journal_entries`); n != 0 {
		t.Fatalf("아무것도 기록되지 않아야 한다: %d", n)
	}
	n := queryInt(t,
		`SELECT count(*) FROM ledger_accounts WHERE id = 'bank:cahs'`)
	if n != 0 {
		t.Fatal("오타 계정이 만들어지면 안 된다")
	}
}

// must 는 "함수를 돌려주는 함수"다. must(t)(f.txs.RecordPayment(...)) 처럼 쓴다.
// Go는 다중 반환값 (Transaction, error) 을 인자 개수가 정확히 맞는 함수에 그대로 넘길 수 있다.
// 하지만 t 같은 다른 인자와 섞을 수는 없어서, t 를 먼저 받아 클로저(람다)를 돌려주는 방식으로 우회한다.
func must(t *testing.T) func(Transaction, error) {
	return func(_ Transaction, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
}
