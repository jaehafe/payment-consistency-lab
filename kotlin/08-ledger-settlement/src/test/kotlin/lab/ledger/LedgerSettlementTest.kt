package lab.ledger

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.LocalDate
import java.util.concurrent.CompletableFuture

@SpringBootTest
@Import(TestcontainersConfiguration::class)
class LedgerSettlementTest {
    @Autowired lateinit var ledger: LedgerService
    @Autowired lateinit var txs: TransactionService
    @Autowired lateinit var settlements: SettlementService
    @Autowired lateinit var reports: ReportQueries
    @Autowired lateinit var jdbc: JdbcTemplate

    private val day: LocalDate = LocalDate.of(2026, 11, 7)
    private val dayTime: Instant = Instant.parse("2026-11-07T01:00:00Z") // = 11-07 10:00 KST
    private val m1Payable = Accounts.merchantPayable("m1")

    @BeforeEach
    fun clean() {
        jdbc.execute("TRUNCATE ledger_lines, journal_entries, ledger_accounts, transactions, settlements")
    }

    @AfterEach
    fun resetHook() {
        settlements.afterAggregateHook = null
    }

    @Test
    fun payment_posting__debits_equal_credits() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)

        val lines = linesOf("PAYMENT", "pg-1")
        assertEquals(lines.sumOf { it.debit }, lines.sumOf { it.credit }, "차변 합 = 대변 합")
        assertEquals(
            setOf(Line.debit(Accounts.PG_RECEIVABLE, 10_000), Line.credit(m1Payable, 9_700), Line.credit(Accounts.FEE_REVENUE, 300)),
            lines.toSet(),
        )
        assertEquals(10_000, ledger.balanceOf(Accounts.PG_RECEIVABLE))
        assertEquals(-9_700, ledger.balanceOf(m1Payable), "부채는 대변 잔액 → debit-positive 규칙에서 음수")
        assertEquals(-300, ledger.balanceOf(Accounts.FEE_REVENUE))
    }

    @Test
    fun unbalanced_entry__rejected() {
        val entry = JournalEntry(
            "PAYMENT", "pg-bad", "차변 1000 / 대변 900",
            listOf(Line.debit(Accounts.PG_RECEIVABLE, 1_000), Line.credit(m1Payable, 900)),
        )

        assertThrows<UnbalancedEntryException> { ledger.post(entry) }
        assertEquals(0, count("journal_entries"), "아무것도 기록되지 않았다")
        assertEquals(0, count("ledger_lines"))
    }

    @Test
    fun fee_rounding__half_up_in_won() {
        assertEquals(32, Fees.calculate(1_050, 300), "31.5원 → 32원 (반올림)")
        assertEquals(31, Fees.calculate(1_049, 300), "31.47원 → 31원")
        assertEquals(2, Fees.calculate(50, 300), "1.5원 → 2원")
        assertEquals(1, Fees.calculate(49, 300), "1.47원 → 1원")
        assertEquals(300, Fees.calculate(10_000, 300))

        // Double이 실제로 틀리는 예: 5,750원 × 2.2% = 126.5 → 127원이어야 하는데
        assertEquals(126.49999999999999, 5_750 * 0.022, "0.022는 2진수로 정확히 표현되지 않는다")
        assertEquals(126, Math.round(5_750 * 0.022), "Double + Math.round → 126원 (1원 손해)")
        assertEquals(127, Fees.calculate(5_750, 220), "정수식 → 127원")

        // 원장에도 반올림된 정수 수수료가 들어간다
        txs.recordPayment("m1", "pg-1050", 1_050, dayTime)
        assertEquals(-32, ledger.balanceOf(Accounts.FEE_REVENUE))
        assertEquals(-1_018, ledger.balanceOf(m1Payable), "가맹점 몫 = 1050 - 32")
    }

    @Test
    fun same_reference_posted_twice__single_entry() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)
        txs.recordPayment("m1", "pg-1", 10_000, dayTime) // 재시도/중복 메시지

        assertEquals(1, count("journal_entries"))
        assertEquals(1, count("transactions"))
        assertEquals(10_000, ledger.balanceOf(Accounts.PG_RECEIVABLE), "20,000이 아니다")
        assertEquals(10_000, settlements.settle(day).single().grossAmount, "정산 gross도 2배가 아니다")
    }

    @Test
    fun unknown_account__rejected() {
        val entry = JournalEntry(
            "PG_DEPOSIT", "dep-typo", "오타 난 계정",
            listOf(Line.debit("bank:cahs", 1_000), Line.credit(Accounts.PG_RECEIVABLE, 1_000)),
        )

        assertThrows<IllegalArgumentException> { ledger.post(entry) }
        assertEquals(0, count("journal_entries"), "아무것도 기록되지 않았다")
        assertEquals(0, count("ledger_accounts"), "오타 계정이 조용히 만들어지지 않는다")
    }

    @Test
    fun refund__reverses_payment_entries() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)
        txs.recordRefund("m1", "pg-1-refund", 10_000, dayTime.plusSeconds(60))

        val payment = linesOf("PAYMENT", "pg-1")
        val refund = linesOf("REFUND", "pg-1-refund")
        assertEquals(payment.map { Line(it.accountId, debit = it.credit, credit = it.debit) }.toSet(), refund.toSet(), "차변/대변을 뒤집은 역분개")
        assertEquals(0, ledger.balanceOf(Accounts.PG_RECEIVABLE))
        assertEquals(0, ledger.balanceOf(m1Payable))
        assertEquals(0, ledger.balanceOf(Accounts.FEE_REVENUE), "전액 환불이면 수수료도 전액 환급")
        assertEquals(2, count("journal_entries"), "원래 분개는 지우지 않는다 (append-only)")
    }

    @Test
    fun settle__net_equals_gross_minus_refund_minus_fee() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)               // fee 300
        txs.recordPayment("m1", "pg-2", 20_000, dayTime)               // fee 600
        txs.recordRefund("m1", "pg-2-refund-part", 5_000, dayTime)     // fee 환급 150
        txs.recordPayment("m2", "pg-3", 7_000, dayTime)                // fee 210

        val result = settlements.settle(day).associateBy { it.merchantId }

        val m1 = result.getValue("m1")
        assertEquals(30_000, m1.grossAmount)
        assertEquals(5_000, m1.refundAmount)
        assertEquals(750, m1.feeAmount, "순수수료 = 300 + 600 - 150")
        assertEquals(24_250, m1.netAmount)
        assertEquals(m1.grossAmount - m1.refundAmount - m1.feeAmount, m1.netAmount)
        assertEquals(-m1.netAmount, ledger.balanceOf(m1Payable), "정산 금액 = 원장의 가맹점 미지급금")

        val m2 = result.getValue("m2")
        assertEquals(6_790, m2.netAmount)

        val report = reports.dailyReport(day).merchants.associateBy { it.merchantId }
        assertEquals(24_250, report.getValue("m1").net, "리포트도 같은 숫자")
    }

    @Test
    fun settle__uses_kst_day_boundary() {
        txs.recordPayment("m1", "pg-late-night", 1_000, Instant.parse("2026-11-07T14:59:59Z")) // 11-07 23:59:59 KST
        txs.recordPayment("m1", "pg-after-midnight", 2_000, Instant.parse("2026-11-07T15:30:00Z")) // 11-08 00:30 KST

        val nov7 = settlements.settle(LocalDate.of(2026, 11, 7)).single()
        val nov8 = settlements.settle(LocalDate.of(2026, 11, 8)).single()

        assertEquals(1_000, nov7.grossAmount, "UTC로는 같은 11-07이지만")
        assertEquals(2_000, nov8.grossAmount, "UTC 15:30 = KST 다음날 00:30 → 다음날 정산")
    }

    @Test
    fun settle_twice_same_date__no_duplicate_settlement() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)

        val first = settlements.settle(day)
        val second = settlements.settle(day) // 배치 재실행

        assertEquals(1, first.size)
        assertEquals(0, second.size, "이미 정산된 거래는 다시 집계되지 않는다")
        assertEquals(1, count("settlements"))
        val settlementId = jdbc.queryForObject("SELECT settlement_id::text FROM transactions WHERE pg_tx_id = 'pg-1'", String::class.java)
        assertEquals(first.single().id.toString(), settlementId)
    }

    @Test
    fun late_transaction_during_settle__not_marked_settled() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)
        // 집계 직후, 다른 스레드(= 다른 트랜잭션)에서 같은 날 늦은 거래를 커밋한다
        settlements.afterAggregateHook = {
            CompletableFuture.runAsync { txs.recordPayment("m1", "pg-late", 5_000, dayTime.plusSeconds(3_600)) }.join()
        }

        val s = settlements.settle(day).single()

        assertEquals(10_000, s.grossAmount, "집계는 늦은 거래 전")
        val lateSettlementId = jdbc.queryForObject(
            "SELECT settlement_id::text FROM transactions WHERE pg_tx_id = 'pg-late'", String::class.java,
        )
        assertNull(lateSettlementId, "집계에 없는 거래를 정산됨으로 표시하면 영영 정산되지 않는다")
        val marked = jdbc.queryForObject(
            "SELECT coalesce(sum(amount), 0) FROM transactions WHERE settlement_id = ?", Long::class.java, s.id,
        )
        assertEquals(s.grossAmount, marked, "정산됨 표시된 거래 합 = 정산 gross")
    }

    @Test
    fun payout__merchant_payable_zero_after_payout() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)
        txs.recordPayment("m1", "pg-2", 20_000, dayTime)
        txs.recordRefund("m1", "pg-2-refund-part", 5_000, dayTime)
        txs.recordPgDeposit("deposit-2026-11-08", 25_000) // PG가 결제-환불 금액을 입금
        assertEquals(0, ledger.balanceOf(Accounts.PG_RECEIVABLE), "받을 돈을 다 받았다")

        val s = settlements.settle(day).single()
        settlements.payout(s.id)

        assertEquals(0, ledger.balanceOf(m1Payable), "가맹점에 줄 돈을 다 줬다")
        assertEquals("PAID", settlements.find(s.id).status)
        assertEquals(750, ledger.balanceOf(Accounts.BANK_CASH), "통장에 남은 돈 = 우리 수수료 수익")
        assertEquals(-750, ledger.balanceOf(Accounts.FEE_REVENUE))
        assertThrows<IllegalStateException> { settlements.payout(s.id) } // 두 번 지급 불가
    }

    @Test
    fun trial_balance__sum_of_balances_is_zero() {
        txs.recordPayment("m1", "pg-1", 10_000, dayTime)
        txs.recordPayment("m2", "pg-2", 33_333, dayTime)
        txs.recordRefund("m1", "pg-1-refund", 4_444, dayTime)
        txs.recordPgDeposit("deposit-1", 30_000)
        settlements.settle(day).forEach { settlements.payout(it.id) }

        val report = reports.dailyReport(day)

        report.trialBalance.forEach { println("${it.accountId.padEnd(24)} ${it.type.padEnd(9)} ${it.balance}") }
        assertEquals(0, report.trialBalanceSum, "모든 계정 잔액의 합 = 0 (차변 합 = 대변 합)")
        report.trialBalance.forEach {
            assertEquals(it.linesBalance, it.balance, "${it.accountId}: 잔액 캐시 = 원장 라인 합계")
        }
    }

    private fun linesOf(referenceType: String, referenceId: String): List<Line> = jdbc.query(
        """
        SELECT l.account_id, l.debit, l.credit FROM ledger_lines l JOIN journal_entries e ON e.id = l.entry_id
        WHERE e.reference_type = ? AND e.reference_id = ?
        """.trimIndent(),
        { rs, _ -> Line(rs.getString(1), rs.getLong(2), rs.getLong(3)) },
        referenceType, referenceId,
    )

    private fun count(table: String) = jdbc.queryForObject("SELECT count(*) FROM $table", Int::class.java)!!
}
