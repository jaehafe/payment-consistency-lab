package lab.ledger

import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

/** 원천 거래(결제/환불/입금)를 기록하고 해당 분개를 전기한다. */
@Service
class TransactionService(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val ledger: LedgerService,
    @Value("\${lab.fee-rate-bps}") private val feeRateBps: Long,
) {
    /**
     * 결제 승인:
     *   Dr pg:receivable          amount        (PG에게 받을 돈이 생김)
     *   Cr merchant:{id}:payable  amount - fee  (가맹점에게 줄 돈이 생김)
     *   Cr platform:fee_revenue   fee           (우리 수익)
     */
    fun recordPayment(merchantId: String, pgTxId: String, amount: Long, occurredAt: Instant): Long {
        val fee = Fees.calculate(amount, feeRateBps)
        tx.executeWithoutResult {
            val posted = ledger.post(
                JournalEntry(
                    "PAYMENT", pgTxId, "결제 승인 $merchantId",
                    listOf(
                        Line.debit(Accounts.PG_RECEIVABLE, amount),
                        Line.credit(Accounts.merchantPayable(merchantId), amount - fee),
                        Line.credit(Accounts.FEE_REVENUE, fee),
                    ),
                ),
            )
            // 이미 전기된 거래면 원천 거래도 다시 넣지 않는다 (분개의 UNIQUE가 멱등성의 기준점)
            if (posted.created) insertTransaction(merchantId, "PAYMENT", pgTxId, amount, fee, occurredAt)
        }
        return fee
    }

    /**
     * 환불(전액/부분): 결제 분개의 역분개. 환불 금액에 대한 수수료도 가맹점에 돌려준다(수수료 환급).
     *   Dr merchant:{id}:payable  amount - fee
     *   Dr platform:fee_revenue   fee
     *   Cr pg:receivable          amount
     * 원래 분개를 지우거나 고치지 않는다 — 원장은 append-only. 정정은 항상 새 분개로.
     */
    fun recordRefund(merchantId: String, refundPgTxId: String, amount: Long, occurredAt: Instant): Long {
        val fee = Fees.calculate(amount, feeRateBps)
        tx.executeWithoutResult {
            val posted = ledger.post(
                JournalEntry(
                    "REFUND", refundPgTxId, "환불 $merchantId",
                    listOf(
                        Line.debit(Accounts.merchantPayable(merchantId), amount - fee),
                        Line.debit(Accounts.FEE_REVENUE, fee),
                        Line.credit(Accounts.PG_RECEIVABLE, amount),
                    ),
                ),
            )
            if (posted.created) insertTransaction(merchantId, "REFUND", refundPgTxId, amount, fee, occurredAt)
        }
        return fee
    }

    /** PG 정산금 입금: Dr bank:cash / Cr pg:receivable (받을 돈이 실제 현금으로 바뀜) */
    fun recordPgDeposit(depositId: String, amount: Long) {
        ledger.post(
            JournalEntry(
                "PG_DEPOSIT", depositId, "PG 정산금 입금",
                listOf(Line.debit(Accounts.BANK_CASH, amount), Line.credit(Accounts.PG_RECEIVABLE, amount)),
            ),
        )
    }

    private fun insertTransaction(merchantId: String, type: String, pgTxId: String, amount: Long, fee: Long, occurredAt: Instant) {
        jdbc.update(
            "INSERT INTO transactions (id, merchant_id, type, pg_tx_id, amount, fee, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            UUID.randomUUID(), merchantId, type, pgTxId, amount, fee, Timestamp.from(occurredAt),
        )
    }
}
