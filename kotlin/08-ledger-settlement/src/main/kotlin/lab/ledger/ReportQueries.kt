package lab.ledger

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.sql.Timestamp
import java.time.LocalDate

data class MerchantDaily(val merchantId: String, val gross: Long, val refund: Long, val fee: Long, val net: Long)

data class TrialBalanceRow(val accountId: String, val type: String, val balance: Long, val linesBalance: Long)

data class DailyReport(val date: LocalDate, val merchants: List<MerchantDaily>, val trialBalance: List<TrialBalanceRow>) {
    /** 모든 계정 잔액(debit-positive)의 합. 복식부기가 지켜졌다면 항상 0. */
    val trialBalanceSum: Long get() = trialBalance.sumOf { it.balance }
}

@Component
class ReportQueries(private val jdbc: JdbcTemplate) {

    fun dailyReport(date: LocalDate): DailyReport {
        val (from, to) = Kst.dayRange(date)
        val merchants = jdbc.query(
            """
            SELECT merchant_id,
                   coalesce(sum(amount) FILTER (WHERE type = 'PAYMENT'), 0) AS gross,
                   coalesce(sum(amount) FILTER (WHERE type = 'REFUND'), 0)  AS refund,
                   coalesce(sum(fee) FILTER (WHERE type = 'PAYMENT'), 0)
                     - coalesce(sum(fee) FILTER (WHERE type = 'REFUND'), 0) AS fee
            FROM transactions
            WHERE occurred_at >= ? AND occurred_at < ?
            GROUP BY merchant_id ORDER BY merchant_id
            """.trimIndent(),
            { rs, _ ->
                val gross = rs.getLong("gross"); val refund = rs.getLong("refund"); val fee = rs.getLong("fee")
                MerchantDaily(rs.getString("merchant_id"), gross, refund, fee, gross - refund - fee)
            },
            Timestamp.from(from), Timestamp.from(to),
        )
        return DailyReport(date, merchants, trialBalance())
    }

    /**
     * 시산표: 계정별 잔액. 캐시된 balance와 라인으로부터 다시 계산한 값(linesBalance)을 함께 보여 준다.
     * 둘이 다르면 잔액 캐시 갱신 버그다.
     */
    fun trialBalance(): List<TrialBalanceRow> = jdbc.query(
        """
        SELECT a.id, a.type, a.balance,
               coalesce(sum(l.debit), 0) - coalesce(sum(l.credit), 0) AS lines_balance
        FROM ledger_accounts a LEFT JOIN ledger_lines l ON l.account_id = a.id
        GROUP BY a.id, a.type, a.balance
        ORDER BY a.id
        """.trimIndent(),
    ) { rs, _ -> TrialBalanceRow(rs.getString("id"), rs.getString("type"), rs.getLong("balance"), rs.getLong("lines_balance")) }
}
