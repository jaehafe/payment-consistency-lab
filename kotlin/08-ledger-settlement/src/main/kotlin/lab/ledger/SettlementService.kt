package lab.ledger

import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.sql.Timestamp
import java.time.LocalDate
import java.util.UUID

data class Settlement(
    val id: UUID,
    val merchantId: String,
    val settlementDate: LocalDate,
    val grossAmount: Long,
    val refundAmount: Long,
    val feeAmount: Long,
    val netAmount: Long,
    val status: String,
)

@Service
class SettlementService(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val ledger: LedgerService,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * KST 거래일(date)의 미정산 거래를 가맹점별로 집계해 settlements를 만든다.
     * 같은 날짜로 다시 실행해도 중복이 생기지 않는다:
     *  - 이미 정산된 거래는 settlement_id가 채워져 있어 집계 대상에서 빠지고
     *  - 최후의 방어선으로 UNIQUE (merchant_id, settlement_date)가 있다.
     * @return 이번 실행에서 새로 만든 정산 목록
     */
    fun settle(date: LocalDate): List<Settlement> = tx.execute {
        val (from, to) = Kst.dayRange(date)
        val fromTs = Timestamp.from(from)
        val toTs = Timestamp.from(to)

        // 1) 대상 거래 행을 잠근다: 정산 배치가 동시에 두 번 돌아도 같은 거래를 두 번 집계하지 않도록.
        //    (두 번째 배치는 여기서 기다렸다가, 첫 배치 커밋 후 settlement_id IS NULL 조건을 다시 평가해 0행이 된다)
        //    경계는 [KST 00:00, 다음날 KST 00:00) 반열린 구간. 23:59:59.999... 같은 끝값 처리 실수를 피한다.
        jdbc.query(
            "SELECT id FROM transactions WHERE occurred_at >= ? AND occurred_at < ? AND settlement_id IS NULL FOR UPDATE",
            { rs, _ -> rs.getString(1) }, fromTs, toTs,
        )

        // 2) 가맹점별 집계. net = gross - refund - (결제 수수료 - 환불 수수료 환급)
        val rows = jdbc.queryForList(
            """
            SELECT merchant_id,
                   coalesce(sum(amount) FILTER (WHERE type = 'PAYMENT'), 0) AS gross,
                   coalesce(sum(amount) FILTER (WHERE type = 'REFUND'), 0)  AS refund,
                   coalesce(sum(fee) FILTER (WHERE type = 'PAYMENT'), 0)
                     - coalesce(sum(fee) FILTER (WHERE type = 'REFUND'), 0) AS fee
            FROM transactions
            WHERE occurred_at >= ? AND occurred_at < ? AND settlement_id IS NULL
            GROUP BY merchant_id
            ORDER BY merchant_id
            """.trimIndent(),
            fromTs, toTs,
        )

        rows.mapNotNull { r ->
            val merchantId = r["merchant_id"] as String
            val gross = (r["gross"] as Number).toLong()
            val refund = (r["refund"] as Number).toLong()
            val fee = (r["fee"] as Number).toLong()
            val s = Settlement(UUID.randomUUID(), merchantId, date, gross, refund, fee, gross - refund - fee, "CALCULATED")

            val inserted = jdbc.update(
                """
                INSERT INTO settlements (id, merchant_id, settlement_date, gross_amount, refund_amount, fee_amount, net_amount, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'CALCULATED')
                ON CONFLICT (merchant_id, settlement_date) DO NOTHING
                """.trimIndent(),
                s.id, merchantId, date, gross, refund, fee, s.netAmount,
            )
            if (inserted == 0) {
                // 이미 정산이 끝난 날짜에 늦게 도착한 거래. 기존 정산을 조용히 고치면 이미 지급한 금액과 어긋난다.
                // 미정산으로 남겨 두고 별도 정정(다음 주기 이월 등)으로 처리해야 한다.
                log.warn("{} {} 은 이미 정산됨 — 늦게 도착한 거래는 미정산으로 남긴다", merchantId, date)
                return@mapNotNull null
            }
            // 3) 거래에 정산 id를 찍는다 (재실행 시 집계 대상에서 빠지는 근거)
            jdbc.update(
                "UPDATE transactions SET settlement_id = ? WHERE merchant_id = ? AND occurred_at >= ? AND occurred_at < ? AND settlement_id IS NULL",
                s.id, merchantId, fromTs, toTs,
            )
            s
        }
    }!!

    /**
     * 가맹점 지급(출금): Dr merchant:{id}:payable / Cr bank:cash, 정산 상태 CALCULATED → PAID.
     * 상태 변경과 분개가 한 트랜잭션 + 분개 UNIQUE(PAYOUT, settlementId) → 두 번 지급되지 않는다.
     */
    fun payout(settlementId: UUID) = tx.executeWithoutResult {
        val updated = jdbc.update("UPDATE settlements SET status = 'PAID' WHERE id = ? AND status = 'CALCULATED'", settlementId)
        if (updated == 0) throw IllegalStateException("지급 가능한 정산이 아니다 (이미 지급됐거나 없음): $settlementId")

        val s = find(settlementId)
        if (s.netAmount > 0) {
            ledger.post(
                JournalEntry(
                    "PAYOUT", settlementId.toString(), "가맹점 지급 ${s.merchantId} ${s.settlementDate}",
                    listOf(Line.debit(Accounts.merchantPayable(s.merchantId), s.netAmount), Line.credit(Accounts.BANK_CASH, s.netAmount)),
                ),
            )
        }
        // net <= 0 (환불이 더 많은 날)이면 지급할 돈이 없다. 실무에선 다음 정산에서 상계하거나 가맹점에 청구한다.
    }

    fun find(id: UUID): Settlement = jdbc.queryForObject(
        "SELECT * FROM settlements WHERE id = ?",
        { rs, _ ->
            Settlement(
                rs.getObject("id", UUID::class.java), rs.getString("merchant_id"), rs.getDate("settlement_date").toLocalDate(),
                rs.getLong("gross_amount"), rs.getLong("refund_amount"), rs.getLong("fee_amount"), rs.getLong("net_amount"),
                rs.getString("status"),
            )
        },
        id,
    )!!
}
