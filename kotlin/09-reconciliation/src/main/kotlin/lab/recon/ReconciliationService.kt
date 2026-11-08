package lab.recon

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.time.LocalDate
import java.util.UUID

enum class DiscrepancyKind { MISSING_IN_INTERNAL, MISSING_IN_PG, AMOUNT_MISMATCH, DUPLICATE_IN_PG, DUPLICATE_IN_INTERNAL }

data class Discrepancy(
    val kind: DiscrepancyKind,
    val pgTxId: String,
    val internalAmount: Long?,
    val pgAmount: Long?,
    val diffAmount: Long,
)

data class ReconReport(
    val runId: UUID,
    val businessDate: LocalDate,
    val matchedCount: Int,
    val discrepancies: List<Discrepancy>,
) {
    val countsByKind: Map<DiscrepancyKind, Int> =
        DiscrepancyKind.entries.associateWith { k -> discrepancies.count { it.kind == k } }
    val diffByKind: Map<DiscrepancyKind, Long> =
        DiscrepancyKind.entries.associateWith { k -> discrepancies.filter { it.kind == k }.sumOf { it.diffAmount } }

    /** 차액 합계 (pg - internal). 0이 아니면 "장부상 돈이 맞지 않는다" */
    val totalDiffAmount: Long = discrepancies.sumOf { it.diffAmount }

    fun toMarkdown(): String = buildString {
        appendLine("# 대사 리포트 $businessDate (KST)")
        appendLine()
        appendLine("- run_id: `$runId`")
        appendLine("- 일치: $matchedCount 건")
        appendLine("- 불일치: ${discrepancies.size} 건, 차액 합계(PG - 내부): ${"%,d".format(totalDiffAmount)}원")
        appendLine()
        appendLine("| 종류 | 건수 | 차액 합계 |")
        appendLine("|---|---:|---:|")
        DiscrepancyKind.entries.forEach { appendLine("| $it | ${countsByKind[it]} | ${"%,d".format(diffByKind[it])} |") }
        if (discrepancies.isNotEmpty()) {
            appendLine()
            appendLine("| 종류 | pg_tx_id | 내부 금액 | PG 금액 | 차액 |")
            appendLine("|---|---|---:|---:|---:|")
            discrepancies.forEach {
                appendLine("| ${it.kind} | ${it.pgTxId} | ${it.internalAmount ?: "-"} | ${it.pgAmount ?: "-"} | ${"%,d".format(it.diffAmount)} |")
            }
        }
    }
}

/**
 * 대사: 우리 기록(internal_transactions)과 PG 정산 파일(pg_settlement_records)을 맞춰 본다.
 * 지금까지의 모든 장치(멱등키, outbox, 상태 머신)가 "이론상" 정합성을 지키지만, 버그/장애/사람 실수는 언제나 있다.
 * 대사는 외부의 진실(PG 파일)과 비교해 그 틈을 "사후에 발견"하는 마지막 안전망이다.
 */
@Service
class ReconciliationService(
    private val jdbc: NamedParameterJdbcTemplate,
    private val tx: TransactionTemplate,
    private val queries: ReconQueries,
) {
    fun run(businessDate: LocalDate): ReconReport {
        val params = MapSqlParameterSource("business_date", businessDate)
        val discrepancies = listOf("discrepancies", "duplicates_in_pg", "duplicates_in_internal").flatMap { name ->
            jdbc.query(queries[name], params) { rs, _ ->
                Discrepancy(
                    kind = DiscrepancyKind.valueOf(rs.getString("kind")),
                    pgTxId = rs.getString("pg_tx_id"),
                    internalAmount = rs.getObject("internal_amount")?.let { (it as Number).toLong() },
                    pgAmount = rs.getObject("pg_amount")?.let { (it as Number).toLong() },
                    diffAmount = rs.getLong("diff_amount"),
                )
            }
        }
        val matched = jdbc.queryForObject(queries["matched_count"], params, Int::class.java)!!
        val report = ReconReport(UUID.randomUUID(), businessDate, matched, discrepancies)

        // 결과를 run_id 단위로 쌓는다 (재실행해도 이전 결과를 덮지 않음 → "언제 무엇이 발견/해소됐나" 추적 가능)
        tx.executeWithoutResult {
            jdbc.batchUpdate(
                """
                INSERT INTO recon_results (run_id, business_date, kind, pg_tx_id, internal_amount, pg_amount, diff_amount)
                VALUES (:run_id, :business_date, :kind, :pg_tx_id, :internal_amount, :pg_amount, :diff_amount)
                """.trimIndent(),
                discrepancies.map {
                    MapSqlParameterSource()
                        .addValue("run_id", report.runId).addValue("business_date", businessDate)
                        .addValue("kind", it.kind.name).addValue("pg_tx_id", it.pgTxId)
                        .addValue("internal_amount", it.internalAmount).addValue("pg_amount", it.pgAmount)
                        .addValue("diff_amount", it.diffAmount)
                }.toTypedArray(),
            )
        }
        return report
    }
}
