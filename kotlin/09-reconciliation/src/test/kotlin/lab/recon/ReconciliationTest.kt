package lab.recon

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/**
 * 시나리오 (영업일 2026-11-07 KST). fixtures/pg_settlement_2026-11-07.csv 와 아래 seedMixedInternal() 비교:
 *
 * | pg_tx_id            | 내부          | PG 파일       | 기대 결과                    |
 * |---------------------|--------------|--------------|-----------------------------|
 * | tx-ok-1, tx-ok-2    | 있음          | 있음(같은 금액) | 일치                         |
 * | tx-mismatch         | 30,000       | 29,000       | AMOUNT_MISMATCH (-1,000)     |
 * | tx-missing-pg       | 15,000       | 없음          | MISSING_IN_PG (-15,000)      |
 * | tx-missing-internal | 없음          | 12,000       | MISSING_IN_INTERNAL (+12,000)|
 * | tx-dup-pg           | 1건           | 2건          | DUPLICATE_IN_PG (+7,000)     |
 * | tx-dup-internal     | 2건           | 1건          | DUPLICATE_IN_INTERNAL (-5,000)|
 * | tx-next-day         | 15:30Z       | 15:30Z       | KST로 11-08 → 대상 아님        |
 */
@SpringBootTest
@Import(TestcontainersConfiguration::class)
class ReconciliationTest {
    @Autowired lateinit var importer: PgSettlementFileImporter
    @Autowired lateinit var recon: ReconciliationService
    @Autowired lateinit var jdbc: JdbcTemplate

    private val businessDate = LocalDate.of(2026, 11, 7)

    @BeforeEach
    fun clean() {
        jdbc.execute("TRUNCATE recon_results, pg_settlement_records, recon_files, internal_transactions")
    }

    @Test
    fun matched_records__no_discrepancy() {
        internal("tx-ok-1", "order-1", "PAYMENT", 10_000, "2026-11-07T01:00:00Z")
        internal("tx-ok-2", "order-2", "PAYMENT", 20_000, "2026-11-07T02:00:00Z")
        internal("tx-refund-1", "order-1", "REFUND", 3_000, "2026-11-07T08:00:00Z")
        importFixture("pg_settlement_matched.csv")

        val report = recon.run(businessDate)

        assertEquals(3, report.matchedCount)
        assertEquals(emptyList<Discrepancy>(), report.discrepancies)
        assertEquals(0, report.totalDiffAmount)
    }

    @Test
    fun missing_in_internal__detected() {
        val report = runMixed()
        assertEquals(Discrepancy(DiscrepancyKind.MISSING_IN_INTERNAL, "tx-missing-internal", null, 12_000, 12_000), report.only(DiscrepancyKind.MISSING_IN_INTERNAL))
    }

    @Test
    fun missing_in_pg__detected() {
        val report = runMixed()
        assertEquals(Discrepancy(DiscrepancyKind.MISSING_IN_PG, "tx-missing-pg", 15_000, null, -15_000), report.only(DiscrepancyKind.MISSING_IN_PG))
    }

    @Test
    fun amount_mismatch__detected_with_diff() {
        val report = runMixed()
        val d = report.only(DiscrepancyKind.AMOUNT_MISMATCH)
        assertEquals("tx-mismatch", d.pgTxId)
        assertEquals(30_000, d.internalAmount)
        assertEquals(29_000, d.pgAmount)
        assertEquals(-1_000, d.diffAmount, "PG - 내부 = -1,000 (우리가 1,000원 더 받았다고 알고 있다)")
    }

    @Test
    fun duplicate_in_pg_file__detected() {
        val report = runMixed()
        val d = report.only(DiscrepancyKind.DUPLICATE_IN_PG)
        assertEquals("tx-dup-pg", d.pgTxId)
        assertEquals(7_000, d.diffAmount, "초과 정산분")
    }

    @Test
    fun duplicate_internal__detected() {
        val report = runMixed()
        val d = report.only(DiscrepancyKind.DUPLICATE_IN_INTERNAL)
        assertEquals("tx-dup-internal", d.pgTxId)
        assertEquals(10_000, d.internalAmount)
        assertEquals(-5_000, d.diffAmount)
    }

    @Test
    fun same_file_imported_twice__ignored() {
        val bytes = fixture("pg_settlement_2026-11-07.csv")

        val first = importer.import("pg_settlement_2026-11-07.csv", bytes)
        val second = importer.import("pg_settlement_2026-11-07(1).csv", bytes) // 이름만 바뀐 재전송

        assertTrue(first.imported)
        assertEquals(8, first.rowCount)
        assertEquals(false, second.imported, "내용 해시가 같으므로 무시")
        assertEquals(1, count("recon_files"))
        assertEquals(8, count("pg_settlement_records"), "16이 아니다 → 가짜 DUPLICATE_IN_PG 폭발을 막음")
    }

    @Test
    fun report__summarizes_counts_and_diff_amount() {
        val report = runMixed()

        assertEquals(4, report.matchedCount, "tx-ok-1, tx-ok-2, tx-dup-pg, tx-dup-internal (중복 외에는 금액 일치)")
        assertEquals(DiscrepancyKind.entries.associateWith { 1 }, report.countsByKind)
        assertEquals(12_000 - 15_000 - 1_000 + 7_000 - 5_000L, report.totalDiffAmount)

        val stored = jdbc.queryForObject(
            "SELECT count(*) FROM recon_results WHERE run_id = ?", Int::class.java, report.runId,
        )
        assertEquals(5, stored, "결과가 run_id로 저장됐다")

        val markdown = report.toMarkdown()
        println(markdown)
        assertTrue(markdown.contains("일치: 4 건"))
        assertTrue(markdown.contains("| AMOUNT_MISMATCH | 1 | -1,000 |"))
        assertTrue(markdown.contains("차액 합계(PG - 내부): -2,000원"))
    }

    // ---- helpers ----

    private fun runMixed(): ReconReport {
        internal("tx-ok-1", "order-1", "PAYMENT", 10_000, "2026-11-07T01:00:00Z")
        internal("tx-ok-2", "order-2", "PAYMENT", 20_000, "2026-11-07T02:00:00Z")
        internal("tx-mismatch", "order-3", "PAYMENT", 30_000, "2026-11-07T03:00:00Z")
        internal("tx-missing-pg", "order-4", "PAYMENT", 15_000, "2026-11-07T03:30:00Z")
        internal("tx-dup-pg", "order-7", "PAYMENT", 7_000, "2026-11-07T05:00:00Z")
        internal("tx-dup-internal", "order-5", "PAYMENT", 5_000, "2026-11-07T06:00:00Z")
        internal("tx-dup-internal", "order-5", "PAYMENT", 5_000, "2026-11-07T06:00:01Z") // 이중 기록
        internal("tx-next-day", "order-8", "PAYMENT", 9_000, "2026-11-07T15:30:00Z")     // KST 11-08 00:30
        importFixture("pg_settlement_2026-11-07.csv")
        return recon.run(businessDate)
    }

    private fun ReconReport.only(kind: DiscrepancyKind): Discrepancy = discrepancies.single { it.kind == kind }

    private fun internal(pgTxId: String, orderId: String, type: String, amount: Long, at: String) {
        jdbc.update(
            "INSERT INTO internal_transactions (id, pg_tx_id, order_id, type, amount, occurred_at) VALUES (?, ?, ?, ?, ?, ?)",
            UUID.randomUUID(), pgTxId, orderId, type, amount, Timestamp.from(Instant.parse(at)),
        )
    }

    private fun fixture(name: String) = ClassPathResource("fixtures/$name").inputStream.readBytes()
    private fun importFixture(name: String) = importer.import(name, fixture(name))
    private fun count(table: String) = jdbc.queryForObject("SELECT count(*) FROM $table", Int::class.java)!!
}
