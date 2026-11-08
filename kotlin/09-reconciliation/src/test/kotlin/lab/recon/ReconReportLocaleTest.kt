package lab.recon

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.Locale
import java.util.UUID

/** DB 없이 리포트 포맷만 검증하는 단위 테스트 */
class ReconReportLocaleTest {
    @Test
    fun markdown_report__independent_of_jvm_locale() {
        val report = ReconReport(
            UUID.randomUUID(), LocalDate.of(2026, 11, 7), 0,
            listOf(Discrepancy(DiscrepancyKind.MISSING_IN_PG, "tx-1", 1_234_567, null, -1_234_567)),
        )
        val original = Locale.getDefault()
        try {
            // 독일 로케일 서버라면 "%,d"가 1.234.567, 프랑스면 1 234 567 (좁은 공백)이 된다
            Locale.setDefault(Locale.GERMANY)
            val markdown = report.toMarkdown()
            assertTrue(markdown.contains("차액 합계(PG - 내부): -1,234,567원"), markdown)
            assertTrue(markdown.contains("| MISSING_IN_PG | 1 | -1,234,567 |"), markdown)
        } finally {
            Locale.setDefault(original)
        }
    }
}
