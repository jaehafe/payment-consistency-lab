package lab.recon

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant

data class ImportResult(val imported: Boolean, val fileId: Long?, val rowCount: Int)

/**
 * PG 정산 파일(CSV) import.
 * 헤더: pg_tx_id,order_id,type,amount,fee,transacted_at
 *
 * 같은 파일을 두 번 import 하면 PG 레코드가 전부 중복돼 "DUPLICATE_IN_PG"가 폭발한다.
 * 파일명은 바뀔 수 있으므로(재전송, 이름 규칙 변경) 내용의 SHA-256으로 판단한다.
 *
 * 단, 원본 바이트가 아니라 "파서가 읽는 내용"을 해시한다. 같은 파일이 CRLF로 다시 저장되거나
 * 끝 개행이 하나 더 붙어 오면 바이트 해시는 달라져서 새 파일로 들어가 버린다.
 * → BOM 제거 + 줄 단위 trim + 빈 줄 제거 후 LF로 이은 문자열의 해시 (= 아래 파싱 입력과 같은 것).
 */
@Component
class PgSettlementFileImporter(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
) {
    private val expectedHeader = "pg_tx_id,order_id,type,amount,fee,transacted_at"

    fun import(fileName: String, content: ByteArray): ImportResult {
        // lines()는 \n, \r\n, \r 모두를 줄 끝으로 본다
        val lines = String(content, Charsets.UTF_8).removePrefix("\uFEFF")
            .lines().map { it.trim() }.filter { it.isNotEmpty() }
        val hash = sha256(lines.joinToString("\n"))
        require(lines.firstOrNull() == expectedHeader) { "CSV 헤더가 다르다: ${lines.firstOrNull()}" }
        val rows = lines.drop(1).mapIndexed { i, line ->
            parseCsvLine(line).also { require(it.size == 6) { "${i + 2}행: 필드 6개가 아니다: $line" } }
        }

        return tx.execute {
            // 이력 insert와 레코드 insert가 한 트랜잭션: 중간에 실패하면 "import 했다"는 기록도 남지 않아 재시도 가능
            val fileId = jdbc.query(
                """
                INSERT INTO recon_files (file_name, file_hash, row_count) VALUES (?, ?, ?)
                ON CONFLICT (file_hash) DO NOTHING
                RETURNING id
                """.trimIndent(),
                { rs, _ -> rs.getLong(1) },
                fileName, hash, rows.size,
            ).firstOrNull() ?: return@execute ImportResult(imported = false, fileId = null, rowCount = 0) // 이미 import 한 파일

            jdbc.batchUpdate(
                """
                INSERT INTO pg_settlement_records (file_id, pg_tx_id, order_id, type, amount, fee, transacted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                rows.map { c ->
                    arrayOf<Any>(fileId, c[0], c[1], c[2], c[3].toLong(), c[4].toLong(), Timestamp.from(Instant.parse(c[5])))
                },
            )
            ImportResult(imported = true, fileId = fileId, rowCount = rows.size)
        }!!
    }

    private fun sha256(text: String): String =
        MessageDigest.getInstance("SHA-256").digest(text.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }

    companion object {
        /**
         * CSV 한 줄 파싱 (RFC 4180의 따옴표 규칙만): `"a,b"` → a,b / `"say ""hi"""` → say "hi".
         * 단순 split(",")은 주문번호나 상품명에 콤마가 들어오는 순간 열이 밀린다.
         * 한계: 따옴표 안의 줄바꿈(여러 줄 필드)은 지원하지 않는다 — 줄 단위로 먼저 자르기 때문.
         */
        fun parseCsvLine(line: String): List<String> {
            val fields = mutableListOf<String>()
            val cur = StringBuilder()
            var quoted = false
            var i = 0
            while (i < line.length) {
                val c = line[i]
                when {
                    quoted && c == '"' && line.getOrNull(i + 1) == '"' -> { cur.append('"'); i++ } // "" → "
                    c == '"' -> quoted = !quoted
                    c == ',' && !quoted -> { fields += cur.toString(); cur.clear() }
                    else -> cur.append(c)
                }
                i++
            }
            require(!quoted) { "따옴표가 닫히지 않았다: $line" }
            fields += cur.toString()
            return fields
        }
    }
}
