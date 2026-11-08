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
 */
@Component
class PgSettlementFileImporter(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
) {
    private val expectedHeader = "pg_tx_id,order_id,type,amount,fee,transacted_at"

    fun import(fileName: String, content: ByteArray): ImportResult {
        val hash = MessageDigest.getInstance("SHA-256").digest(content).joinToString("") { "%02x".format(it) }
        val lines = String(content).lines().map { it.trim() }.filter { it.isNotEmpty() }
        require(lines.firstOrNull() == expectedHeader) { "CSV 헤더가 다르다: ${lines.firstOrNull()}" }
        val rows = lines.drop(1).map { it.split(",") }

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
}
