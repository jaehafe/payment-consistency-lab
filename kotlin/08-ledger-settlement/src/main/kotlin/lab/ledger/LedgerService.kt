package lab.ledger

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID

data class PostResult(val entryId: UUID?, val created: Boolean)

/**
 * 복식부기 원장에 분개를 전기(post)한다.
 *
 * 규칙: 모든 거래는 차변 합 = 대변 합. 돈은 생기거나 사라지지 않고 계정 사이를 "이동"만 한다.
 * 그래서 전체 계정 잔액(debit-positive)의 합은 항상 0이다 (시산표 검증).
 * 잔액만 UPDATE 하는 단식 방식은 "이 잔액이 왜 이 값인가"를 설명할 수 없고, 버그로 돈이 새도 알아챌 방법이 없다.
 */
@Service
class LedgerService(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
) {
    fun post(entry: JournalEntry): PostResult {
        validate(entry)

        // 호출자가 이미 트랜잭션 안이면 그 트랜잭션에 참여한다 (PROPAGATION_REQUIRED, 기본값)
        // → 원천 거래 insert와 분개가 함께 커밋/롤백된다.
        return tx.execute {
            val entryId = UUID.randomUUID()
            // 원장 멱등성: 같은 거래(reference)는 UNIQUE 제약으로 한 번만 들어간다.
            // 재시도/중복 메시지(05)로 같은 결제가 두 번 와도 이중 전기되지 않는다.
            val inserted = jdbc.update(
                """
                INSERT INTO journal_entries (id, reference_type, reference_id, description) VALUES (?, ?, ?, ?)
                ON CONFLICT (reference_type, reference_id) DO NOTHING
                """.trimIndent(),
                entryId, entry.referenceType, entry.referenceId, entry.description,
            )
            if (inserted == 0) return@execute PostResult(null, created = false)

            // 계정 id 순서로 갱신한다: 여러 분개가 동시에 같은 계정들을 갱신할 때 잠금 순서를 통일해 데드락 방지 (06의 이체와 같은 원리)
            for (line in entry.lines.sortedBy { it.accountId }) {
                jdbc.update(
                    "INSERT INTO ledger_accounts (id, type) VALUES (?, ?) ON CONFLICT (id) DO NOTHING",
                    line.accountId, Accounts.typeOf(line.accountId),
                )
                jdbc.update(
                    "INSERT INTO ledger_lines (entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                    entryId, line.accountId, line.debit, line.credit,
                )
                // 잔액 캐시: 라인 합계를 매번 SUM 하지 않기 위해. 라인과 같은 트랜잭션이라 항상 일치한다.
                jdbc.update(
                    "UPDATE ledger_accounts SET balance = balance + ? - ? WHERE id = ?",
                    line.debit, line.credit, line.accountId,
                )
            }
            PostResult(entryId, created = true)
        }!!
    }

    private fun validate(entry: JournalEntry) {
        if (entry.lines.size < 2) throw UnbalancedEntryException("분개는 최소 두 줄이어야 한다")
        for (l in entry.lines) {
            val oneSided = (l.debit > 0 && l.credit == 0L) || (l.credit > 0 && l.debit == 0L)
            if (!oneSided) throw UnbalancedEntryException("한 줄은 차변 또는 대변 한쪽만 양수여야 한다: $l")
        }
        val debits = entry.lines.sumOf { it.debit }
        val credits = entry.lines.sumOf { it.credit }
        if (debits != credits) throw UnbalancedEntryException("차변 합($debits) ≠ 대변 합($credits): ${entry.referenceType}/${entry.referenceId}")
    }

    fun balanceOf(accountId: String): Long =
        jdbc.query("SELECT balance FROM ledger_accounts WHERE id = ?", { rs, _ -> rs.getLong(1) }, accountId)
            .firstOrNull() ?: 0L
}
