package lab.statemachine

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

@Repository
class PaymentRepository(private val jdbc: JdbcTemplate) {
    private val mapper = RowMapper { rs, _ ->
        Payment(
            id = rs.getObject("id", UUID::class.java),
            orderId = rs.getString("order_id"),
            amount = rs.getLong("amount"),
            status = PaymentStatus.valueOf(rs.getString("status")),
            idempotencyKey = rs.getString("idempotency_key"),
            pgTxId = rs.getString("pg_tx_id"),
            attemptCount = rs.getInt("attempt_count"),
            nextRetryAt = rs.getTimestamp("next_retry_at")?.toInstant(),
            lastError = rs.getString("last_error"),
            createdAt = rs.getTimestamp("created_at").toInstant(),
        )
    }

    fun insertRequested(id: UUID, orderId: String, amount: Long, idempotencyKey: String) {
        jdbc.update(
            "INSERT INTO payments (id, order_id, amount, status, idempotency_key) VALUES (?, ?, ?, 'REQUESTED', ?)",
            id, orderId, amount, idempotencyKey,
        )
    }

    fun find(id: UUID): Payment = jdbc.queryForObject("SELECT * FROM payments WHERE id = ?", mapper, id)!!

    /**
     * compare-and-set 전이: "현재 상태가 from일 때만" to로 바꾼다. 영향 행 수(0 또는 1)를 돌려준다.
     * SELECT 해서 상태를 확인한 뒤 UPDATE 하면 그 사이에 다른 처리가 끼어들 수 있다 (06의 나이브 출금과 같은 문제).
     * WHERE status = ? 를 UPDATE에 넣으면 확인과 변경이 한 문장으로 원자적이다.
     */
    fun compareAndSetStatus(
        id: UUID, from: PaymentStatus, to: PaymentStatus,
        pgTxId: String?, lastError: String?, nextRetryAt: Instant?,
    ): Int = jdbc.update(
        """
        UPDATE payments
        SET status = ?, pg_tx_id = coalesce(?, pg_tx_id), last_error = coalesce(?, last_error),
            next_retry_at = ?, updated_at = now()
        WHERE id = ? AND status = ?
        """.trimIndent(),
        to.name, pgTxId, lastError, nextRetryAt?.let(Timestamp::from), id, from.name,
    )

    fun insertHistory(paymentId: UUID, from: PaymentStatus?, to: PaymentStatus, reason: String?) {
        jdbc.update(
            "INSERT INTO payment_status_history (payment_id, from_status, to_status, reason) VALUES (?, ?, ?, ?)",
            paymentId, from?.name, to.name, reason,
        )
    }

    /**
     * janitor가 처리할 행을 "집어 간다(claim)".
     * FOR UPDATE SKIP LOCKED로 다른 janitor와 겹치지 않게 고르고, next_retry_at을 lease만큼 미뤄 둔 뒤 바로 커밋한다.
     * → PG 조회(느린 외부 호출) 동안 DB 트랜잭션/행 잠금을 쥐고 있지 않아도, 다른 janitor가 같은 행을 다시 집지 않는다.
     *   (janitor가 처리 도중 죽으면 lease가 지난 뒤 다른 janitor가 이어받는다)
     */
    fun claimDue(status: PaymentStatus, now: Instant, leaseUntil: Instant, limit: Int): List<Payment> =
        jdbc.query(
            """
            UPDATE payments SET next_retry_at = ?
            WHERE id IN (
              SELECT id FROM payments
              WHERE status = ? AND next_retry_at <= ?
              ORDER BY next_retry_at
              LIMIT ?
              FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """.trimIndent(),
            mapper,
            Timestamp.from(leaseUntil), status.name, Timestamp.from(now), limit,
        )

    /** 상태는 그대로 두고 다음 확인 시각만 미룬다 (CAS: 그 사이 상태가 바뀌었으면 0행) */
    fun scheduleRetry(id: UUID, expected: PaymentStatus, attemptCount: Int, nextRetryAt: Instant, lastError: String): Int =
        jdbc.update(
            """
            UPDATE payments SET attempt_count = ?, next_retry_at = ?, last_error = ?, updated_at = now()
            WHERE id = ? AND status = ?
            """.trimIndent(),
            attemptCount, Timestamp.from(nextRetryAt), lastError, id, expected.name,
        )

    fun setAttemptCount(id: UUID, attemptCount: Int) {
        jdbc.update("UPDATE payments SET attempt_count = ? WHERE id = ?", attemptCount, id)
    }
}
