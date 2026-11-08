package lab.idempotency

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.time.Duration

data class IdempotencyKey(
    val key: String,
    val requestPath: String,
    val requestHash: String,
    val lockActive: Boolean,      // locked_at이 lockTimeout 이내인가
    val recoveryPoint: String,    // started | pg_charged | finished
    val responseCode: Int?,
    val responseBody: String?,
    val pgTxId: String?,
)

object RecoveryPoint {
    const val STARTED = "started"
    const val PG_CHARGED = "pg_charged"
    const val FINISHED = "finished"
}

@Repository
class IdempotencyKeyRepository(private val jdbc: JdbcTemplate) {

    /**
     * 새 key면 "잠근 상태"로 insert하고 true.
     * 이미 있으면 아무것도 안 하고 false (ON CONFLICT DO NOTHING).
     * 동시에 같은 key로 insert하면 UNIQUE 제약 때문에 뒤의 트랜잭션은 앞의 것이 커밋될 때까지 기다린 뒤 0행이 된다.
     */
    fun insertLocked(key: String, path: String, hash: String): Boolean =
        jdbc.update(
            """
            INSERT INTO idempotency_keys (idempotency_key, request_path, request_hash, recovery_point, locked_at)
            VALUES (?, ?, ?, 'started', now())
            ON CONFLICT (idempotency_key) DO NOTHING
            """.trimIndent(),
            key, path, hash,
        ) == 1

    /** 행 잠금(FOR UPDATE)을 잡고 읽는다 — 이 트랜잭션이 끝날 때까지 같은 key를 다른 요청이 판정하지 못하게. */
    fun findForUpdate(key: String, lockTimeout: Duration): IdempotencyKey =
        jdbc.queryForObject(
            """
            SELECT idempotency_key, request_path, request_hash, recovery_point, response_code,
                   response_body::text AS response_body, pg_tx_id,
                   (locked_at IS NOT NULL AND locked_at > now() - make_interval(secs => ?)) AS lock_active
            FROM idempotency_keys WHERE idempotency_key = ?
            FOR UPDATE
            """.trimIndent(),
            { rs, _ ->
                IdempotencyKey(
                    key = rs.getString("idempotency_key"),
                    requestPath = rs.getString("request_path"),
                    requestHash = rs.getString("request_hash"),
                    lockActive = rs.getBoolean("lock_active"),
                    recoveryPoint = rs.getString("recovery_point"),
                    responseCode = rs.getObject("response_code") as Int?,
                    responseBody = rs.getString("response_body"),
                    pgTxId = rs.getString("pg_tx_id"),
                )
            },
            lockTimeout.toMillis() / 1000.0, key,
        )!!

    fun lock(key: String) {
        jdbc.update("UPDATE idempotency_keys SET locked_at = now(), last_run_at = now() WHERE idempotency_key = ?", key)
    }

    fun unlock(key: String) {
        jdbc.update("UPDATE idempotency_keys SET locked_at = NULL WHERE idempotency_key = ?", key)
    }

    fun markPgCharged(key: String, pgTxId: String) {
        jdbc.update(
            "UPDATE idempotency_keys SET recovery_point = 'pg_charged', pg_tx_id = ? WHERE idempotency_key = ?",
            pgTxId, key,
        )
    }

    /**
     * @return 저장된 응답 바디 (jsonb가 정규화한 텍스트).
     * jsonb는 키 순서/공백을 바꿔 저장하므로, 첫 응답도 이 값을 돌려줘야 재시도 응답과 바이트 단위까지 같다.
     */
    fun finish(key: String, code: Int, body: String): String =
        jdbc.queryForObject(
            """
            UPDATE idempotency_keys
            SET recovery_point = 'finished', response_code = ?, response_body = ?::jsonb, locked_at = NULL
            WHERE idempotency_key = ?
            RETURNING response_body::text
            """.trimIndent(),
            String::class.java,
            code, body, key,
        )!!
}
