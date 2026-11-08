package lab.outbox.polling

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.util.UUID

/** outbox 테이블의 한 행 = 브로커로 나갈 메시지 한 건 */
data class OutboxMessage(
    val id: UUID,              // Kafka 헤더 message_id (컨슈머 중복 제거 키, 05 참고)
    val aggregateType: String,
    val aggregateId: String,   // Kafka key (= orderId)
    val eventType: String,
    val payload: String,       // JSON
)

@Repository
class OutboxRepository(private val jdbc: JdbcTemplate) {

    fun insert(m: OutboxMessage) {
        jdbc.update(
            "INSERT INTO outbox_polling (id, aggregate_type, aggregate_id, event_type, payload) VALUES (?, ?, ?, ?, ?::jsonb)",
            m.id, m.aggregateType, m.aggregateId, m.eventType, m.payload,
        )
    }

    /**
     * 미발행 행을 오래된 순으로 가져오면서 행 잠금을 건다. 반드시 트랜잭션 안에서 호출해야 한다.
     *
     * - FOR UPDATE   : 이 트랜잭션이 끝날 때까지 다른 relay가 같은 행을 가져가지 못한다.
     * - SKIP LOCKED  : 다른 relay가 이미 잠근 행은 기다리지 않고 건너뛴다.
     *                  → relay 여러 대가 서로 다른 행을 나눠 가지며 병렬로 일한다 (기다림도, 중복도 없음).
     *                  SKIP LOCKED가 없으면 두 번째 relay는 첫 번째가 커밋할 때까지 블록된다.
     *                  FOR UPDATE 자체가 없으면 두 relay가 같은 행을 동시에 읽어서 중복 발행한다.
     */
    fun lockUnpublishedBatch(batchSize: Int): List<OutboxMessage> =
        jdbc.query(
            """
            SELECT id, aggregate_type, aggregate_id, event_type, payload::text AS payload
            FROM outbox_polling
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """.trimIndent(),
            { rs, _ ->
                OutboxMessage(
                    id = rs.getObject("id", UUID::class.java),
                    aggregateType = rs.getString("aggregate_type"),
                    aggregateId = rs.getString("aggregate_id"),
                    eventType = rs.getString("event_type"),
                    payload = rs.getString("payload"),
                )
            },
            batchSize,
        )

    fun markPublished(ids: List<UUID>) {
        if (ids.isEmpty()) return
        jdbc.batchUpdate("UPDATE outbox_polling SET published_at = now() WHERE id = ?", ids.map { arrayOf<Any>(it) })
    }

    fun countUnpublished(): Int =
        jdbc.queryForObject("SELECT count(*) FROM outbox_polling WHERE published_at IS NULL", Int::class.java)!!
}
