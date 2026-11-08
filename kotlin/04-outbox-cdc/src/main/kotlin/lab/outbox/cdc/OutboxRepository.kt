package lab.outbox.cdc

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.util.UUID

data class OutboxEvent(
    val id: UUID,
    val aggregateType: String,
    val aggregateId: String,
    val type: String,
    val payload: String, // JSON
)

/**
 * 03과 달리 insert만 있다. 조회/발행/published_at 갱신 코드가 전부 사라졌다 — 그 일을 Debezium이 한다.
 */
@Repository
class OutboxRepository(private val jdbc: JdbcTemplate) {
    fun insert(e: OutboxEvent) {
        jdbc.update(
            "INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload) VALUES (?, ?, ?, ?, ?::jsonb)",
            e.id, e.aggregateType, e.aggregateId, e.type, e.payload,
        )
    }
}
