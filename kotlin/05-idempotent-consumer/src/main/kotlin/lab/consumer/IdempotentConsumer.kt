package lab.consumer

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate

/**
 * 멱등 컨슈머: message_id(= outbox.id)를 processed_messages에 기록해서 같은 메시지를 두 번 처리하지 않는다.
 */
class IdempotentConsumer(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val consumerGroup: String,
) : MessageHandler {
    override fun handle(messageId: String, event: PaymentCompletedEvent): Boolean = tx.execute {
        // 1) "처리했음" 표시를 먼저 시도한다. 이미 있으면(PK 충돌) 0행 → 중복 메시지이므로 건너뛴다.
        //    두 컨슈머가 같은 메시지를 동시에 잡아도(리밸런스 직후 등) PK 덕분에 하나만 1행을 얻는다.
        //    (뒤의 트랜잭션은 앞의 것이 커밋될 때까지 기다렸다가 0행이 된다)
        val inserted = jdbc.update(
            "INSERT INTO processed_messages (consumer_group, message_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
            consumerGroup, messageId,
        )
        if (inserted == 0) return@execute false

        // 2) 실제 부작용. 표시와 "같은 트랜잭션"이라는 게 핵심이다.
        //    - 부작용이 실패하면 표시도 함께 롤백 → 재전송 시 다시 처리된다 (유실 없음)
        //    - 표시가 커밋됐다면 부작용도 반드시 커밋됐다 (중복 없음)
        //    표시를 별도 트랜잭션/Redis 같은 다른 저장소에 두면 이 둘 사이에 또 틈이 생긴다 (01의 이중 쓰기와 같은 문제).
        PaymentCompletedEffects.apply(jdbc, event)
        true
    }!!
}
