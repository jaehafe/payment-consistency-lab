package lab.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.Consumer
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList

/** 컨슈머가 "DB 커밋 후, offset 커밋 전"에 죽는 상황을 흉내내는 예외 */
class SimulatedCrashException : RuntimeException("장애 주입: DB 커밋 후, offset 커밋 전에 프로세스 사망")

/**
 * Kafka poll 루프. 순서가 핵심이다:
 *
 *   poll → (메시지마다) DB 트랜잭션 커밋 → offset 커밋(commitSync)
 *
 * - offset을 먼저 커밋하고 DB 처리 중 죽으면 → 그 메시지는 다시 오지 않는다 = 유실 (at-most-once)
 * - DB를 먼저 커밋하고 offset 커밋 전에 죽으면 → 재시작 후 같은 메시지가 다시 온다 = 중복 (at-least-once)
 * 유실은 되돌릴 수 없지만 중복은 IdempotentConsumer가 걸러낼 수 있다 → 후자를 택한다.
 */
class ConsumerLoop(
    private val consumer: Consumer<String, String>,
    topic: String,
    private val handler: MessageHandler,
    private val objectMapper: ObjectMapper,
) : AutoCloseable {
    /** 테스트 관찰용: 이 루프가 받은 메시지와 처리 여부 */
    data class Delivery(
        val messageId: String,
        val orderId: String,
        val processed: Boolean,
        val partition: Int,
        val offset: Long,
    )

    val deliveries: MutableList<Delivery> = CopyOnWriteArrayList()

    /** true면 DB 처리 후 offset 커밋 직전에 죽는다 */
    @Volatile var crashBeforeOffsetCommit = false

    init {
        consumer.subscribe(listOf(topic))
    }

    /** @return 이번 poll에서 받은 메시지 수 */
    fun pollOnce(timeout: Duration = Duration.ofMillis(500)): Int {
        val records = consumer.poll(timeout)
        if (records.isEmpty) return 0

        for (record in records) {
            // message_id가 없으면 중복 제거 키가 없으므로 처리할 수 없다 → 무엇이 빠졌는지 알 수 있게 실패한다.
            // (offset은 커밋되지 않으므로 이 메시지는 계속 다시 온다 = poison message. README "한계" 참고)
            val messageId = record.headers().lastHeader("message_id")?.value()?.let { String(it) }
                ?: error("message_id 헤더 없음: ${record.topic()}-${record.partition()}@${record.offset()} (key=${record.key()})")
            val event = objectMapper.readValue<PaymentCompletedEvent>(record.value())
            val processed = handler.handle(messageId, event) // 리턴 시점에 DB 트랜잭션은 이미 커밋됨
            deliveries += Delivery(messageId, event.orderId, processed, record.partition(), record.offset())
        }

        if (crashBeforeOffsetCommit) throw SimulatedCrashException()

        // DB 커밋이 모두 끝난 "다음에" offset을 커밋한다.
        // (간단히 poll 배치 단위로 커밋. 배치 중간에 죽으면 배치 전체가 재전송되지만, 멱등 컨슈머라 괜찮다)
        consumer.commitSync()
        return records.count()
    }

    // enable.auto.commit=false이므로 close()는 offset을 커밋하지 않는다 → 커밋 안 된 메시지는 재전송된다.
    override fun close() = consumer.close()
}
