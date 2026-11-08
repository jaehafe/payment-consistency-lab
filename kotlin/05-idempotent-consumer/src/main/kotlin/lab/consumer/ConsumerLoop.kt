package lab.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList

/** message_id 헤더가 없는 메시지. 중복 제거 키가 없으니 처리할 수 없다. */
class MissingMessageIdException(message: String) :
    IllegalStateException(message)

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
    private val deadLetter: DeadLetterPublisher? = null,
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

    // close() 중에 오는 revoke 콜백은 무시하기 위한 표시 (아래 rebalanceListener)
    @Volatile private var closing = false

    /**
     * 리밸런스 때 DeadLetterPublisher의 실패 기록을 정리한다 (메모리 누수 방지).
     * - revoked/lost: 이 인스턴스에서 빠진 파티션의 기록을 지운다
     * - assigned: 지금 할당 말고는 지운다 (재생성 사이에 다른 인스턴스로 넘어간 파티션)
     *
     * ★ close() 중의 revoke는 무시한다. ConsumerRunner는 실패할 때마다 컨슈머를 닫고
     * 새로 만드는데, KafkaConsumer.close()는 그룹을 떠나며 onPartitionsRevoked를 부른다.
     * 그때 지우면 재시도마다 횟수가 0으로 돌아가 poison message가 영영 DLT에 못 간다.
     * 같은 인스턴스가 다시 그 파티션을 받으면 기록이 그대로 이어진다.
     */
    internal val rebalanceListener = object : ConsumerRebalanceListener {
        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
            if (!closing) deadLetter?.forgetPartitions(partitions)
        }

        override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
            if (!closing) deadLetter?.forgetPartitions(partitions)
        }

        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
            // partitions는 cooperative 프로토콜에서 "새로 받은 것"만이라 assignment() 전체를 쓴다
            deadLetter?.retainPartitions(consumer.assignment())
        }
    }

    init {
        consumer.subscribe(listOf(topic), rebalanceListener)
    }

    /** @return 이번 poll에서 받은 메시지 수 */
    fun pollOnce(timeout: Duration = Duration.ofMillis(500)): Int {
        val records = consumer.poll(timeout)
        if (records.isEmpty) return 0

        // 파티션별 "여기까지 끝냄" (처리 or 스킵 or DLT ack). 커밋할 offset = 다음 위치
        val done = mutableMapOf<TopicPartition, OffsetAndMetadata>()
        for (record in records) {
            try {
                handleRecord(record)
                deadLetter?.clear(record)
            } catch (e: Exception) {
                try {
                    deadLetterOrRethrow(record, e)
                } catch (fatal: Exception) {
                    // 실패한 레코드 "앞"까지만 커밋하고 던진다 → ConsumerRunner가
                    // 컨슈머를 새로 만들면 실패한 레코드부터 다시 받는다.
                    // (앞에서 DLT로 보낸 레코드가 다시 와서 DLT에 또 가는 것을 막는다)
                    commitBeforeFailure(done, fatal)
                    throw fatal
                }
            }
            done[TopicPartition(record.topic(), record.partition())] =
                OffsetAndMetadata(record.offset() + 1)
        }

        if (crashBeforeOffsetCommit) throw SimulatedCrashException()

        // DB 커밋(과 DLT ack)이 모두 끝난 "다음에" offset을 커밋한다.
        // (간단히 poll 배치 단위로 커밋. 배치 중간에 죽으면 배치 전체가 재전송되지만, 멱등 컨슈머라 괜찮다)
        consumer.commitSync(done)
        return records.count()
    }

    private fun handleRecord(record: ConsumerRecord<String, String>) {
        // message_id가 없으면 중복 제거 키가 없으므로 처리할 수 없다
        // → 무엇이 빠졌는지 알 수 있게 실패한다. deadLetter가 있으면
        // 재시도 없이 곧장 DLT, 없으면 계속 다시 온다 (poison message).
        val where = "${record.topic()}-${record.partition()}@${record.offset()}"
        val messageId = record.headers().lastHeader("message_id")
            ?.value()?.let { String(it) }
            ?: throw MissingMessageIdException(
                "message_id 헤더 없음: $where (key=${record.key()})",
            )
        val event = objectMapper.readValue<PaymentCompletedEvent>(record.value())
        // 리턴 시점에 DB 트랜잭션은 이미 커밋됨
        val processed = handler.handle(messageId, event)
        deliveries += Delivery(
            messageId, event.orderId, processed,
            record.partition(), record.offset(),
        )
    }

    /**
     * 처리 실패한 레코드를 어떻게 할지 정한다 (Spring DefaultErrorHandler가 하는 일).
     * - 아직 재시도할 차례 → 원래 예외를 다시 던진다 (ConsumerRunner가 재시도)
     * - 한도 도달/재시도 무의미 → DLT로 보내고 ack까지 기다린 뒤 정상 리턴
     *   → 호출자가 이 레코드를 done에 넣고 다음 레코드로 넘어간다
     * - DLT 발행 실패 → DeadLetterPublishException (커밋 안 됨)
     *
     * ★ DLT ack를 받은 "뒤에만" offset을 넘긴다. 커밋을 먼저 하고 DLT 발행이 실패하면
     * 메시지는 원래 토픽에서도 DLT에서도 사라진다(유실). 이 순서면 최악이 "DLT에 두 번"이다.
     */
    private fun deadLetterOrRethrow(
        record: ConsumerRecord<String, String>,
        e: Exception,
    ) {
        val dlt = deadLetter ?: throw e
        if (!dlt.recordFailure(record, e)) throw e
        dlt.publish(record, e)
    }

    private fun commitBeforeFailure(
        done: Map<TopicPartition, OffsetAndMetadata>,
        failure: Exception,
    ) {
        if (done.isEmpty()) return
        // 커밋이 실패해도 무해하다: 성공분이 다시 오면
        // processed_messages가 건너뛴다
        runCatching { consumer.commitSync(done) }
            .onFailure { failure.addSuppressed(it) }
    }

    // enable.auto.commit=false이므로 close()는 offset을 커밋하지 않는다 → 커밋 안 된 메시지는 재전송된다.
    override fun close() {
        closing = true
        consumer.close()
    }
}
