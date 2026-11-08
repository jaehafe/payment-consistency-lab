package lab.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/** DLT 발행이 실패했다. 원래 메시지의 offset은 커밋하면 안 된다. */
class DeadLetterPublishException(message: String, cause: Throwable) :
    RuntimeException(message, cause)

/**
 * 계속 실패하는 메시지(poison message)를 DLT로 치운다.
 * Spring Kafka의 DefaultErrorHandler(실패 횟수 세기, 재시도 판단) +
 * DeadLetterPublishingRecoverer(DLT 발행)를 손으로 옮긴 것.
 *
 * ConsumerRunner는 실패할 때마다 ConsumerLoop를 새로 만들므로,
 * 실패 횟수는 루프가 아니라 루프보다 오래 사는 이 객체에 둔다.
 * (Spring에서도 FailedRecordTracker는 에러 핸들러 안에 있다)
 *
 * 예외는 [FailureClassifier]로 나눈다:
 *  - TRANSIENT(DB·네트워크 장애): 세지 않는다, DLT로 안 간다 (ConsumerRunner가 백오프로 재시도)
 *  - NOT_RETRYABLE: 1번 만에 DLT
 *  - RETRYABLE: maxAttempts번째에 DLT
 *
 * ★ 실패 횟수는 이 인스턴스의 메모리에만 있다 = maxAttempts는 "컨슈머 인스턴스 하나 기준".
 * 재시작하거나 리밸런스로 파티션이 넘어가면 0부터 다시 센다 (README "한계").
 * 한 컨슈머(한 스레드)에 하나씩 쓴다 — ConsumerLoop의 리밸런스 리스너가 지금 할당되지
 * 않은 파티션의 기록을 지우므로, 여러 컨슈머가 공유하면 서로의 기록을 지운다.
 */
class DeadLetterPublisher(
    private val producer: Producer<String, String>,
    val maxAttempts: Int,
    private val consumerGroup: String,
    private val sendTimeout: Duration = Duration.ofSeconds(30),
    private val classifier: (Throwable) -> FailureKind =
        FailureClassifier::classify,
) : AutoCloseable {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        // 첫 실패 때가 아니라 시작할 때 잡는다: 0 이하면 첫 실패에 곧장 DLT (재시도 없음)
        require(maxAttempts >= 1) { "maxAttempts must be >= 1, got $maxAttempts" }
        require(!sendTimeout.isNegative && !sendTimeout.isZero) {
            "sendTimeout must be > 0, got $sendTimeout"
        }
    }

    private data class RecordKey(
        val topic: String,
        val partition: Int,
        val offset: Long,
    )

    // (토픽, 파티션, offset) → RETRYABLE/NOT_RETRYABLE 실패 횟수.
    // 메모리에만 있으므로 재시작하면 0부터 다시 센다 (README "한계").
    private val attempts = ConcurrentHashMap<RecordKey, Int>()

    private fun keyOf(r: ConsumerRecord<*, *>) =
        RecordKey(r.topic(), r.partition(), r.offset())

    /**
     * 실패 1회를 기록한다.
     * @return true = 재시도를 그만두고 DLT로 보낼 차례
     */
    fun recordFailure(record: ConsumerRecord<*, *>, e: Exception): Boolean {
        val kind = classifier(e)
        if (kind == FailureKind.TRANSIENT) {
            log.warn(
                "인프라 장애(transient), DLT 대상 아님 — 백오프 후 재시도: {}-{}@{}: {}",
                record.topic(), record.partition(), record.offset(), e.toString(),
            )
            return false
        }
        val n = attempts.merge(keyOf(record), 1, Int::plus)!!
        val giveUp = n >= maxAttempts || kind == FailureKind.NOT_RETRYABLE
        if (!giveUp) {
            log.warn(
                "처리 실패 {}/{} ({}), 재시도: {}-{}@{}: {}", n, maxAttempts,
                kind, record.topic(), record.partition(), record.offset(),
                e.toString(),
            )
        }
        return giveUp
    }

    /** 처리에 성공한 레코드의 실패 기록을 지운다 */
    fun clear(record: ConsumerRecord<*, *>) {
        attempts.remove(keyOf(record))
    }

    /** 이 인스턴스에서 빠진(revoked/lost) 파티션의 실패 기록을 지운다 */
    fun forgetPartitions(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) return
        attempts.keys.removeIf { TopicPartition(it.topic, it.partition) in partitions }
    }

    /** 지금 할당된 파티션 말고는 지운다 (재생성 중 다른 인스턴스로 넘어간 파티션 정리) */
    fun retainPartitions(assigned: Collection<TopicPartition>) {
        attempts.keys.removeIf { TopicPartition(it.topic, it.partition) !in assigned }
    }

    /**
     * DLT로 보내고 브로커 ack까지 기다린다 (실패하면 예외).
     * key·value·헤더는 그대로 두고 실패 정보 헤더를 덧붙인다.
     */
    fun publish(record: ConsumerRecord<String, String>, cause: Exception) {
        val n = attempts[keyOf(record)] ?: 1
        val headers = RecordHeaders(record.headers().toArray())
        fun add(k: String, v: String) = headers.add(k, v.toByteArray())
        add(ORIGINAL_TOPIC, record.topic())
        add(ORIGINAL_PARTITION, record.partition().toString())
        add(ORIGINAL_OFFSET, record.offset().toString())
        add(ORIGINAL_CONSUMER_GROUP, consumerGroup)
        add(EXCEPTION_FQCN, cause.javaClass.name)
        add(EXCEPTION_MESSAGE, cause.message ?: "")
        add(ATTEMPTS, n.toString())

        // partition = null → 같은 key는 DLT에서도 같은 파티션
        // (Spring 기본은 "원래와 같은 번호의 파티션")
        val out = ProducerRecord(
            dltTopic(record.topic()), null,
            record.key(), record.value(), headers,
        )
        try {
            // ★ get()으로 ack를 기다린다. 이게 끝나야 호출자가 offset을 커밋한다.
            producer.send(out)
                .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: Exception) {
            val where =
                "${record.topic()}-${record.partition()}@${record.offset()}"
            throw DeadLetterPublishException(
                "DLT 발행 실패 ${out.topic()}: $where", e,
            )
        }
        attempts.remove(keyOf(record))
        log.warn(
            "DLT로 보냄 (시도 {}번): {}-{}@{} → {}: {}", n, record.topic(),
            record.partition(), record.offset(), out.topic(), cause.toString(),
        )
    }

    /** 테스트 관찰용: 그 레코드의 지금까지 실패 횟수 */
    fun attempts(topic: String, partition: Int, offset: Long): Int =
        attempts[RecordKey(topic, partition, offset)] ?: 0

    override fun close() = producer.close()

    companion object {
        // Spring Kafka KafkaHeaders.DLT_* 와 같은 이름. 차이: Spring은
        // partition/offset을 4/8바이트 big-endian으로 넣지만, 여기서는
        // rpk로 바로 읽히게 전부 UTF-8 문자열로 넣는다.
        const val ORIGINAL_TOPIC = "kafka_dlt-original-topic"
        const val ORIGINAL_PARTITION = "kafka_dlt-original-partition"
        const val ORIGINAL_OFFSET = "kafka_dlt-original-offset"
        const val ORIGINAL_CONSUMER_GROUP =
            "kafka_dlt-original-consumer-group"
        const val EXCEPTION_FQCN = "kafka_dlt-exception-fqcn"
        const val EXCEPTION_MESSAGE = "kafka_dlt-exception-message"
        const val ATTEMPTS = "kafka_dlt-attempts" // lab 전용

        /** Spring Kafka 3.3+ 기본값과 같은 "<topic>-dlt" (그 전엔 ".DLT") */
        fun dltTopic(topic: String) = "$topic-dlt"
    }
}
