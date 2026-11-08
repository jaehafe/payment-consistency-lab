package lab.outbox.polling

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.time.Instant

/**
 * 테스트용: 토픽을 처음부터 "현재 끝"까지 읽어서 조건에 맞는 메시지를 모은다.
 * 발행이 동기(send().get())이므로, 읽기 시작 시점의 end offset까지만 읽으면
 * "이벤트가 없다"도 타임아웃 대기 없이 확정적으로 판단할 수 있다.
 */
class EventProbe(private val bootstrapServers: String, private val topic: String) {
    data class Received(val key: String, val value: String, val messageId: String?, val eventType: String?)

    fun eventsFor(key: String): List<Received> = events { it.key == key }

    fun events(filter: (Received) -> Boolean): List<Received> {
        val props = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        )
        KafkaConsumer<String, String>(props).use { consumer ->
            val partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10))
                .map { TopicPartition(topic, it.partition()) }
            consumer.assign(partitions)
            consumer.seekToBeginning(partitions)
            val end = consumer.endOffsets(partitions)

            val out = mutableListOf<Received>()
            val deadline = Instant.now().plusSeconds(15)
            while (partitions.any { consumer.position(it) < end.getValue(it) }) {
                check(Instant.now() < deadline) { "토픽 끝까지 읽지 못함" }
                for (r in consumer.poll(Duration.ofMillis(200))) {
                    val messageId = r.headers().lastHeader("message_id")?.value()?.let { String(it) }
                    val eventType = r.headers().lastHeader("event_type")?.value()?.let { String(it) }
                    val received = Received(r.key(), r.value(), messageId, eventType)
                    if (filter(received)) out += received
                }
            }
            return out
        }
    }
}
