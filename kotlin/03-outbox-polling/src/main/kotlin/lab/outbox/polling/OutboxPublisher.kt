package lab.outbox.polling

import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit

/** outbox 행을 Kafka 메시지로 바꿔 동기 발행한다. 테스트용 장애/지연 주입 토글을 가진다. */
@Component
class OutboxPublisher(
    private val kafka: KafkaTemplate<String, String>,
    @Value("\${lab.topic}") private val topic: String,
) {
    /** true면 브로커가 죽은 것처럼 발행이 실패한다 */
    @Volatile var brokerDown = false

    /** 발행 1건당 인위적 지연 (relay 동시 실행 테스트에서 두 relay가 확실히 겹치도록) */
    @Volatile var latency: Duration = Duration.ZERO

    fun publish(m: OutboxMessage) {
        if (brokerDown) throw KafkaException("브로커 장애 주입: payment.events 발행 실패")
        if (!latency.isZero) Thread.sleep(latency)

        val record = ProducerRecord(topic, m.aggregateId, m.payload) // key = orderId → 주문 단위 순서 보장
        record.headers().add("message_id", m.id.toString().toByteArray()) // outbox.id = 컨슈머 중복 제거 키
        record.headers().add("event_type", m.eventType.toByteArray())

        // 동기 발행: 브로커 ack(acks=all)를 받아야만 다음 단계(published_at 기록)로 간다.
        // 비동기로 보내고 바로 published_at을 찍으면, 실제로는 안 나간 메시지를 "보냈다"고 기록할 수 있다.
        kafka.send(record).get(10, TimeUnit.SECONDS)
    }
}
