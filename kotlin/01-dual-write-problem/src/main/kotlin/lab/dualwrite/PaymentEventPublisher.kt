package lab.dualwrite

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.TimeUnit

/** KafkaTemplate을 감싼 발행기. 테스트에서 브로커 장애를 주입하기 위한 토글을 가진다. */
@Component
class PaymentEventPublisher(
    private val kafka: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    @Value("\${lab.topic}") private val topic: String,
) {
    /** true면 브로커가 죽은 것처럼 발행이 실패한다 (장애 주입용) */
    @Volatile
    var brokerDown = false

    fun publish(event: PaymentCompletedEvent) {
        if (brokerDown) throw KafkaException("브로커 장애 주입: payment.events 발행 실패")

        // key = orderId → 같은 주문의 이벤트는 같은 파티션으로 가서 순서가 보장된다.
        val record = ProducerRecord(topic, event.orderId, objectMapper.writeValueAsString(event))
        record.headers().add("message_id", UUID.randomUUID().toString().toByteArray())

        // 동기 발행: 브로커가 ack(acks=all)할 때까지 기다린다.
        // .get()이 리턴하면 이벤트는 이미 브로커 로그에 기록됐고, 누구도 되돌릴 수 없다.
        kafka.send(record).get(10, TimeUnit.SECONDS)
    }
}
