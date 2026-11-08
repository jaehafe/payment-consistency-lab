package lab.outbox.cdc

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID

@Service
class PaymentService(
    private val pg: PaymentGateway,
    private val payments: PaymentRepository,
    private val outbox: OutboxRepository,
    private val tx: TransactionTemplate,
    private val objectMapper: ObjectMapper,
) {
    fun pay(orderId: String, amount: Long): Payment {
        // PG 승인은 트랜잭션 밖 (02 참고)
        val pgTxId = pg.charge(idempotencyKey = orderId, orderId = orderId, amount = amount)
        val payment = Payment(UUID.randomUUID(), orderId, amount, "COMPLETED", pgTxId)

        // 03과 똑같이 결제 + outbox를 한 트랜잭션에 쓴다. 애플리케이션의 책임은 여기까지.
        // 커밋되면 이 변경이 WAL(Write-Ahead Log)에 기록되고, Debezium이 WAL을 tail 하다가
        // outbox insert를 발견하면 Outbox Event Router가 payment.events 토픽으로 메시지를 만든다.
        // 롤백되면 WAL에 커밋 레코드가 없으므로 Debezium도 아무것도 보내지 않는다.
        tx.executeWithoutResult {
            payments.insert(payment)
            outbox.insert(
                OutboxEvent(
                    id = UUID.randomUUID(),
                    aggregateType = "payment",      // → 토픽 "payment.events" (route.topic.replacement=${routedByValue}.events)
                    aggregateId = orderId,          // → Kafka key (주문 단위 순서 보장)
                    type = "PaymentCompleted",      // → 헤더 event_type
                    payload = objectMapper.writeValueAsString(PaymentCompletedEvent.from(payment)),
                ),
            )
        }
        return payment
    }
}
