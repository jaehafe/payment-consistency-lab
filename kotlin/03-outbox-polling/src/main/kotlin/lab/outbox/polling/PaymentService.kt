package lab.outbox.polling

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
    private val dbFailure: DbFailureInjector,
) {
    fun pay(orderId: String, amount: Long): Payment {
        // PG 승인은 트랜잭션 밖 (외부 호출 동안 DB 커넥션/락을 잡지 않기 위해. 02 참고)
        val pgTxId = pg.charge(idempotencyKey = orderId, orderId = orderId, amount = amount)
        val payment = Payment(UUID.randomUUID(), orderId, amount, "COMPLETED", pgTxId)

        // 핵심: 결제 저장과 "이벤트를 보내야 한다는 사실"을 같은 DB의 같은 트랜잭션에 쓴다.
        // Kafka에 직접 보내지 않으므로 두 시스템에 걸친 이중 쓰기가 사라지고,
        // 둘 다 커밋되거나 둘 다 롤백된다 (Postgres가 원자성을 보장).
        // 실제 발행은 OutboxRelay가 나중에 비동기로 한다.
        tx.executeWithoutResult {
            payments.insert(payment)
            outbox.insert(
                OutboxMessage(
                    id = UUID.randomUUID(),
                    aggregateType = "payment",
                    aggregateId = orderId,
                    eventType = "PaymentCompleted",
                    payload = objectMapper.writeValueAsString(PaymentCompletedEvent.from(payment)),
                ),
            )
            dbFailure.maybeFail()
        }
        return payment
    }
}
