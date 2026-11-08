package lab.outbox.polling

import java.time.Instant
import java.util.UUID

data class Payment(
    val id: UUID,
    val orderId: String,
    val amount: Long,
    val status: String,
    val pgTxId: String,
)

/** Kafka `payment.events` 토픽에 실리는 이벤트 payload (SPEC 공통 포맷) */
data class PaymentCompletedEvent(
    val paymentId: String,
    val orderId: String,
    val amount: Long,
    val pgTxId: String,
    val occurredAt: Instant,
) {
    companion object {
        fun from(p: Payment) = PaymentCompletedEvent(p.id.toString(), p.orderId, p.amount, p.pgTxId, Instant.now())
    }
}
