package lab.consumer

import java.time.Instant

/** Kafka `payment.events` 토픽의 이벤트 payload (SPEC 공통 포맷) */
data class PaymentCompletedEvent(
    val paymentId: String,
    val orderId: String,
    val amount: Long,
    val pgTxId: String,
    val occurredAt: Instant,
)
