package lab.statemachine

import java.time.Instant
import java.util.UUID

data class Payment(
    val id: UUID,
    val orderId: String,
    val amount: Long,
    val status: PaymentStatus,
    val idempotencyKey: String,
    val pgTxId: String?,
    val attemptCount: Int,
    val nextRetryAt: Instant?,
    val lastError: String?,
    val createdAt: Instant,
)
