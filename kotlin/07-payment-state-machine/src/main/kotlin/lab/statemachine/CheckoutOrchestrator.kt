package lab.statemachine

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant

/**
 * 주문 결제 오케스트레이션 (Saga): 결제 승인 → 재고 확보.
 *
 * 재고 확보가 실패하면 이미 승인된 결제를 DB 롤백으로 되돌릴 수 없다 (PG는 외부 시스템).
 * 그래서 "반대 동작"인 PG 취소를 실행한다 = 보상(compensation) 트랜잭션.
 * 보상조차 실패할 수 있으므로, 먼저 CANCEL_REQUESTED("취소해야 함")를 커밋해 두고 취소를 시도한다.
 * 취소가 실패하면 그 상태로 남고 janitor가 재시도한다 → 보상이 유실되지 않는다.
 */
@Service
class CheckoutOrchestrator(
    private val paymentService: PaymentService,
    private val stateMachine: PaymentStateMachine,
    private val payments: PaymentRepository,
    private val pg: PaymentGateway,
    private val inventory: InventoryClient,
    @Value("\${lab.janitor.base-backoff}") private val baseBackoff: Duration,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun checkout(orderId: String, amount: Long, sku: String): Payment {
        val payment = paymentService.pay(orderId, amount)
        if (payment.status != PaymentStatus.APPROVED) return payment // FAILED/UNKNOWN이면 후속 단계로 가지 않는다

        try {
            inventory.reserve(orderId, sku)
            return payment
        } catch (e: Exception) {
            log.warn("후속 단계 실패, 결제 보상(취소) 시작: {}", e.message)
            compensate(payment, e.message ?: "후속 단계 실패")
            return payments.find(payment.id)
        }
    }

    private fun compensate(payment: Payment, reason: String) {
        // 1) "취소해야 한다"는 의도를 먼저 기록 (이 커밋 이후엔 서버가 죽어도 janitor가 취소를 이어서 한다)
        stateMachine.transition(
            payment.id, PaymentStatus.APPROVED, PaymentStatus.CANCEL_REQUESTED, "보상: $reason",
            nextRetryAt = Instant.now().plus(baseBackoff),
        )
        // 2) PG 취소 (트랜잭션 밖)
        try {
            pg.cancel(payment.pgTxId!!)
            stateMachine.transition(payment.id, PaymentStatus.CANCEL_REQUESTED, PaymentStatus.CANCELED, "PG 취소 완료")
        } catch (e: PgException) {
            // 취소 실패 → CANCEL_REQUESTED로 남긴다. next_retry_at에 janitor가 재시도.
            log.warn("PG 취소 실패, janitor가 재시도: {}", e.message)
            payments.scheduleRetry(
                payment.id, PaymentStatus.CANCEL_REQUESTED, 1, Instant.now().plus(baseBackoff), e.message ?: "취소 실패",
            )
        }
    }
}
