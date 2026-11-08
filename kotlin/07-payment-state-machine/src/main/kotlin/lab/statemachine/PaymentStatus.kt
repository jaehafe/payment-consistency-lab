package lab.statemachine

/**
 * 결제 상태와 허용 전이표. 여기 없는 전이는 전부 예외다.
 *
 *   REQUESTED → PENDING → APPROVED | FAILED | UNKNOWN
 *   UNKNOWN   → APPROVED | FAILED | MANUAL_REVIEW
 *   APPROVED  → CANCEL_REQUESTED → CANCELED
 *   FAILED    → CANCEL_REQUESTED  (늦은 승인 망취소 전용)
 *
 * UNKNOWN이 핵심이다. PG 타임아웃은 "실패"가 아니라 "모름"이다 — 승인됐을 수도 있다.
 * FAILED로 단정하면 고객 돈은 빠져나갔는데 주문은 실패한 상태가 되고, 재결제하면 이중 결제가 된다.
 */
enum class PaymentStatus {
    REQUESTED, PENDING, APPROVED, FAILED, UNKNOWN, MANUAL_REVIEW, CANCEL_REQUESTED, CANCELED;

    fun canTransitionTo(to: PaymentStatus): Boolean = to in (ALLOWED[this] ?: emptySet())

    companion object {
        val ALLOWED: Map<PaymentStatus, Set<PaymentStatus>> = mapOf(
            REQUESTED to setOf(PENDING),
            PENDING to setOf(APPROVED, FAILED, UNKNOWN),
            UNKNOWN to setOf(APPROVED, FAILED, MANUAL_REVIEW),
            APPROVED to setOf(CANCEL_REQUESTED),
            CANCEL_REQUESTED to setOf(CANCELED),
            // FAILED → CANCEL_REQUESTED는 "늦은 승인" 하나만을 위한 출구다: janitor가 FAILED로
            // 확정한 뒤 진행 중이던 요청이 PG 승인을 받았다(돈이 빠졌다). 되살리지(→ APPROVED)
            // 않고 망취소한다. PaymentService.netCancel.
            FAILED to setOf(CANCEL_REQUESTED),
            // CANCELED, MANUAL_REVIEW: 종결 상태 (MANUAL_REVIEW는 사람이 DB/어드민으로 처리)
        )
    }
}

/** 전이표에 없는 전이 (코드 버그) */
class IllegalTransitionException(from: PaymentStatus, to: PaymentStatus) :
    RuntimeException("허용되지 않은 전이: $from → $to")

/** 기대한 현재 상태가 아니다 = 다른 누군가가 먼저 전이시켰다 (동시성 충돌) */
class StaleStateException(id: Any, expected: PaymentStatus) :
    RuntimeException("결제 $id 의 상태가 이미 $expected 가 아니다 (다른 처리가 먼저 전이함)")
