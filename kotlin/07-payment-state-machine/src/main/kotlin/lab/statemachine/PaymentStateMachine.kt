package lab.statemachine

import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

/**
 * 모든 상태 변경은 이 클래스를 통한다.
 * 1) 전이표 검사 (코드 버그 차단)
 * 2) compare-and-set UPDATE (동시성 충돌 차단)
 * 3) history insert
 * 2)와 3)은 한 트랜잭션 — 상태는 바뀌었는데 기록이 없는(또는 그 반대) 경우가 없다.
 */
@Component
class PaymentStateMachine(
    private val payments: PaymentRepository,
    private val tx: TransactionTemplate,
) {
    fun transition(
        id: UUID,
        from: PaymentStatus,
        to: PaymentStatus,
        reason: String,
        pgTxId: String? = null,
        lastError: String? = null,
        nextRetryAt: Instant? = null,
        attemptCount: Int? = null,
    ) {
        if (!from.canTransitionTo(to)) throw IllegalTransitionException(from, to)

        tx.executeWithoutResult {
            val updated = payments.compareAndSetStatus(
                id, from, to, pgTxId, lastError, nextRetryAt, attemptCount,
            )
            // 0행 = 현재 상태가 from이 아니다 → 다른 스레드/서버가 먼저 전이시켰다. 덮어쓰지 않고 실패시킨다.
            if (updated == 0) throw StaleStateException(id, from)
            payments.insertHistory(id, from, to, reason)
        }
    }
}
