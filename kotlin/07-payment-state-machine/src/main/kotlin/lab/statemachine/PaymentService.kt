package lab.statemachine

import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

@Service
class PaymentService(
    private val payments: PaymentRepository,
    private val stateMachine: PaymentStateMachine,
    private val pg: PaymentGateway,
    private val tx: TransactionTemplate,
) {
    fun pay(orderId: String, amount: Long): Payment {
        val id = UUID.randomUUID()
        val idempotencyKey = UUID.randomUUID().toString() // PG 호출용 key. janitor가 이 key로 결과를 조회한다

        tx.executeWithoutResult {
            payments.insertRequested(id, orderId, amount, idempotencyKey)
            payments.insertHistory(id, null, PaymentStatus.REQUESTED, "결제 요청 생성")
        }

        // PG 호출 "전에" PENDING을 커밋해 둔다. 호출 도중 서버가 죽어도 "PG에 요청했을 수 있음"이 DB에 남는다.
        stateMachine.transition(id, PaymentStatus.REQUESTED, PaymentStatus.PENDING, "PG 승인 요청")

        // PG 호출은 트랜잭션 밖 (02 참고)
        try {
            val pgTxId = pg.charge(idempotencyKey, orderId, amount)
            stateMachine.transition(id, PaymentStatus.PENDING, PaymentStatus.APPROVED, "PG 승인", pgTxId = pgTxId)
        } catch (e: PgDeclinedException) {
            // 명확한 거절 응답 → 확실한 실패
            stateMachine.transition(id, PaymentStatus.PENDING, PaymentStatus.FAILED, "PG 거절", lastError = e.message)
        } catch (e: PgException) {
            // 타임아웃/네트워크 오류 → 승인됐는지 모른다. FAILED로 단정하지 않고 UNKNOWN으로 두고 janitor에게 맡긴다.
            stateMachine.transition(
                id, PaymentStatus.PENDING, PaymentStatus.UNKNOWN, "PG 응답 불명",
                lastError = e.message, nextRetryAt = Instant.now(),
            )
        }
        return payments.find(id)
    }
}
