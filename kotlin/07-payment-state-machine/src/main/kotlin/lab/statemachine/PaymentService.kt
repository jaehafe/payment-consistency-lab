package lab.statemachine

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Service
class PaymentService(
    private val payments: PaymentRepository,
    private val stateMachine: PaymentStateMachine,
    private val pg: PaymentGateway,
    private val tx: TransactionTemplate,
    // 늦은 승인을 망취소하다 실패했을 때 janitor가 처음 재시도하기까지의 지연 (오케스트레이터 보상과 같은 값)
    @Value("\${lab.janitor.base-backoff}") private val cancelRetryDelay: Duration,
) {
    private val log = LoggerFactory.getLogger(javaClass)

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
        val outcome = try {
            ChargeOutcome.Approved(pg.charge(idempotencyKey, orderId, amount))
        } catch (e: PgDeclinedException) {
            // 명확한 거절 응답 → 확실한 실패
            ChargeOutcome.Declined(e.message ?: "PG 거절")
        } catch (e: PgException) {
            // 타임아웃/네트워크 오류 → 승인됐는지 모른다. FAILED로 단정하지 않는다.
            ChargeOutcome.Unknown(e.message ?: "PG 응답 불명")
        }
        return record(id, outcome)
    }

    /** PG 응답을 PENDING → APPROVED | FAILED | UNKNOWN 으로 기록한다 */
    private fun record(id: UUID, outcome: ChargeOutcome): Payment {
        try {
            when (outcome) {
                is ChargeOutcome.Approved -> stateMachine.transition(
                    id, PaymentStatus.PENDING, PaymentStatus.APPROVED, "PG 승인", pgTxId = outcome.pgTxId,
                )
                is ChargeOutcome.Declined -> stateMachine.transition(
                    id, PaymentStatus.PENDING, PaymentStatus.FAILED, "PG 거절", lastError = outcome.message,
                )
                // UNKNOWN으로 두고 janitor에게 맡긴다 (FAILED로 단정하면 재결제 → 이중 결제)
                is ChargeOutcome.Unknown -> stateMachine.transition(
                    id, PaymentStatus.PENDING, PaymentStatus.UNKNOWN, "PG 응답 불명",
                    lastError = outcome.message, nextRetryAt = Instant.now(),
                )
            }
        } catch (e: StaleStateException) {
            // PG 응답이 pending-after보다 늦게 와서 janitor가 PENDING을 먼저
            // 정리했다. janitor가 본 PG 기록은 "그때"의 것이라 요청이 지금
            // 아는 결과와 다를 수 있다 → 맞춰 본다.
            return reconcileAfterStale(id, outcome)
        }
        return payments.find(id)
    }

    /**
     * janitor가 먼저 정한 상태와 요청이 PG에서 받은 결과를 맞춘다.
     *
     * | 요청이 아는 것 | 저장된 상태 | 처리 |
     * |---|---|---|
     * | 승인(pgTxId) | APPROVED | 그대로 (같은 결론) |
     * | 승인(pgTxId) | UNKNOWN | UNKNOWN → APPROVED (+pg_tx_id) |
     * | 승인(pgTxId) | FAILED | ★ 늦은 승인 → 망취소: FAILED → CANCEL_REQUESTED → PG 취소 |
     * | 거절 | UNKNOWN | UNKNOWN → FAILED |
     * | 거절 | FAILED | 그대로 |
     * | 모름(타임아웃) | 아무거나 | 그대로 (janitor가 더 많이 안다) |
     * | 그 외 (MANUAL_REVIEW, CANCEL_*, 모순) | | 그대로 + 로그 |
     *
     * 맞추는 전이도 janitor와 또 부딪힐 수 있다 (UNKNOWN을 reconcileUnknown이
     * 동시에 확정). 다시 읽고 다시 판단하되 상한을 둔다. 상태는 앞으로만 가므로
     * 몇 번이면 끝난다.
     */
    private fun reconcileAfterStale(id: UUID, outcome: ChargeOutcome): Payment {
        repeat(MAX_STALE_REREADS) {
            val stored = payments.find(id)
            try {
                when {
                    outcome is ChargeOutcome.Unknown -> return stored

                    outcome is ChargeOutcome.Approved && stored.status == PaymentStatus.UNKNOWN ->
                        stateMachine.transition(
                            id, PaymentStatus.UNKNOWN, PaymentStatus.APPROVED,
                            "PG 승인 (janitor가 UNKNOWN으로 넘긴 뒤 도착)", pgTxId = outcome.pgTxId,
                        )

                    outcome is ChargeOutcome.Declined && stored.status == PaymentStatus.UNKNOWN ->
                        stateMachine.transition(
                            id, PaymentStatus.UNKNOWN, PaymentStatus.FAILED,
                            "PG 거절 (janitor가 UNKNOWN으로 넘긴 뒤 도착)", lastError = outcome.message,
                        )

                    outcome is ChargeOutcome.Approved && stored.status == PaymentStatus.FAILED ->
                        return netCancel(id, outcome.pgTxId)

                    else -> {
                        val agrees = (outcome is ChargeOutcome.Approved && stored.status == PaymentStatus.APPROVED) ||
                            (outcome is ChargeOutcome.Declined && stored.status == PaymentStatus.FAILED)
                        if (!agrees) {
                            // MANUAL_REVIEW(사람 몫)이거나 PG와 모순되는 기록. 자동으로 고치지 않고 남긴다.
                            log.warn("결제 {}: 요청이 받은 PG 결과 {} 와 저장된 상태 {} 가 다르다. 그대로 둔다", id, outcome, stored.status)
                        }
                        return stored
                    }
                }
                return payments.find(id)
            } catch (e: StaleStateException) {
                log.info("맞추는 중 다른 처리가 또 전이함, 다시 읽는다: {}", e.message)
            }
        }
        log.warn("결제 {}: {}번 다시 읽어도 경쟁이 끝나지 않음. 저장된 상태를 돌려준다", id, MAX_STALE_REREADS)
        return payments.find(id)
    }

    /**
     * "janitor가 FAILED로 확정한 뒤 PG 승인이 도착한" 결제를 취소한다.
     *
     * FAILED는 이미 밖으로 나간 결론이다 (고객은 재결제했을 수 있다). 그래서
     * FAILED → APPROVED로 되살리지 않고, 돈을 돌려주는 쪽(취소)으로 간다.
     * PG 연동에서 말하는 망취소(net-cancel): 승인은 됐는데 가맹점이 그 결과를
     * 반영하지 못했으면 승인을 바로 취소한다.
     *
     * 주문 보상과 같은 순서: CANCEL_REQUESTED를 먼저 커밋 → PG 취소. 취소가
     * 실패하거나 그 사이 죽어도 CANCEL_REQUESTED가 남아 janitor가 재시도한다.
     * "돈은 빠졌는데 FAILED, 후속 조치 없음"이 되지 않는다.
     */
    private fun netCancel(id: UUID, pgTxId: String): Payment {
        log.error("결제 {}: janitor가 FAILED로 확정한 뒤 PG 승인(pgTxId={})이 도착 → 망취소", id, pgTxId)
        val reason = "늦은 PG 승인 (FAILED 이후) → 망취소"
        // next_retry_at = 지금 + 지연: 이 요청이 바로 취소를 부르는 동안 janitor가 같은 건을 집어 경쟁하지 않게
        stateMachine.transition(
            id, PaymentStatus.FAILED, PaymentStatus.CANCEL_REQUESTED, reason,
            pgTxId = pgTxId, lastError = reason,
            nextRetryAt = Instant.now().plus(cancelRetryDelay), attemptCount = 0,
        )
        try {
            pg.cancel(pgTxId)
            stateMachine.transition(id, PaymentStatus.CANCEL_REQUESTED, PaymentStatus.CANCELED, "망취소: PG 취소 완료")
        } catch (e: PgException) {
            log.error("결제 {}: 망취소 실패, CANCEL_REQUESTED로 남겨 janitor가 재시도: {}", id, e.message)
            payments.scheduleRetry(
                id, PaymentStatus.CANCEL_REQUESTED, 1, Instant.now().plus(cancelRetryDelay), e.message ?: "취소 실패",
            )
        } catch (e: StaleStateException) {
            log.info("janitor가 먼저 취소를 끝냄: {}", e.message)
        }
        return payments.find(id)
    }

    /** 요청이 PG에서 받은 결과 */
    private sealed interface ChargeOutcome {
        data class Approved(val pgTxId: String) : ChargeOutcome
        data class Declined(val message: String) : ChargeOutcome
        data class Unknown(val message: String) : ChargeOutcome
    }

    private companion object {
        const val MAX_STALE_REREADS = 3
    }
}
