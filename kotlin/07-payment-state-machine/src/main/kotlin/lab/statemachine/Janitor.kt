package lab.statemachine

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

/**
 * 확정되지 않은 결제를 뒤처리하는 배치.
 * 앱에선 JanitorScheduler가 주기 실행하고, 테스트는 시각을 넘겨 직접 호출한다.
 *
 * - 오래된 PENDING: 요청 도중 죽은 결제. PG에 물어 확정하거나 UNKNOWN으로
 * - UNKNOWN: PG에 "그래서 승인됐어?"를 물어 APPROVED/FAILED로 확정
 * - CANCEL_REQUESTED: 실패한 PG 취소를 재시도
 *
 * now를 인자로 받는 이유: 지수 백오프/유예 시간을 테스트에서 실제로 기다리지 않고 검증하기 위해.
 */
@Component
class Janitor(
    private val payments: PaymentRepository,
    private val stateMachine: PaymentStateMachine,
    private val pg: PaymentGateway,
    @Value("\${lab.janitor.max-attempts}") private val maxAttempts: Int,
    @Value("\${lab.janitor.base-backoff}") private val baseBackoff: Duration,
    @Value("\${lab.janitor.not-found-grace}") private val notFoundGrace: Duration,
    @Value("\${lab.janitor.lease}") private val lease: Duration,
    @Value("\${lab.janitor.pending-after}") private val pendingAfter: Duration,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        // 0 이하면 방금 PENDING이 된(PG 응답을 기다리는) 결제까지 집는다 → 설정 단계에서 막는다 (Go는 SweepPending이 에러)
        require(!pendingAfter.isNegative && !pendingAfter.isZero) {
            "lab.janitor.pending-after must be > 0 (was $pendingAfter)"
        }
    }

    /**
     * 오래된 PENDING 정리: PENDING 커밋 후 PG 응답을 기록하기 전에
     * 서버가 죽으면 결제가 PENDING에 영원히 남는다.
     * pending-after보다 오래된 PENDING만 집어 PG에 묻는다
     * (진행 중인 요청과 경쟁하지 않게. killbill createdDateBefore).
     *
     * - APPROVED / FAILED → 바로 확정
     * - NOT_FOUND → UNKNOWN과 같은 grace 규칙. grace가 지났으면
     *   FAILED, 아니면 UNKNOWN으로 넘겨 grace 끝에 다시 본다(안 셈)
     * - 판단 불가(PG 조회 오류) → UNKNOWN으로 넘긴다(1회로 셈).
     *   재시도·MANUAL_REVIEW는 UNKNOWN 흐름 하나가 맡는다.
     * 요청 스레드가 먼저 확정했으면 CAS 0행 → 건너뛴다.
     *
     * @return 이번에 집어 간 건수
     */
    fun sweepPending(now: Instant, batchSize: Int = 100): Int {
        val due = payments.claimStalePending(
            now.minus(pendingAfter), now, now.plus(lease), batchSize,
        ) // attempt_count +1 됨
        for (p in due) {
            try {
                settlePending(p, now)
            } catch (e: StaleStateException) {
                // 요청 스레드(PG 응답)가 먼저 확정했다. 덮어쓰지 않는다.
                log.info("다른 처리가 먼저 확정함: {}", e.message)
            }
        }
        return due.size
    }

    private fun settlePending(p: Payment, now: Instant) {
        if (p.attemptCount > maxAttempts) {
            // 집힐 때마다 처리 도중 죽었다. PG도 부르지 않고 UNKNOWN으로
            // 넘긴다 → reconcileUnknown의 crash-loop 규칙이 보류시킨다.
            val error = "janitor: PENDING ${p.attemptCount - 1}회 처리 도중 중단됨"
            toUnknown(p, error, now, p.attemptCount)
            return
        }
        val status = try {
            pg.getStatus(p.idempotencyKey) // 트랜잭션 밖에서 PG 조회
        } catch (e: PgException) {
            // 결과를 모른다 = UNKNOWN. 이번 확인도 1회로 센다.
            val error = e.message ?: "PG 조회 실패"
            val next = now.plus(backoff(p.attemptCount))
            toUnknown(p, error, next, p.attemptCount)
            return
        }
        when (status.code) {
            PgStatusCode.APPROVED -> stateMachine.transition(
                p.id, PaymentStatus.PENDING, PaymentStatus.APPROVED,
                "janitor: 오래된 PENDING, PG 조회 결과 승인",
                pgTxId = status.pgTxId,
            )
            PgStatusCode.FAILED -> stateMachine.transition(
                p.id, PaymentStatus.PENDING, PaymentStatus.FAILED,
                "janitor: 오래된 PENDING, PG 조회 결과 실패",
            )
            PgStatusCode.NOT_FOUND -> {
                val graceEnd = p.createdAt.plus(notFoundGrace)
                if (now >= graceEnd) {
                    // 요청이 PG에 도달하지 못하고 죽었다 (돈 안 빠짐)
                    stateMachine.transition(
                        p.id, PaymentStatus.PENDING, PaymentStatus.FAILED,
                        "janitor: 오래된 PENDING, 유예 후에도 PG 기록 없음",
                    )
                } else {
                    // UNKNOWN 규칙과 같다: grace 안의 NOT_FOUND는
                    // 시도로 세지 않고 grace 끝 시각에 다시 본다.
                    toUnknown(
                        p, "PG 기록 없음 (유예 시간 내)", graceEnd,
                        p.attemptCount - 1,
                    )
                }
            }
        }
    }

    /** PENDING → UNKNOWN: 이후 재시도/보류는 reconcileUnknown이 맡는다 */
    private fun toUnknown(
        p: Payment, error: String, nextRetryAt: Instant, attempts: Int,
    ) = stateMachine.transition(
        p.id, PaymentStatus.PENDING, PaymentStatus.UNKNOWN,
        "janitor: 오래된 PENDING - $error",
        lastError = error, nextRetryAt = nextRetryAt, attemptCount = attempts,
    )

    /** @return 이번에 집어 간(확인한) 건수 */
    fun reconcileUnknown(now: Instant, batchSize: Int = 100): Int {
        val due = payments.claimDue(PaymentStatus.UNKNOWN, now, now.plus(lease), batchSize) // attempt_count +1 됨
        for (p in due) {
            try {
                if (p.attemptCount > maxAttempts) {
                    // 정상 흐름이면 maxAttempts회째 확인에서 MANUAL_REVIEW로 빠진다. 그보다 많이 집혔다
                    // = 이전 시도들이 결과를 남기기 전에 죽었다 (처리 자체가 원인일 수 있으니 PG도 부르지 않는다).
                    val error = "janitor: ${p.attemptCount - 1}회 처리 도중 중단됨 (crash loop 의심)"
                    stateMachine.transition(p.id, PaymentStatus.UNKNOWN, PaymentStatus.MANUAL_REVIEW, error, lastError = error)
                    continue
                }
                val status = pg.getStatus(p.idempotencyKey) // 트랜잭션 밖에서 PG 조회
                when (status.code) {
                    PgStatusCode.APPROVED ->
                        stateMachine.transition(p.id, PaymentStatus.UNKNOWN, PaymentStatus.APPROVED, "janitor: PG 조회 결과 승인", pgTxId = status.pgTxId)
                    PgStatusCode.FAILED ->
                        stateMachine.transition(p.id, PaymentStatus.UNKNOWN, PaymentStatus.FAILED, "janitor: PG 조회 결과 실패")
                    PgStatusCode.NOT_FOUND -> {
                        // 기록이 없다고 바로 FAILED로 단정하지 않는다: 요청이 아직 PG에서 처리 중일 수 있다.
                        // 충분한 시간이 지나도 없으면 "요청이 PG에 도달하지 못했다"고 판단한다.
                        val graceEnd = p.createdAt.plus(notFoundGrace)
                        if (now >= graceEnd) {
                            stateMachine.transition(p.id, PaymentStatus.UNKNOWN, PaymentStatus.FAILED, "janitor: 유예 시간 후에도 PG 기록 없음")
                        } else {
                            // 유예 시간 안의 NOT_FOUND는 "확인 실패"가 아니라 "아직 판단할 때가 아님"이다.
                            // → 시도 횟수를 되돌리고(claim에서 올린 1 취소) 유예가 끝나는 시각으로 바로 재예약.
                            // (시도로 세면 백오프 합(10+20+40+80s) < 유예(5m)라 유예가 끝나기도 전에 MANUAL_REVIEW로 간다)
                            payments.scheduleRetry(p.id, PaymentStatus.UNKNOWN, p.attemptCount - 1, graceEnd, "PG 기록 없음 (유예 시간 내)")
                        }
                    }
                }
            } catch (e: StaleStateException) {
                log.info("다른 처리가 먼저 확정함: {}", e.message) // 문제 없음
            } catch (e: PgException) {
                retryLater(p, now, e.message ?: "PG 조회 실패")
            }
        }
        return due.size
    }

    /** @return 이번에 집어 간 건수 */
    fun retryCancelRequested(now: Instant, batchSize: Int = 100): Int {
        val due = payments.claimDue(PaymentStatus.CANCEL_REQUESTED, now, now.plus(lease), batchSize) // attempt_count +1 됨
        for (p in due) {
            try {
                pg.cancel(p.pgTxId!!) // PG 취소는 pgTxId 기준 멱등이어야 재시도가 안전하다
                stateMachine.transition(p.id, PaymentStatus.CANCEL_REQUESTED, PaymentStatus.CANCELED, "janitor: PG 취소 재시도 성공")
            } catch (e: StaleStateException) {
                // 그 사이 다른 경로가 먼저 확정했다. 이 건은 건너뛰고 배치의 나머지를 계속 처리한다.
                log.info("다른 처리가 먼저 확정함: {}", e.message)
            } catch (e: PgException) {
                // 취소는 반드시 성공시켜야 하는 보상이므로 포기하지 않고 백오프로 계속 재시도한다 (+ 알림 대상)
                val attempts = p.attemptCount
                payments.scheduleRetry(p.id, PaymentStatus.CANCEL_REQUESTED, attempts, now.plus(backoff(attempts)), e.message ?: "취소 실패")
            }
        }
        return due.size
    }

    private fun retryLater(p: Payment, now: Instant, error: String) {
        val attempts = p.attemptCount // claim에서 이번 시도까지 세어 커밋해 둔 값
        if (attempts >= maxAttempts) {
            // 자동으로는 확정할 수 없다 → 보류. 사람이 PG 어드민/정산 파일(09 대사)로 확인한다.
            // (attempt_count는 claim 때 이미 저장됐으므로 전이와 따로 갱신하지 않는다)
            stateMachine.transition(p.id, PaymentStatus.UNKNOWN, PaymentStatus.MANUAL_REVIEW, "janitor: $attempts 회 확인 실패 - $error", lastError = error)
        } else {
            payments.scheduleRetry(p.id, PaymentStatus.UNKNOWN, attempts, now.plus(backoff(attempts)), error)
        }
    }

    /** 지수 백오프: base * 2^(attempts-1). PG 장애 중에 조회 폭탄을 던지지 않기 위해. */
    private fun backoff(attempts: Int): Duration = baseBackoff.multipliedBy(1L shl (attempts - 1).coerceAtMost(20))
}
