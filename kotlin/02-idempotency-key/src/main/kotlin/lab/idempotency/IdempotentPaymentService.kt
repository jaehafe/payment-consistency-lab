package lab.idempotency

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.security.MessageDigest
import java.time.Duration
import java.util.UUID

/**
 * 멱등키로 보호되는 결제 처리.
 *
 * 하나의 요청을 여러 "atomic phase"(각각 짧은 DB 트랜잭션)로 나누고,
 * phase 사이사이의 진행 상황을 idempotency_keys.recovery_point에 기록한다.
 * 어느 지점에서 죽든, 같은 key로 재시도하면 기록된 지점부터 이어서 실행한다.
 *
 *   Phase 1 (tx)  key 생성/검증/잠금                         recovery_point = started
 *   외부 호출      PG charge — 트랜잭션 밖!
 *   Phase 2 (tx)  payments insert + pg_tx_id 기록           recovery_point = pg_charged
 *   Phase 3 (tx)  응답 저장 + 잠금 해제                        recovery_point = finished
 */
@Service
class IdempotentPaymentService(
    private val pg: PaymentGateway,
    private val keys: IdempotencyKeyRepository,
    private val payments: PaymentRepository,
    private val tx: TransactionTemplate,
    private val objectMapper: ObjectMapper,
    @Value("\${lab.idempotency.lock-timeout}") private val lockTimeout: Duration,
) {
    fun pay(idempotencyKey: String, requestPath: String, request: PaymentRequest): StoredResponse {
        val requestHash = sha256(objectMapper.writeValueAsString(request))

        // ---- Phase 1 (tx): key upsert + 검증 + 잠금 ----
        val key = tx.execute { phase1LockKey(idempotencyKey, requestPath, requestHash) }!!

        // 이미 끝난 요청이면 저장해 둔 응답을 그대로 재생한다. PG도, payments도 건드리지 않는다.
        if (key.recoveryPoint == RecoveryPoint.FINISHED) {
            return StoredResponse(key.responseCode!!, key.responseBody!!)
        }

        try {
            if (key.recoveryPoint == RecoveryPoint.STARTED) {
                // ---- 외부 호출 (트랜잭션 밖) ----
                // PG 승인은 "외부 세계의 상태 변경"이라 DB 롤백으로 되돌릴 수 없다.
                // 그래서 트랜잭션 안에 넣어봐야 원자성은 안 생기고, 느린 외부 호출 동안 DB 커넥션/락만 붙잡게 된다.
                // 대신 우리 idempotency key를 PG에도 그대로 넘긴다 → 재시도해도 PG가 중복 승인하지 않는다.
                val pgTxId = pg.charge(idempotencyKey, request.orderId, request.amount)

                // ---- Phase 2 (tx): 결제 저장 + 진행 지점 기록을 "한 트랜잭션"으로 ----
                // 둘이 같이 커밋되므로 "payments는 있는데 recovery_point는 started" 같은 어중간한 상태가 없다.
                tx.executeWithoutResult {
                    payments.insert(UUID.randomUUID(), request.orderId, request.amount, pgTxId)
                    keys.markPgCharged(idempotencyKey, pgTxId)
                }
            }

            // ---- Phase 3 (tx): 응답 저장 + finished + 잠금 해제 ----
            return tx.execute {
                val body = objectMapper.writeValueAsString(payments.findByOrderId(request.orderId))
                // 재생(replay)될 응답과 바이트 단위까지 같도록, 저장된(jsonb 정규화된) 바디를 그대로 돌려준다.
                StoredResponse(201, keys.finish(idempotencyKey, 201, body))
            }!!
        } catch (e: PaymentGatewayException) {
            // PG 에러/타임아웃은 "재시도 가능한 에러"다. 잠금을 풀어 두면 클라이언트가 같은 key로 바로 재시도할 수 있고,
            // recovery_point가 started로 남아 있으므로 재시도는 PG 호출부터 다시 한다 (PG 멱등성 덕분에 이중 승인 없음).
            keys.unlock(idempotencyKey)
            throw IdempotencyException(503, "PG 호출 실패, 같은 Idempotency-Key로 재시도하세요: ${e.message}")
        } catch (e: Exception) {
            keys.unlock(idempotencyKey)
            throw e
        }
    }

    /** 반드시 트랜잭션 안에서 호출된다 (FOR UPDATE 행 잠금이 트랜잭션 끝까지 유지되어야 하므로). */
    private fun phase1LockKey(idempotencyKey: String, path: String, hash: String): IdempotencyKey {
        val created = keys.insertLocked(idempotencyKey, path, hash)
        val key = keys.findForUpdate(idempotencyKey, lockTimeout)
        if (created) return key // 처음 보는 key → 이미 잠근 상태로 만들었다

        // 같은 key인데 요청 내용이 다르다 → 클라이언트 버그. 절대 이전 응답을 돌려주면 안 된다.
        if (key.requestPath != path || key.requestHash != hash) {
            throw IdempotencyException(422, "같은 Idempotency-Key로 다른 요청이 들어왔습니다")
        }
        if (key.recoveryPoint == RecoveryPoint.FINISHED) return key

        // 다른 요청이 지금 처리 중이다. 기다리게 하지 않고 바로 409로 거절한다.
        if (key.lockActive) throw IdempotencyException(409, "같은 Idempotency-Key 요청이 처리 중입니다")

        // 잠금이 없거나(이전 시도가 재시도 가능 에러로 끝남) 만료됐다(처리하던 서버가 죽음) → 이어받는다.
        keys.lock(idempotencyKey)
        return key
    }

    // 요청 바디를 그대로가 아니라 "파싱 후 재직렬화"한 값으로 해시한다.
    // 공백/필드 순서 차이 때문에 같은 요청이 다른 요청으로 판정되는 것을 막기 위해.
    private fun sha256(s: String): String =
        MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }
}
