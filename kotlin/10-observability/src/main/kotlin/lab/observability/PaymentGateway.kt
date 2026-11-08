package lab.observability

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

interface PaymentGateway {
    fun charge(idempotencyKey: String, orderId: String, amount: Long): String
}

open class PgException(message: String) : RuntimeException(message)
class PgTimeoutException(message: String) : PgException(message)
class PgDeclinedException(message: String) : PgException(message)

/**
 * Fake PG. 장애 시나리오(지연 급증, 타임아웃 급증)를 토글로 주입한다. /admin/pg-faults 로 실행 중에도 바꿀 수 있다.
 *
 * 타임아웃은 "즉시 예외"가 아니라 호출자가 timeoutMs 동안 기다린 뒤에 난다 (실제 HTTP 클라이언트의 read timeout).
 * 즉시 던지면 pg_call_duration_seconds 에 수 ms로 기록돼 PgLatencyHigh(p99 > 3s)가 절대 뜨지 않는다.
 */
@Component
class FakePaymentGateway(
    // 운영 기본 5초(application.yml). 테스트는 짧게 덮어쓴다.
    @Value("\${lab.pg.timeout-ms:5000}") private val configuredTimeoutMs: Long,
) : PaymentGateway {
    @Volatile var latencyMs = 20L                 // 평소 응답 시간
    @Volatile var timeoutAfterCharge = false      // 승인 후 응답 유실 → UNKNOWN
    @Volatile var declineCharge = false           // 명확한 거절 → FAILED
    @Volatile var timeoutMs = configuredTimeoutMs // 호출자(우리) 쪽 PG 응답 타임아웃

    private val txIds = ConcurrentHashMap<String, String>()

    override fun charge(idempotencyKey: String, orderId: String, amount: Long): String {
        if (declineCharge) {
            // 거절 응답도 타임아웃 뒤에 오면 우리는 모른다 → 타임아웃(UNKNOWN)
            if (latencyMs >= timeoutMs) {
                sleep(timeoutMs)
                throw PgTimeoutException("PG 응답 타임아웃 (${timeoutMs}ms)")
            }
            sleep(latencyMs)
            throw PgDeclinedException("PG 거절: 한도 초과")
        }
        // 승인은 PG 쪽에서 먼저 일어난다. 그 뒤 응답이 타임아웃 안에 오지 않으면 우리는 결과를 모른다 → UNKNOWN
        val pgTxId = txIds.computeIfAbsent(idempotencyKey) { "pg_" + UUID.randomUUID() }
        if (timeoutAfterCharge || latencyMs >= timeoutMs) {
            sleep(timeoutMs)
            throw PgTimeoutException("PG 응답 타임아웃 (${timeoutMs}ms)")
        }
        sleep(latencyMs)
        return pgTxId
    }

    private fun sleep(ms: Long) {
        if (ms > 0) Thread.sleep(ms)
    }

    fun reset() {
        latencyMs = 20
        timeoutAfterCharge = false
        declineCharge = false
        timeoutMs = configuredTimeoutMs
    }
}
