package lab.observability

import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

interface PaymentGateway {
    fun charge(idempotencyKey: String, orderId: String, amount: Long): String
}

open class PgException(message: String) : RuntimeException(message)
class PgTimeoutException(message: String) : PgException(message)
class PgDeclinedException(message: String) : PgException(message)

/** Fake PG. 장애 시나리오(지연 급증, 타임아웃 급증)를 토글로 주입한다. /admin/pg-faults 로 실행 중에도 바꿀 수 있다. */
@Component
class FakePaymentGateway : PaymentGateway {
    @Volatile var latencyMs = 20L                 // 평소 응답 시간
    @Volatile var timeoutAfterCharge = false      // 승인 후 응답 유실 → UNKNOWN
    @Volatile var declineCharge = false           // 명확한 거절 → FAILED

    private val txIds = ConcurrentHashMap<String, String>()

    override fun charge(idempotencyKey: String, orderId: String, amount: Long): String {
        if (latencyMs > 0) Thread.sleep(latencyMs)
        if (declineCharge) throw PgDeclinedException("PG 거절: 한도 초과")
        val pgTxId = txIds.computeIfAbsent(idempotencyKey) { "pg_" + UUID.randomUUID() }
        if (timeoutAfterCharge) throw PgTimeoutException("PG 응답 타임아웃")
        return pgTxId
    }

    fun reset() {
        latencyMs = 20
        timeoutAfterCharge = false
        declineCharge = false
    }
}
