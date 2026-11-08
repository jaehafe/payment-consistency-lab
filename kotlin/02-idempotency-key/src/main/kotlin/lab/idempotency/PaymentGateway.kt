package lab.idempotency

import org.springframework.stereotype.Component
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** 외부 결제사(PG) 승인 API. 토스페이먼츠/Stripe처럼 idempotency key를 지원한다. */
interface PaymentGateway {
    /** @return PG 거래 ID (pgTxId) */
    fun charge(idempotencyKey: String, orderId: String, amount: Long): String
}

open class PaymentGatewayException(message: String) : RuntimeException(message)

/** 승인은 됐는데 응답이 유실된 경우. 호출자는 "성공인지 실패인지 모른다". */
class PaymentGatewayTimeoutException(message: String) : PaymentGatewayException(message)

/** 테스트/학습용 in-process Fake PG. 실패 주입 토글과 검증용 카운터를 가진다. */
@Component
class FakePaymentGateway : PaymentGateway {
    /** 승인 전에 에러 (돈은 안 빠져나감) */
    @Volatile var failBeforeCharge = false

    /** 승인은 됐는데 응답이 유실 → 호출자는 타임아웃만 봄 (돈은 빠져나감!) */
    @Volatile var timeoutAfterCharge = false

    /** PG 응답 지연. 동시성 테스트에서 요청들이 겹치도록 만든다. */
    @Volatile var latency: Duration = Duration.ZERO

    private val txIdsByKey = ConcurrentHashMap<String, String>()
    private val chargeCount = AtomicInteger()
    private val callCount = AtomicInteger()

    override fun charge(idempotencyKey: String, orderId: String, amount: Long): String {
        callCount.incrementAndGet()
        if (!latency.isZero) Thread.sleep(latency)
        if (failBeforeCharge) throw PaymentGatewayException("PG 장애 주입: 승인 전 에러")

        // PG 쪽 멱등성: 같은 key로 다시 오면 새로 결제하지 않고 이전 pgTxId를 돌려준다.
        val pgTxId = txIdsByKey.computeIfAbsent(idempotencyKey) {
            chargeCount.incrementAndGet()
            "pg_" + UUID.randomUUID()
        }

        if (timeoutAfterCharge) throw PaymentGatewayTimeoutException("PG 장애 주입: 승인 후 응답 유실(타임아웃)")
        return pgTxId
    }

    /** 실제로 승인(돈이 빠져나감)된 횟수 */
    fun chargeCount(): Int = chargeCount.get()

    /** charge()가 호출된 횟수 (멱등 재응답 포함) — 우리 서버가 PG를 몇 번 두드렸는지 */
    fun callCount(): Int = callCount.get()

    fun reset() {
        failBeforeCharge = false
        timeoutAfterCharge = false
        latency = Duration.ZERO
        txIdsByKey.clear()
        chargeCount.set(0)
        callCount.set(0)
    }
}
