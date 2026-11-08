package lab.dualwrite

import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** 외부 결제사(PG) 승인 API. 토스페이먼츠/Stripe처럼 idempotency key를 지원한다. */
interface PaymentGateway {
    /** @return PG 거래 ID (pgTxId) */
    fun charge(idempotencyKey: String, orderId: String, amount: Long): String
}

class PaymentGatewayException(message: String) : RuntimeException(message)

/**
 * 테스트/학습용 in-process Fake PG.
 * 이 단계(01)의 주제는 PG가 아니라 "DB 저장 vs Kafka 발행"이므로 PG는 항상 성공한다고 보면 된다.
 */
@Component
class FakePaymentGateway : PaymentGateway {
    private val txIdsByKey = ConcurrentHashMap<String, String>()
    private val chargeCount = AtomicInteger()

    override fun charge(idempotencyKey: String, orderId: String, amount: Long): String =
        // 같은 key로 다시 오면 새로 결제하지 않고 이전 pgTxId를 돌려준다 (PG 쪽 멱등성)
        txIdsByKey.computeIfAbsent(idempotencyKey) {
            chargeCount.incrementAndGet()
            "pg_" + UUID.randomUUID()
        }

    /** 실제로 승인(돈이 빠져나감)된 횟수 */
    fun chargeCount(): Int = chargeCount.get()
}
