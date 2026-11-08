package lab.outbox.cdc

import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** 외부 결제사(PG) 승인 API. 토스페이먼츠/Stripe처럼 idempotency key를 지원한다. */
interface PaymentGateway {
    /** @return PG 거래 ID (pgTxId) */
    fun charge(idempotencyKey: String, orderId: String, amount: Long): String
}

/** 테스트/학습용 Fake PG. 이 단계의 주제는 outbox이므로 PG는 항상 성공한다. (PG 멱등성은 02 참고) */
@Component
class FakePaymentGateway : PaymentGateway {
    private val txIdsByKey = ConcurrentHashMap<String, String>()
    private val chargeCount = AtomicInteger()

    override fun charge(idempotencyKey: String, orderId: String, amount: Long): String =
        txIdsByKey.computeIfAbsent(idempotencyKey) {
            chargeCount.incrementAndGet()
            "pg_" + UUID.randomUUID()
        }

    fun chargeCount(): Int = chargeCount.get()
}
