package lab.statemachine

import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** 외부 결제사(PG) API */
interface PaymentGateway {
    /** 승인. @return pgTxId */
    fun charge(idempotencyKey: String, orderId: String, amount: Long): String

    /** 승인 결과 조회 (타임아웃 후 "그래서 됐어?"를 묻는 API) */
    fun getStatus(idempotencyKey: String): PgStatus

    /** 승인 취소 */
    fun cancel(pgTxId: String)
}

enum class PgStatusCode { APPROVED, FAILED, NOT_FOUND }

data class PgStatus(val code: PgStatusCode, val pgTxId: String? = null)

open class PgException(message: String) : RuntimeException(message)
class PgTimeoutException(message: String) : PgException(message)
class PgDeclinedException(message: String) : PgException(message)

/** 테스트/학습용 Fake PG */
@Component
class FakePaymentGateway : PaymentGateway {
    /** 요청이 PG에 도달하지 못하고 타임아웃 (돈 안 빠짐) */
    @Volatile var timeoutBeforeCharge = false

    /** 승인은 됐는데 응답 유실 → 타임아웃 (돈 빠짐!) */
    @Volatile var timeoutAfterCharge = false

    /** 카드사 거절 등 명확한 실패 응답 */
    @Volatile var declineCharge = false

    /** 취소 API 실패 */
    @Volatile var failCancel = false

    /** 조회 API 자체가 안 됨 (PG 장애) */
    @Volatile var statusUnavailable = false

    /** 테스트 훅: 취소 요청이 PG에 도착한 순간 호출 (경쟁 상황을 결정적으로 재현하기 위해) */
    @Volatile var onCancel: ((pgTxId: String) -> Unit)? = null

    /** 테스트 훅: 승인 요청이 PG에 닿기 전에 호출 (요청이 PG 앞에서 멈춘 상황. 아직 PG 기록 없음) */
    @Volatile var beforeCharge: ((idempotencyKey: String) -> Unit)? = null

    /** 테스트 훅: 승인이 PG에 기록된 직후, 응답을 돌려주기 전에 호출 */
    @Volatile var onCharge: ((idempotencyKey: String) -> Unit)? = null

    /** 테스트 훅: 조회 요청이 PG에 도착한 순간 호출 */
    @Volatile var onGetStatus: ((idempotencyKey: String) -> Unit)? = null

    private data class Record(val pgTxId: String, val code: PgStatusCode, @Volatile var canceled: Boolean = false)

    private val records = ConcurrentHashMap<String, Record>()
    private val chargeCount = AtomicInteger()
    private val cancelCount = AtomicInteger()

    override fun charge(idempotencyKey: String, orderId: String, amount: Long): String {
        beforeCharge?.invoke(idempotencyKey)
        if (timeoutBeforeCharge) throw PgTimeoutException("PG 장애 주입: 요청이 PG에 도달하지 못함")
        if (declineCharge) {
            records.putIfAbsent(idempotencyKey, Record("pg_" + UUID.randomUUID(), PgStatusCode.FAILED))
            throw PgDeclinedException("PG 거절: 한도 초과")
        }
        // PG 멱등성: 같은 key면 기존 승인을 돌려준다
        val record = records.computeIfAbsent(idempotencyKey) {
            chargeCount.incrementAndGet()
            Record("pg_" + UUID.randomUUID(), PgStatusCode.APPROVED)
        }
        onCharge?.invoke(idempotencyKey)
        if (timeoutAfterCharge) throw PgTimeoutException("PG 장애 주입: 승인 후 응답 유실")
        return record.pgTxId
    }

    override fun getStatus(idempotencyKey: String): PgStatus {
        onGetStatus?.invoke(idempotencyKey)
        if (statusUnavailable) throw PgException("PG 장애 주입: 조회 API 응답 없음")
        val r = records[idempotencyKey] ?: return PgStatus(PgStatusCode.NOT_FOUND)
        return PgStatus(r.code, r.pgTxId)
    }

    override fun cancel(pgTxId: String) {
        if (failCancel) throw PgException("PG 장애 주입: 취소 실패")
        onCancel?.invoke(pgTxId)
        // 모르는 거래면 PG 오류로 응답한다 (실제 PG도 오류 코드로 답한다).
        // first { } 의 NoSuchElementException 은 PgException 이 아니라
        // janitor 배치를 통째로 멈췄다.
        val r = records.values.firstOrNull { it.pgTxId == pgTxId }
            ?: throw PgException("PG: 알 수 없는 pgTxId $pgTxId")
        if (!r.canceled) {
            r.canceled = true
            cancelCount.incrementAndGet()
        }
    }

    fun chargeCount() = chargeCount.get()
    fun cancelCount() = cancelCount.get()
    fun isCanceled(pgTxId: String) = records.values.any { it.pgTxId == pgTxId && it.canceled }

    fun reset() {
        timeoutBeforeCharge = false
        timeoutAfterCharge = false
        declineCharge = false
        failCancel = false
        statusUnavailable = false
        onCancel = null
        beforeCharge = null
        onCharge = null
        onGetStatus = null
        records.clear()
        chargeCount.set(0)
        cancelCount.set(0)
    }
}
