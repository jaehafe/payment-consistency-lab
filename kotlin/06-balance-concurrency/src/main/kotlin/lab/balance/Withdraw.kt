package lab.balance

import java.sql.SQLException

enum class WithdrawResult { SUCCESS, INSUFFICIENT }

/** 출금 전략. 5가지 구현의 차이는 "읽기 → 검사 → 쓰기" 사이의 틈을 어떻게 막느냐다. */
interface WithdrawStrategy {
    fun withdraw(accountId: String, amount: Long): WithdrawResult
}

/** 재시도해도 결국 실패한 경우 (낙관적 락/SERIALIZABLE에서 경합이 너무 심할 때) */
class RetryExhaustedException(message: String) : RuntimeException(message)

/** 예외 체인을 따라 내려가 Postgres SQLSTATE를 꺼낸다 (Spring이 SQLException을 감싸서 던지므로). */
fun sqlStateOf(e: Throwable): String? {
    var cur: Throwable? = e
    while (cur != null) {
        if (cur is SQLException && cur.sqlState != null) return cur.sqlState
        cur = cur.cause
    }
    return null
}

/** 테스트에서 "읽기와 쓰기 사이"를 벌려 레이스를 결정적으로 재현하기 위한 지연 */
internal fun pause(delayMs: Long) {
    if (delayMs > 0) Thread.sleep(delayMs)
}
