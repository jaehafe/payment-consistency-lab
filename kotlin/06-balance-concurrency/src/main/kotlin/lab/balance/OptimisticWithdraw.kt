package lab.balance

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.util.concurrent.atomic.AtomicInteger

/**
 * 낙관적 락: 잠그지 않고 읽은 뒤, 쓸 때 "내가 읽은 version 그대로인가?"를 조건으로 건다.
 *   UPDATE ... SET balance=?, version=version+1 WHERE id=? AND version=?
 * 0행이면 그 사이 누군가 바꿨다는 뜻 → 처음부터 다시 읽고 재시도.
 * 충돌이 드물면 잠금 대기가 없어 빠르지만, 충돌이 잦으면(핫 계좌) 재시도 폭풍이 된다.
 */
@Component
class OptimisticWithdraw(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val accounts: AccountRepository,
) : WithdrawStrategy {
    @Volatile var readWriteDelayMs = 0L
    var maxAttempts = 20

    /** 테스트 관찰용: 버전 충돌로 재시도한 총 횟수 */
    val retries = AtomicInteger()

    override fun withdraw(accountId: String, amount: Long): WithdrawResult {
        repeat(maxAttempts) { attempt ->
            if (attempt > 0) retries.incrementAndGet()
            val result = tx.execute {
                val (balance, version) = jdbc.queryForObject(
                    "SELECT balance, version FROM accounts WHERE id = ?",
                    { rs, _ -> rs.getLong("balance") to rs.getLong("version") },
                    accountId,
                )!!
                if (balance < amount) return@execute WithdrawResult.INSUFFICIENT

                pause(readWriteDelayMs)

                // compare-and-set: 읽은 version이 그대로일 때만 쓴다
                val updated = jdbc.update(
                    "UPDATE accounts SET balance = ?, version = version + 1 WHERE id = ? AND version = ?",
                    balance - amount, accountId, version,
                )
                if (updated == 0) return@execute null // 충돌 → 재시도
                accounts.recordWithdrawal(accountId, amount)
                WithdrawResult.SUCCESS
            }
            if (result != null) return result
        }
        throw RetryExhaustedException("낙관적 락 재시도 $maxAttempts 회 초과")
    }
}
