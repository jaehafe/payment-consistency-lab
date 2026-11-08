package lab.balance

import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.util.concurrent.atomic.AtomicInteger

/**
 * SERIALIZABLE 격리 수준: 코드는 나이브 버전과 똑같이 "읽고 → 검사 → 절대값 쓰기"인데도 안전하다.
 * Postgres(SSI)가 "이 트랜잭션들을 어떤 순서로 하나씩 실행해도 나올 수 없는 결과"를 감지하면
 * 한쪽을 SQLSTATE 40001(serialization_failure)로 실패시킨다. 앱은 그걸 받아 트랜잭션 전체를 재시도해야 한다.
 * 장점: 복잡한 불변식도 DB가 지켜 준다. 단점: 재시도 로직 필수, 경합 시 abort가 많다.
 */
@Component
class SerializableWithdraw(
    private val jdbc: JdbcTemplate,
    txManager: PlatformTransactionManager,
    private val accounts: AccountRepository,
) : WithdrawStrategy {
    private val tx = TransactionTemplate(txManager).apply {
        isolationLevel = TransactionDefinition.ISOLATION_SERIALIZABLE
    }

    @Volatile var readWriteDelayMs = 0L
    /** 첫 시도를 "포함한" 총 시도 횟수 (Go `MaxAttempts`와 같은 의미) */
    var maxAttempts = 20

    /** 테스트 관찰용: 40001로 재시도한 총 횟수 */
    val retries = AtomicInteger()

    override fun withdraw(accountId: String, amount: Long): WithdrawResult {
        repeat(maxAttempts) {
            try {
                return tx.execute {
                    val balance = jdbc.queryForObject("SELECT balance FROM accounts WHERE id = ?", Long::class.java, accountId)!!
                    if (balance < amount) return@execute WithdrawResult.INSUFFICIENT
                    pause(readWriteDelayMs)
                    jdbc.update("UPDATE accounts SET balance = ? WHERE id = ?", balance - amount, accountId)
                    accounts.recordWithdrawal(accountId, amount)
                    WithdrawResult.SUCCESS
                }!!
            } catch (e: DataAccessException) {
                // 40001만 재시도한다. 다른 에러(제약 위반 등)는 재시도해도 같은 결과이므로 그대로 던진다.
                if (sqlStateOf(e) != "40001") throw e
                retries.incrementAndGet()
            }
        }
        throw RetryExhaustedException("SERIALIZABLE 재시도 $maxAttempts 회 초과")
    }
}
