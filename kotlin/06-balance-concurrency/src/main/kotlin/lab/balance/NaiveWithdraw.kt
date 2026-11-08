package lab.balance

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate

/**
 * [안티패턴] READ COMMITTED에서 읽고 → 앱에서 검사 → "읽은 값 - amount"를 덮어쓴다.
 *
 * 트랜잭션으로 감쌌지만 소용없다. READ COMMITTED의 SELECT는 잠금을 걸지 않으므로
 * 10개 요청이 모두 balance=1000을 읽고, 모두 "잔액 충분"이라 판단하고, 모두 balance=0을 쓴다.
 * 결과: 10건 출금 성공(10,000원 지급), 잔액은 0 → 9,000원이 허공에서 생겼다 (이중 지불 / lost update).
 */
@Component
class NaiveWithdraw(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val accounts: AccountRepository,
) : WithdrawStrategy {
    /** 읽기와 쓰기 사이 지연(ms). 테스트에서 레이스를 결정적으로 만들기 위해 주입한다. */
    @Volatile var readWriteDelayMs = 0L

    override fun withdraw(accountId: String, amount: Long): WithdrawResult = tx.execute {
        val balance = jdbc.queryForObject("SELECT balance FROM accounts WHERE id = ?", Long::class.java, accountId)!!
        if (balance < amount) return@execute WithdrawResult.INSUFFICIENT

        pause(readWriteDelayMs) // ← 이 틈에 다른 트랜잭션들도 같은 balance를 읽는다

        // 앱이 계산한 "절대값"을 쓴다. 그 사이 다른 트랜잭션이 바꾼 값은 덮어써져 사라진다.
        jdbc.update("UPDATE accounts SET balance = ? WHERE id = ?", balance - amount, accountId)
        accounts.recordWithdrawal(accountId, amount)
        WithdrawResult.SUCCESS
    }!!
}
