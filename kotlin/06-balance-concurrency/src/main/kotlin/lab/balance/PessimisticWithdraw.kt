package lab.balance

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate

/**
 * 비관적 락: SELECT ... FOR UPDATE로 행을 잠그고 읽는다.
 * 두 번째 요청은 첫 번째가 커밋할 때까지 SELECT에서 기다렸다가, 커밋된 "새" 잔액(0)을 읽는다.
 * → 재시도 없이 항상 정확. 대신 같은 계좌 요청은 줄을 서므로 핫 계좌에서는 처리량이 떨어진다.
 */
@Component
class PessimisticWithdraw(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val accounts: AccountRepository,
) : WithdrawStrategy {
    @Volatile var readWriteDelayMs = 0L

    override fun withdraw(accountId: String, amount: Long): WithdrawResult = tx.execute {
        val balance = jdbc.queryForObject(
            "SELECT balance FROM accounts WHERE id = ? FOR UPDATE", Long::class.java, accountId,
        )!!
        if (balance < amount) return@execute WithdrawResult.INSUFFICIENT

        pause(readWriteDelayMs) // 지연을 넣어도 안전하다: 행 잠금을 쥐고 있으므로 아무도 끼어들 수 없다

        // 상대값(balance - ?)으로 쓴다. Go 버전은 나이브처럼 "읽은 값 - amount" 절대값을 쓴다.
        // FOR UPDATE를 쥐고 있는 한 둘은 똑같이 안전하다. 차이는 FOR UPDATE를 지웠을 때 드러난다:
        //   - 여기(상대값): 10건 모두 통과 → 10번 차감 → 잔액 -9,000 (초과 인출, 잔액 = 1,000 - 출금 합계는 성립)
        //   - Go(절대값):   10건 모두 통과 → 모두 0을 덮어씀 → 잔액 0 (lost update, 합계도 깨짐)
        jdbc.update("UPDATE accounts SET balance = balance - ? WHERE id = ?", amount, accountId)
        accounts.recordWithdrawal(accountId, amount)
        WithdrawResult.SUCCESS
    }!!
}
