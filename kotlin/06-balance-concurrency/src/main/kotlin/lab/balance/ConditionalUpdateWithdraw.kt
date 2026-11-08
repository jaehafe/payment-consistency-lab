package lab.balance

import org.springframework.stereotype.Component
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate

/**
 * 조건부 UPDATE 한 문장: 검사와 차감을 DB가 원자적으로 한다. 실무에서 가장 흔한 답.
 *   UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?
 * - 1행: 성공. 0행: 잔액 부족.
 * - UPDATE는 행 잠금을 잡고, 기다렸다면 커밋된 최신 행으로 WHERE를 "다시 평가"한다 (Postgres READ COMMITTED 동작).
 *   그래서 두 번째 요청은 balance=0을 보고 0행이 된다.
 * - 앱으로 값을 가져왔다 돌려보내지 않으므로 "읽기와 쓰기 사이의 틈" 자체가 없다.
 * 한계: 조건이 한 행 안에서 표현될 때만 가능하다 (여러 행에 걸친 규칙은 FOR UPDATE나 SERIALIZABLE).
 */
@Component
class ConditionalUpdateWithdraw(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
    private val accounts: AccountRepository,
) : WithdrawStrategy {
    override fun withdraw(accountId: String, amount: Long): WithdrawResult = tx.execute {
        val updated = jdbc.update(
            "UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?",
            amount, accountId, amount,
        )
        if (updated == 0) return@execute WithdrawResult.INSUFFICIENT
        accounts.recordWithdrawal(accountId, amount) // 같은 트랜잭션: 차감과 출금 기록이 함께 커밋/롤백
        WithdrawResult.SUCCESS
    }!!
}
