package lab.balance

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class AccountRepository(private val jdbc: JdbcTemplate) {
    fun create(id: String, balance: Long) {
        jdbc.update("INSERT INTO accounts (id, balance) VALUES (?, ?)", id, balance)
    }

    fun balanceOf(id: String): Long =
        jdbc.queryForObject("SELECT balance FROM accounts WHERE id = ?", Long::class.java, id)!!

    fun recordWithdrawal(accountId: String, amount: Long) {
        jdbc.update("INSERT INTO withdrawals (account_id, amount) VALUES (?, ?)", accountId, amount)
    }

    fun withdrawnSum(accountId: String): Long =
        jdbc.queryForObject("SELECT coalesce(sum(amount), 0) FROM withdrawals WHERE account_id = ?", Long::class.java, accountId)!!
}
