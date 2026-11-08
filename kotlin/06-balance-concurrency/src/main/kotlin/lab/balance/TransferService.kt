package lab.balance

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate

class InsufficientBalanceException(accountId: String) : RuntimeException("잔액 부족: $accountId")

/**
 * 계좌 이체: 두 행을 잠가야 한다.
 *
 * 데드락이 생기는 경우: T1이 A→B (A 잠금 후 B 대기), T2가 B→A (B 잠금 후 A 대기) → 서로를 영원히 기다림.
 * Postgres는 deadlock_timeout(기본 1초) 후 이를 감지하고 한쪽을 SQLSTATE 40P01로 죽인다.
 *
 * 해결: 이체 방향과 상관없이 "항상 id 순서로" 잠근다. 모두가 같은 순서로 잠그면 순환 대기가 생길 수 없다.
 */
@Service
class TransferService(
    private val jdbc: JdbcTemplate,
    private val tx: TransactionTemplate,
) {
    /** 첫 번째 잠금과 두 번째 잠금 사이 지연(ms). 테스트에서 잠금 구간이 확실히 겹치게 한다. */
    @Volatile var lockGapMs = 0L

    /** 올바른 버전: id 순서로 잠근다 */
    fun transfer(from: String, to: String, amount: Long) = tx.executeWithoutResult {
        val (first, second) = listOf(from, to).sorted() // ← 핵심: 방향과 무관한 전역 잠금 순서
        lock(first)
        pause(lockGapMs)
        lock(second)
        move(from, to, amount)
    }

    /** [안티패턴] 요청 방향대로(from → to) 잠근다. 반대 방향 이체가 동시에 오면 데드락. */
    fun transferUnordered(from: String, to: String, amount: Long) = tx.executeWithoutResult {
        lock(from)
        pause(lockGapMs)
        lock(to)
        move(from, to, amount)
    }

    private fun lock(id: String) {
        jdbc.queryForObject("SELECT balance FROM accounts WHERE id = ? FOR UPDATE", Long::class.java, id)
    }

    private fun move(from: String, to: String, amount: Long) {
        // 두 행 모두 잠금을 쥔 상태이므로 조건 검사 후 갱신해도 안전하다
        val updated = jdbc.update("UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?", amount, from, amount)
        if (updated == 0) throw InsufficientBalanceException(from)
        jdbc.update("UPDATE accounts SET balance = balance + ? WHERE id = ?", amount, to)
    }
}
