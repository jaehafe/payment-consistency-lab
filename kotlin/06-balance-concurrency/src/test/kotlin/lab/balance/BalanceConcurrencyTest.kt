package lab.balance

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.util.UUID

/**
 * 잔액 1,000원 계좌에 1,000원 출금 10건을 "동시에" 보낸다.
 * 정답: 정확히 1건 성공, 9건 잔액 부족, 최종 잔액 0, 출금 기록 합계 1,000.
 */
@SpringBootTest
@Import(TestcontainersConfiguration::class)
class BalanceConcurrencyTest {
    @Autowired lateinit var accounts: AccountRepository
    @Autowired lateinit var naive: NaiveWithdraw
    @Autowired lateinit var pessimistic: PessimisticWithdraw
    @Autowired lateinit var optimistic: OptimisticWithdraw
    @Autowired lateinit var serializable: SerializableWithdraw
    @Autowired lateinit var conditional: ConditionalUpdateWithdraw
    @Autowired lateinit var transfers: TransferService

    private val requests = 10

    @Test
    fun naive__concurrent_withdrawals__double_spending() {
        val account = newAccount(1_000)
        naive.readWriteDelayMs = 300 // 모든 요청이 "쓰기 전에" 잔액을 읽도록 틈을 벌린다

        val results = runConcurrently(requests) { naive.withdraw(account, 1_000) }.map { it.getOrThrow() }
        naive.readWriteDelayMs = 0

        val successes = results.count { it == WithdrawResult.SUCCESS }
        println("naive: 성공 $successes 건, 잔액 ${accounts.balanceOf(account)}, 출금 합계 ${accounts.withdrawnSum(account)}")
        assertTrue(successes > 1, "1,000원짜리 계좌에서 여러 번 출금이 성공했다 = 이중 지불 (성공 $successes 건)")
        assertNotEquals(
            1_000 - accounts.withdrawnSum(account), accounts.balanceOf(account),
            "잔액 ≠ 초기 잔액 - 출금 합계: 갱신이 서로를 덮어썼다 (lost update)",
        )
    }

    @Test
    fun pessimistic__concurrent_withdrawals__exactly_one_succeeds() {
        val account = newAccount(1_000)
        pessimistic.readWriteDelayMs = 100 // 같은 틈을 줘도 FOR UPDATE가 막는다
        val results = runConcurrently(requests) { pessimistic.withdraw(account, 1_000) }.map { it.getOrThrow() }
        pessimistic.readWriteDelayMs = 0

        assertExactlyOneSucceeded(account, results)
    }

    @Test
    fun optimistic__concurrent_withdrawals__exactly_one_succeeds_with_retries() {
        val account = newAccount(1_000)
        val retriesBefore = optimistic.retries.get()
        optimistic.readWriteDelayMs = 200
        val results = runConcurrently(requests) { optimistic.withdraw(account, 1_000) }.map { it.getOrThrow() }
        optimistic.readWriteDelayMs = 0

        assertExactlyOneSucceeded(account, results)
        val retries = optimistic.retries.get() - retriesBefore
        println("optimistic: 버전 충돌 재시도 $retries 회")
        assertTrue(retries > 0, "version 충돌로 재시도가 실제로 일어났다")
    }

    @Test
    fun serializable__concurrent_withdrawals__exactly_one_succeeds_with_retries() {
        val account = newAccount(1_000)
        val retriesBefore = serializable.retries.get()
        serializable.readWriteDelayMs = 200 // 나이브와 똑같은 코드 + 같은 틈. 격리 수준만 다르다
        val results = runConcurrently(requests) { serializable.withdraw(account, 1_000) }.map { it.getOrThrow() }
        serializable.readWriteDelayMs = 0

        assertExactlyOneSucceeded(account, results)
        val retries = serializable.retries.get() - retriesBefore
        println("serializable: 40001 재시도 $retries 회")
        assertTrue(retries > 0, "SQLSTATE 40001(serialization_failure)로 재시도가 실제로 일어났다")
    }

    @Test
    fun conditional_update__concurrent_withdrawals__exactly_one_succeeds() {
        val account = newAccount(1_000)
        val results = runConcurrently(requests) { conditional.withdraw(account, 1_000) }.map { it.getOrThrow() }

        assertExactlyOneSucceeded(account, results)
    }

    @Test
    fun transfer__opposite_directions_concurrently__no_deadlock() {
        val a = newAccount(10_000)
        val b = newAccount(10_000)
        transfers.lockGapMs = 50 // 잠금 사이에 틈이 있어도 순서가 같으면 순환 대기가 없다

        // 짝수 인덱스는 A→B, 홀수는 B→A (각 10건)
        val results = runConcurrently(20) { i ->
            if (i % 2 == 0) transfers.transfer(a, b, 100) else transfers.transfer(b, a, 100)
        }
        transfers.lockGapMs = 0

        val failures = results.mapNotNull { it.exceptionOrNull() }
        assertEquals(emptyList<Throwable>(), failures, "데드락(40P01) 없이 모두 성공")
        assertEquals(20_000, accounts.balanceOf(a) + accounts.balanceOf(b), "총액 보존")
        assertEquals(10_000, accounts.balanceOf(a), "같은 금액을 10번씩 주고받았으므로 원래대로")
    }

    @Test
    fun transfer__unordered_locking__deadlock_detected() {
        val a = newAccount(10_000)
        val b = newAccount(10_000)
        transfers.lockGapMs = 500 // T1: A 잠금 → (대기) → B 요청 / T2: B 잠금 → (대기) → A 요청 → 순환 대기

        val results = runConcurrently(2) { i ->
            if (i == 0) transfers.transferUnordered(a, b, 100) else transfers.transferUnordered(b, a, 100)
        }
        transfers.lockGapMs = 0

        val states = results.map { r -> r.exceptionOrNull()?.let { sqlStateOf(it) } }
        println("unordered transfer 결과 SQLSTATE: $states")
        assertEquals(1, states.count { it == "40P01" }, "Postgres가 데드락을 감지하고 한쪽을 40P01로 죽였다: $states")
        assertEquals(1, results.count { it.isSuccess }, "살아남은 쪽은 성공")
        assertEquals(20_000, accounts.balanceOf(a) + accounts.balanceOf(b), "죽은 쪽은 롤백되어 총액은 보존")
    }

    private fun assertExactlyOneSucceeded(account: String, results: List<WithdrawResult>) {
        assertEquals(1, results.count { it == WithdrawResult.SUCCESS }, "정확히 1건 성공: $results")
        assertEquals(requests - 1, results.count { it == WithdrawResult.INSUFFICIENT })
        assertEquals(0, accounts.balanceOf(account))
        assertEquals(1_000, accounts.withdrawnSum(account), "잔액 변화 = 출금 기록 합계")
    }

    private fun newAccount(balance: Long): String =
        ("acct-" + UUID.randomUUID()).also { accounts.create(it, balance) }
}
