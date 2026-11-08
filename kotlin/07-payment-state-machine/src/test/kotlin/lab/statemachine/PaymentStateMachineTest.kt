package lab.statemachine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@SpringBootTest
@Import(TestcontainersConfiguration::class)
class PaymentStateMachineTest {
    @Autowired lateinit var paymentService: PaymentService
    @Autowired lateinit var checkout: CheckoutOrchestrator
    @Autowired lateinit var stateMachine: PaymentStateMachine
    @Autowired lateinit var payments: PaymentRepository
    @Autowired lateinit var janitor: Janitor
    @Autowired lateinit var pg: FakePaymentGateway
    @Autowired lateinit var inventory: FakeInventoryClient
    @Autowired lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun setUp() {
        pg.reset()
        inventory.failReserve = false
        // janitor는 테이블 전체를 훑으므로 이전 테스트가 남긴 UNKNOWN/CANCEL_REQUESTED가 섞이지 않게 비운다
        jdbc.execute("TRUNCATE payments, payment_status_history")
    }

    @Test
    fun invalid_transition__rejected() {
        val payment = paymentService.pay(newOrderId(), 10_000) // APPROVED

        // 승인된 결제를 FAILED로? 전이표에 없다 → 코드 버그로 보고 거부
        assertThrows<IllegalTransitionException> {
            stateMachine.transition(payment.id, PaymentStatus.APPROVED, PaymentStatus.FAILED, "잘못된 전이")
        }
        assertThrows<IllegalTransitionException> {
            stateMachine.transition(payment.id, PaymentStatus.APPROVED, PaymentStatus.CANCELED, "CANCEL_REQUESTED 건너뛰기")
        }
        assertEquals(PaymentStatus.APPROVED, payments.find(payment.id).status, "상태는 그대로")
    }

    @Test
    fun every_transition_recorded_in_history() {
        val payment = paymentService.pay(newOrderId(), 10_000)

        assertEquals(
            listOf(null to "REQUESTED", "REQUESTED" to "PENDING", "PENDING" to "APPROVED"),
            history(payment.id),
        )
    }

    @Test
    fun concurrent_transition__only_one_wins() {
        pg.timeoutAfterCharge = true
        val payment = paymentService.pay(newOrderId(), 10_000)
        assertEquals(PaymentStatus.UNKNOWN, payment.status)

        // janitor 여러 대 + 웹훅 등이 동시에 같은 UNKNOWN 결제를 서로 다른 결과로 확정하려는 상황
        val threads = 10
        val start = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(threads)
        val futures = (0 until threads).map { i ->
            pool.submit<Boolean> {
                start.await()
                val to = if (i % 2 == 0) PaymentStatus.APPROVED else PaymentStatus.FAILED
                try {
                    stateMachine.transition(payment.id, PaymentStatus.UNKNOWN, to, "동시 확정 $i")
                    true
                } catch (e: StaleStateException) {
                    false
                }
            }
        }
        start.countDown()
        val wins = futures.map { it.get(30, TimeUnit.SECONDS) }
        pool.shutdown()

        assertEquals(1, wins.count { it }, "compare-and-set 덕분에 정확히 하나만 이긴다")
        val fromUnknown = history(payment.id).filter { it.first == "UNKNOWN" }
        assertEquals(1, fromUnknown.size, "history에도 UNKNOWN에서 나간 전이는 하나뿐: $fromUnknown")
    }

    @Test
    fun pg_timeout_after_charge__unknown__janitor_confirms_approved() {
        pg.timeoutAfterCharge = true
        val payment = paymentService.pay(newOrderId(), 10_000)

        assertEquals(PaymentStatus.UNKNOWN, payment.status, "타임아웃을 실패로 단정하지 않는다")
        assertEquals(1, pg.chargeCount(), "실제로는 승인됐다")

        janitor.reconcileUnknown(Instant.now().plusSeconds(1))

        val confirmed = payments.find(payment.id)
        assertEquals(PaymentStatus.APPROVED, confirmed.status, "PG 조회로 승인 확정")
        assertTrue(confirmed.pgTxId != null)
        assertEquals(1, pg.chargeCount(), "재결제하지 않았다")
    }

    @Test
    fun pg_timeout_before_charge__unknown__janitor_marks_failed() {
        pg.timeoutBeforeCharge = true
        val payment = paymentService.pay(newOrderId(), 10_000)
        assertEquals(PaymentStatus.UNKNOWN, payment.status)

        // 유예 시간(5분) 안: PG에 기록이 없어도 아직 단정하지 않는다
        janitor.reconcileUnknown(Instant.now().plusSeconds(1))
        val stillUnknown = payments.find(payment.id)
        assertEquals(PaymentStatus.UNKNOWN, stillUnknown.status)
        assertEquals(1, stillUnknown.attemptCount)

        // 유예 시간이 지난 뒤에도 기록 없음 → 요청이 PG에 도달하지 못했다 → FAILED
        janitor.reconcileUnknown(Instant.now().plus(Duration.ofMinutes(10)))
        assertEquals(PaymentStatus.FAILED, payments.find(payment.id).status)
        assertEquals(0, pg.chargeCount())
    }

    @Test
    fun unknown_exceeds_max_attempts__moved_to_manual_review() {
        pg.timeoutAfterCharge = true
        val payment = paymentService.pay(newOrderId(), 10_000)
        pg.statusUnavailable = true // PG 조회 API까지 장애

        // 시각을 넉넉히 넘기며 janitor를 반복 실행 (지수 백오프를 기다리는 대신)
        var now = Instant.now()
        val backoffs = mutableListOf<Duration>()
        repeat(5) {
            now = now.plus(Duration.ofHours(1))
            janitor.reconcileUnknown(now)
            payments.find(payment.id).nextRetryAt?.let { backoffs += Duration.between(now, it) }
        }
        // 마지막 실행은 MANUAL_REVIEW로 가므로 next_retry_at이 비워진다 → 백오프는 4번 기록됨
        assertEquals(listOf(10L, 20L, 40L, 80L), backoffs.map { it.seconds }, "지수 백오프")

        val p = payments.find(payment.id)
        assertEquals(PaymentStatus.MANUAL_REVIEW, p.status, "자동 확정 포기 → 사람 확인 대기(보류)")
        assertEquals(5, p.attemptCount)
        assertEquals("UNKNOWN" to "MANUAL_REVIEW", history(payment.id).last())
    }

    @Test
    fun downstream_failure__compensated_by_pg_cancel() {
        inventory.failReserve = true

        val result = checkout.checkout(newOrderId(), 10_000, "SKU-1")

        assertEquals(PaymentStatus.CANCELED, result.status)
        assertEquals(1, pg.cancelCount())
        assertTrue(pg.isCanceled(result.pgTxId!!), "PG 쪽 승인도 실제로 취소됐다")
        assertEquals(
            listOf("APPROVED" to "CANCEL_REQUESTED", "CANCEL_REQUESTED" to "CANCELED"),
            history(result.id).takeLast(2),
        )
    }

    @Test
    fun cancel_failure__left_cancel_requested__janitor_retries_cancel() {
        inventory.failReserve = true
        pg.failCancel = true

        val result = checkout.checkout(newOrderId(), 10_000, "SKU-1")
        assertEquals(PaymentStatus.CANCEL_REQUESTED, result.status, "취소 실패 → '취소해야 함' 상태로 남는다 (보상 유실 없음)")
        assertEquals(0, pg.cancelCount())

        // backoff 전에는 집어 가지 않는다
        assertEquals(0, janitor.retryCancelRequested(Instant.now()))

        pg.failCancel = false // PG 복구
        janitor.retryCancelRequested(Instant.now().plus(Duration.ofHours(1)))

        assertEquals(PaymentStatus.CANCELED, payments.find(result.id).status)
        assertEquals(1, pg.cancelCount())
    }

    private fun history(paymentId: UUID): List<Pair<String?, String>> =
        jdbc.query(
            "SELECT from_status, to_status FROM payment_status_history WHERE payment_id = ? ORDER BY id",
            { rs, _ -> rs.getString("from_status") to rs.getString("to_status") },
            paymentId,
        )

    private fun newOrderId() = "order-" + UUID.randomUUID()
}
