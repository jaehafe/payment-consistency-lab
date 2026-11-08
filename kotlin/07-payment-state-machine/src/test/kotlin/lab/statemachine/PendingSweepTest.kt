package lab.statemachine

import lab.statemachine.PaymentStatus.APPROVED
import lab.statemachine.PaymentStatus.CANCELED
import lab.statemachine.PaymentStatus.CANCEL_REQUESTED
import lab.statemachine.PaymentStatus.FAILED
import lab.statemachine.PaymentStatus.MANUAL_REVIEW
import lab.statemachine.PaymentStatus.PENDING
import lab.statemachine.PaymentStatus.UNKNOWN
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * 오래된 PENDING 정리 (Janitor.sweepPending).
 * "REQUESTED → PENDING 커밋 후, PG 응답을 기록하기 전에 서버가 죽은"
 * 결제를 행을 직접 넣어 재현한다.
 *
 * 기본 설정: pending-after 2m, not-found-grace 5m,
 * max-attempts 5, base-backoff 10s, lease 1m.
 */
@SpringBootTest(properties = ["lab.janitor.enabled=false"])
@Import(TestcontainersConfiguration::class)
class PendingSweepTest {
    @Autowired lateinit var janitor: Janitor
    @Autowired lateinit var payments: PaymentRepository
    @Autowired lateinit var stateMachine: PaymentStateMachine
    @Autowired lateinit var paymentService: PaymentService
    @Autowired lateinit var pg: FakePaymentGateway
    @Autowired lateinit var jdbc: JdbcTemplate

    private val pendingAfter = Duration.ofMinutes(2)
    private val grace = Duration.ofMinutes(5)

    @BeforeEach
    fun setUp() {
        pg.reset()
        jdbc.execute("TRUNCATE payments, payment_status_history")
    }

    @Test
    fun stuck_pending__pg_approved__janitor_marks_approved() {
        // PG는 승인했는데, 응답을 기록하기 전에 서버가 죽었다
        val created = nowMicros()
        val p = insertPending(created)
        val pgTxId = pg.charge(p.key, p.orderId, 10_000)

        val picked = janitor.sweepPending(created.plus(Duration.ofMinutes(3)))

        assertEquals(1, picked)
        val after = payments.find(p.id)
        assertEquals(APPROVED, after.status)
        assertEquals(pgTxId, after.pgTxId)
        assertNull(after.nextRetryAt, "lease 흔적이 남지 않는다")
        assertEquals(listOf("PENDING" to "APPROVED"), history(p.id))
        assertEquals(1, pg.chargeCount(), "재결제하지 않았다")
    }

    @Test
    fun stuck_pending__no_pg_record__failed_after_grace() {
        // PENDING 커밋 직후, PG를 부르기 전에 죽었다 → PG 기록 없음
        val created = nowMicros()
        val p = insertPending(created)

        // 3분 뒤: threshold(2m)는 지났지만 grace(5m) 안.
        // UNKNOWN 규칙과 같이 아직 FAILED로 단정하지 않는다
        // → UNKNOWN 흐름으로 넘기고, 시도로 세지 않는다.
        janitor.sweepPending(created.plus(Duration.ofMinutes(3)))
        val handed = payments.find(p.id)
        assertEquals(UNKNOWN, handed.status)
        assertEquals(0, handed.attemptCount)
        assertEquals(created.plus(grace), handed.nextRetryAt)

        // grace가 끝나는 시각에 UNKNOWN 흐름이 FAILED로 확정
        janitor.reconcileUnknown(handed.nextRetryAt!!)
        assertEquals(FAILED, payments.find(p.id).status)
        assertEquals(
            listOf("PENDING" to "UNKNOWN", "UNKNOWN" to "FAILED"),
            history(p.id),
        )
        assertEquals(0, pg.chargeCount())
    }

    @Test
    fun stuck_pending__no_pg_record_past_grace__failed_directly() {
        val created = nowMicros()
        val p = insertPending(created)

        janitor.sweepPending(created.plus(Duration.ofHours(1)))

        assertEquals(FAILED, payments.find(p.id).status)
        assertEquals(listOf("PENDING" to "FAILED"), history(p.id))
    }

    @Test
    fun pending_younger_than_threshold__not_touched() {
        val now = nowMicros()
        // 대조군: threshold를 넘긴 PENDING (집혀야 한다)
        val old = insertPending(now.minus(Duration.ofMinutes(3)))
        // 1초 차이로 threshold 안: 요청 스레드가 아직 PG 응답을
        // 기다리는 중일 수 있다. PG에 승인 기록이 있어도 건드리지 않는다.
        val young = insertPending(now.minus(pendingAfter).plusSeconds(1))
        pg.charge(young.key, young.orderId, 10_000)
        val queried = mutableListOf<String>()
        pg.onGetStatus = { queried += it }

        val picked = janitor.sweepPending(now)

        assertEquals(1, picked, "오래된 건만 집는다")
        assertEquals(listOf(old.key), queried, "젊은 건은 PG에 묻지도 않는다")
        val y = payments.find(young.id)
        assertEquals(PENDING, y.status)
        assertEquals(0, y.attemptCount)
        assertNull(y.nextRetryAt)
        assertEquals(emptyList<Any>(), history(young.id))
    }

    @Test
    fun stuck_pending__pg_status_unavailable__handed_to_unknown_flow() {
        val created = nowMicros()
        val p = insertPending(created)
        pg.charge(p.key, p.orderId, 10_000)
        pg.statusUnavailable = true // 조회 API 장애

        val at = created.plus(Duration.ofMinutes(3))
        janitor.sweepPending(at)

        // 판단 불가 = "결과를 모름" = UNKNOWN. 재시도/보류는 UNKNOWN
        // 흐름 하나가 맡는다. 이번 확인은 1회로 센다.
        val handed = payments.find(p.id)
        assertEquals(UNKNOWN, handed.status)
        assertEquals(1, handed.attemptCount)
        assertEquals(at.plusSeconds(10), handed.nextRetryAt, "백오프 1회차")

        pg.statusUnavailable = false
        janitor.reconcileUnknown(handed.nextRetryAt!!)
        assertEquals(APPROVED, payments.find(p.id).status)
        assertEquals(
            listOf("PENDING" to "UNKNOWN", "UNKNOWN" to "APPROVED"),
            history(p.id),
        )
    }

    @Test
    fun pending_race__janitor_settles_first__request_gets_no_error() {
        // 요청 스레드가 PG 응답을 받기 직전에 janitor가 같은 결제를
        // 확정한다 (pending-after보다 오래 걸린 PG 응답을, janitor에
        // 넘기는 now를 당겨서 재현).
        var picked = -1
        pg.onCharge = {
            picked = janitor.sweepPending(Instant.now().plus(Duration.ofMinutes(3)))
        }

        val result = paymentService.pay(newOrderId(), 10_000) // 던지면 실패

        assertEquals(1, picked, "janitor가 진행 중인 PENDING을 집었다")
        assertEquals(APPROVED, result.status)
        assertNotNull(result.pgTxId)
        val fromPending = historyWithReason(result.id)
            .filter { it.first == "PENDING" }
        assertEquals(1, fromPending.size, "PENDING에서 나간 전이는 하나: $fromPending")
        assertEquals("APPROVED", fromPending.single().second)
        assertEquals(true, fromPending.single().third.startsWith("janitor"))
        // 같은 결론(APPROVED)이면 요청은 아무 전이도 더 하지 않는다
        assertEquals(
            listOf(null to "REQUESTED", "REQUESTED" to "PENDING", "PENDING" to "APPROVED"),
            history(result.id),
        )
        assertEquals(1, pg.chargeCount())
        assertEquals(0, pg.cancelCount())
    }

    // ---- 요청이 늦게 돌아왔는데 janitor가 이미 다르게 정한 경우 ----
    // 요청이 PENDING을 커밋하고 PG 앞에서 멈춘다(beforeCharge). 그 사이
    // janitor가 PG에 물으면 NOT_FOUND다. 그 뒤 요청이 깨어나 PG에서 결과를
    // 받는다. PENDING → * CAS는 0행이지만, 요청이 아는 PG 결과를 버리면 안 된다.

    @Test
    fun late_pg_response__janitor_moved_to_unknown__request_approved__approved() {
        // +3m: pending-after(2m)는 지났고 grace(5m) 안 → NOT_FOUND → UNKNOWN
        pg.beforeCharge = { janitor.sweepPending(Instant.now().plus(Duration.ofMinutes(3))) }

        val result = paymentService.pay(newOrderId(), 10_000)

        // 요청은 PG 승인(pgTxId)을 안다 → UNKNOWN으로 두면 안 된다
        assertEquals(APPROVED, result.status)
        assertNotNull(result.pgTxId)
        assertNull(result.nextRetryAt, "UNKNOWN 재시도 예약은 지운다")
        assertEquals(
            listOf(
                null to "REQUESTED", "REQUESTED" to "PENDING",
                "PENDING" to "UNKNOWN", "UNKNOWN" to "APPROVED",
            ),
            history(result.id),
        )
        assertEquals(1, pg.chargeCount())
        assertEquals(0, pg.cancelCount())
    }

    @Test
    fun late_pg_response__janitor_moved_to_unknown__request_declined__failed() {
        pg.declineCharge = true
        pg.beforeCharge = { janitor.sweepPending(Instant.now().plus(Duration.ofMinutes(3))) }

        val result = paymentService.pay(newOrderId(), 10_000)

        // PG가 명확히 거절했다 → UNKNOWN이 아니라 FAILED
        assertEquals(FAILED, result.status)
        assertNull(result.nextRetryAt)
        assertEquals(
            listOf(
                null to "REQUESTED", "REQUESTED" to "PENDING",
                "PENDING" to "UNKNOWN", "UNKNOWN" to "FAILED",
            ),
            history(result.id),
        )
    }

    /**
     * 가장 위험한 경우: janitor는 grace가 지나도록 PG 기록이 없어 FAILED로
     * 확정했는데, 요청이 그 뒤 PG에 닿아 승인됐다(돈이 빠짐).
     * FAILED로 두면 고객은 재결제 → 이중 결제. 망취소(net-cancel)한다.
     */
    @Test
    fun late_pg_response__janitor_failed__request_approved__net_canceled() {
        pg.beforeCharge = { janitor.sweepPending(Instant.now().plus(Duration.ofHours(1))) }

        val result = paymentService.pay(newOrderId(), 10_000)

        assertEquals(CANCELED, result.status, "늦은 승인은 취소된다")
        assertNotNull(result.pgTxId)
        assertTrue(pg.isCanceled(result.pgTxId!!), "PG에서 실제로 취소")
        assertEquals(1, pg.cancelCount())
        assertEquals(
            listOf(
                null to "REQUESTED", "REQUESTED" to "PENDING", "PENDING" to "FAILED",
                "FAILED" to "CANCEL_REQUESTED", "CANCEL_REQUESTED" to "CANCELED",
            ),
            history(result.id),
        )
    }

    @Test
    fun late_pg_response__janitor_failed__net_cancel_fails__janitor_retries() {
        pg.failCancel = true // 망취소마저 실패
        pg.beforeCharge = { janitor.sweepPending(Instant.now().plus(Duration.ofHours(1))) }

        val result = paymentService.pay(newOrderId(), 10_000)

        // "돈은 빠졌는데 FAILED, 후속 조치 없음"이 되면 안 된다
        // → CANCEL_REQUESTED로 남아 janitor의 취소 재시도 대상이 된다
        assertEquals(CANCEL_REQUESTED, result.status)
        assertNotNull(result.pgTxId)
        assertEquals(1, result.attemptCount, "취소 1회 실패")
        assertNotNull(result.nextRetryAt, "재시도 예약")

        pg.failCancel = false
        janitor.retryCancelRequested(Instant.now().plus(Duration.ofHours(1)))
        assertEquals(CANCELED, payments.find(result.id).status)
        assertTrue(pg.isCanceled(result.pgTxId!!))
    }

    /** 요청도 결과를 모르면(타임아웃) janitor가 정한 상태가 더 많이 안다 */
    @Test
    fun late_pg_response__janitor_failed__request_timed_out__keeps_stored() {
        pg.timeoutBeforeCharge = true
        pg.beforeCharge = { janitor.sweepPending(Instant.now().plus(Duration.ofHours(1))) }

        val result = paymentService.pay(newOrderId(), 10_000)

        assertEquals(FAILED, result.status)
        assertEquals(
            listOf(null to "REQUESTED", "REQUESTED" to "PENDING", "PENDING" to "FAILED"),
            history(result.id),
        )
    }

    /** pending-after가 0 이하면 방금 PENDING이 된(PG 응답을 기다리는) 결제까지 집는다 → 설정 단계에서 거부 */
    @Test
    fun pending_after_zero_or_negative__rejected_at_construction() {
        for (bad in listOf(Duration.ZERO, Duration.ofSeconds(-1))) {
            assertThrows<IllegalArgumentException> {
                Janitor(
                    payments, stateMachine, pg,
                    maxAttempts = 5, baseBackoff = Duration.ofSeconds(10),
                    notFoundGrace = grace, lease = Duration.ofMinutes(1),
                    pendingAfter = bad,
                )
            }
        }
    }

    @Test
    fun pending_race__request_settles_first__janitor_skips_rest_continue() {
        val created = nowMicros()
        val raced = insertPending(created)
        val pgTxId = pg.charge(raced.key, raced.orderId, 10_000)
        val other = insertPending(created) // PG 기록 없음, grace 지남
        // janitor가 PG에 묻는 사이 요청 스레드의 PG 응답이 와서 먼저 확정
        pg.onGetStatus = { key ->
            if (key == raced.key) {
                stateMachine.transition(
                    raced.id, PENDING, APPROVED, "PG 승인", pgTxId = pgTxId,
                )
            }
        }

        val picked = janitor.sweepPending(created.plus(Duration.ofHours(1)))

        assertEquals(2, picked)
        assertEquals(APPROVED, payments.find(raced.id).status)
        assertEquals(
            listOf(Triple("PENDING", "APPROVED", "PG 승인")),
            historyWithReason(raced.id),
            "janitor의 CAS는 0행 → 덮어쓰지 않는다",
        )
        assertEquals(FAILED, payments.find(other.id).status, "배치는 계속")
    }

    @Test
    fun pending_crash_loop__claims_counted__ends_manual_review() {
        val created = nowMicros()
        val p = insertPending(created)
        var queried = false
        pg.onGetStatus = { queried = true }

        // claim만 하고 죽기를 max-attempts(5)번. lease(1m)가 지나면
        // 다시 집힌다 → claim에서 세지 않으면 영원히 반복된다.
        var now = created.plus(Duration.ofMinutes(3))
        repeat(5) {
            val claimed = payments.claimStalePending(
                now.minus(pendingAfter), now, now.plus(Duration.ofMinutes(1)), 10,
            )
            assertEquals(1, claimed.size)
            now = now.plus(Duration.ofMinutes(2))
        }

        // 6번째: PG를 부르지 않고 UNKNOWN 흐름으로 넘긴다
        janitor.sweepPending(now)
        assertEquals(UNKNOWN, payments.find(p.id).status)
        // UNKNOWN 쪽 crash-loop 보류 규칙이 MANUAL_REVIEW로 보낸다
        janitor.reconcileUnknown(now)

        assertEquals(MANUAL_REVIEW, payments.find(p.id).status)
        assertFalse(queried, "처리하다 계속 죽는 건은 PG도 부르지 않는다")
        assertEquals(
            listOf("PENDING" to "UNKNOWN", "UNKNOWN" to "MANUAL_REVIEW"),
            history(p.id),
        )
    }

    private data class Row(val id: UUID, val orderId: String, val key: String)

    /** PENDING까지 커밋하고 죽은 결제 (next_retry_at 없음) */
    private fun insertPending(createdAt: Instant): Row {
        val id = UUID.randomUUID()
        val row = Row(id, "o-$id", "idem-$id")
        jdbc.update(
            """
            INSERT INTO payments (id, order_id, amount, status,
                                  idempotency_key, created_at)
            VALUES (?, ?, 10000, 'PENDING', ?, ?)
            """.trimIndent(),
            id, row.orderId, row.key, Timestamp.from(createdAt),
        )
        return row
    }

    private fun history(id: UUID): List<Pair<String?, String>> =
        historyWithReason(id).map { it.first to it.second }

    private fun historyWithReason(id: UUID) =
        jdbc.query(
            """
            SELECT from_status, to_status, reason
              FROM payment_status_history
             WHERE payment_id = ? ORDER BY id
            """.trimIndent(),
            { rs, _ ->
                Triple(
                    rs.getString("from_status"),
                    rs.getString("to_status"),
                    rs.getString("reason"),
                )
            },
            id,
        )

    /** Postgres timestamptz는 마이크로초까지라 비교용으로 자른다 */
    private fun nowMicros() = Instant.now().truncatedTo(ChronoUnit.MICROS)

    private fun newOrderId() = "order-" + UUID.randomUUID()
}
