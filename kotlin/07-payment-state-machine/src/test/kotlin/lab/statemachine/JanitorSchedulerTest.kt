package lab.statemachine

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 스케줄링 자체를 검증한다: 테스트는 janitor를 직접 부르지 않는다.
 * 앱이 스스로 janitor를 주기 실행해서 UNKNOWN을 확정하는지만 본다.
 *
 * - 다른 테스트(PaymentStateMachineTest)는 시각을 직접 넘기므로
 *   스케줄러를 끈다(lab.janitor.enabled=false).
 * - 여기선 켜고 주기를 짧게(200ms) 준다.
 * - @DirtiesContext: 끝나면 컨텍스트(=스케줄러)를 닫아
 *   다른 테스트 클래스에 스케줄러가 남아 돌지 않게 한다.
 */
@SpringBootTest(
    properties = [
        "lab.janitor.enabled=true",
        "lab.janitor.interval=200ms",
    ],
)
@Import(TestcontainersConfiguration::class)
@DirtiesContext
class JanitorSchedulerTest {
    @Autowired lateinit var pg: FakePaymentGateway
    @Autowired lateinit var jdbc: JdbcTemplate

    @Test
    fun scheduler_reconciles_due_unknown_without_direct_call() {
        val id = UUID.randomUUID()
        val key = "idem-$id"
        // PG에는 승인 기록이 있다 (= 승인 후 응답 유실 상황)
        val pgTxId = pg.charge(key, "o-$id", 10_000)
        // DB에는 UNKNOWN, 재확인 시각은 이미 지남
        jdbc.update(
            """
            INSERT INTO payments (id, order_id, amount, status,
                                  idempotency_key, next_retry_at)
            VALUES (?, ?, 10000, 'UNKNOWN', ?, ?)
            """.trimIndent(),
            id, "o-$id", key,
            Timestamp.from(Instant.now().minusSeconds(60)),
        )

        // 고정 sleep 없이: 조건이 맞을 때까지 짧게 폴링, 상한 10초
        awaitUntil(Duration.ofSeconds(10)) { status(id) == "APPROVED" }

        assertEquals(
            pgTxId,
            jdbc.queryForObject(
                "SELECT pg_tx_id FROM payments WHERE id = ?",
                String::class.java, id,
            ),
        )
        assertEquals(
            listOf("UNKNOWN->APPROVED"),
            jdbc.queryForList(
                """
                SELECT from_status || '->' || to_status
                  FROM payment_status_history
                 WHERE payment_id = ? ORDER BY id
                """.trimIndent(),
                String::class.java, id,
            ),
        )
    }

    @Test
    fun scheduler_settles_stale_pending_without_direct_call() {
        val id = UUID.randomUUID()
        val key = "idem-$id"
        // PENDING 커밋 → PG 승인 → 응답 기록 전에 서버가 죽었다.
        // 3분 전 생성 (pending-after 2m 지남), next_retry_at 없음.
        val pgTxId = pg.charge(key, "o-$id", 10_000)
        jdbc.update(
            """
            INSERT INTO payments (id, order_id, amount, status,
                                  idempotency_key, created_at)
            VALUES (?, ?, 10000, 'PENDING', ?, ?)
            """.trimIndent(),
            id, "o-$id", key,
            Timestamp.from(Instant.now().minusSeconds(180)),
        )

        awaitUntil(Duration.ofSeconds(10)) { status(id) == "APPROVED" }

        assertEquals(
            pgTxId,
            jdbc.queryForObject(
                "SELECT pg_tx_id FROM payments WHERE id = ?",
                String::class.java, id,
            ),
        )
    }

    private fun status(id: UUID): String? =
        jdbc.queryForObject(
            "SELECT status FROM payments WHERE id = ?",
            String::class.java, id,
        )

    private fun awaitUntil(timeout: Duration, cond: () -> Boolean) {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            if (cond()) return
            Thread.sleep(50) // 폴링 간격일 뿐, 결과를 기다리는 고정 대기가 아니다
        }
        fail<Unit>("$timeout 안에 조건이 만족되지 않음")
    }
}
