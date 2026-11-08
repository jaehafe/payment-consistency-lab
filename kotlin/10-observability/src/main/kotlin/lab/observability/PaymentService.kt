package lab.observability

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.UUID

data class PaymentView(val paymentId: String, val orderId: String, val amount: Long, val status: String, val pgTxId: String?)

data class PayOutcome(val httpStatus: Int, val payment: PaymentView, val result: PaymentMetrics.Result)

/**
 * 07의 결제 흐름 간소화판 (PG 호출 + UNKNOWN 처리 + 멱등키 재요청) + 관측성.
 * 로그 한 줄 한 줄에 request_id/payment_id/order_id/idempotency_key가 붙도록 MDC를 채운다.
 */
@Service
class PaymentService(
    private val jdbc: JdbcTemplate,
    private val pg: PaymentGateway,
    private val metrics: PaymentMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun pay(idempotencyKey: String, orderId: String, amount: Long): PayOutcome {
        MDC.put(Mdc.IDEMPOTENCY_KEY, idempotencyKey)
        MDC.put(Mdc.ORDER_ID, orderId)
        log.info("결제 요청 수신 amount={}", amount)

        val id = UUID.randomUUID()
        val inserted = jdbc.update(
            """
            INSERT INTO payments (id, order_id, amount, status, idempotency_key) VALUES (?, ?, ?, 'REQUESTED', ?)
            ON CONFLICT (idempotency_key) DO NOTHING
            """.trimIndent(),
            id, orderId, amount, idempotencyKey,
        )
        if (inserted == 0) return replayOrConflict(idempotencyKey)

        MDC.put(Mdc.PAYMENT_ID, id.toString())
        updateStatus(id, "PENDING", null)

        log.info("PG 승인 요청")
        val started = System.nanoTime()
        try {
            val pgTxId = pg.charge(idempotencyKey, orderId, amount)
            recordPg("success", started)
            updateStatus(id, "APPROVED", pgTxId)
            log.info("결제 승인 pg_tx_id={}", pgTxId)
            return outcome(201, id, PaymentMetrics.Result.APPROVED)
        } catch (e: PgDeclinedException) {
            recordPg("declined", started)
            updateStatus(id, "FAILED", null)
            log.warn("결제 실패(PG 거절): {}", e.message)
            return outcome(402, id, PaymentMetrics.Result.FAILED)
        } catch (e: PgException) {
            recordPg("timeout", started)
            // 실패로 단정하지 않는다 (07 참고). payment_unknown_current 게이지가 이 건만큼 올라간다.
            updateStatus(id, "UNKNOWN", null)
            log.warn("PG 응답 불명 → UNKNOWN (janitor 확인 필요): {}", e.message)
            return outcome(202, id, PaymentMetrics.Result.UNKNOWN)
        }
    }

    private fun replayOrConflict(idempotencyKey: String): PayOutcome {
        val existing = find("idempotency_key = ?", idempotencyKey)
        MDC.put(Mdc.PAYMENT_ID, existing.paymentId)
        if (existing.status == "REQUESTED" || existing.status == "PENDING") {
            log.warn("같은 멱등키 요청이 처리 중 → 409")
            metrics.request(PaymentMetrics.Result.CONFLICT)
            return PayOutcome(409, existing, PaymentMetrics.Result.CONFLICT)
        }
        // 재요청이 많다 = 클라이언트가 응답을 못 받고 있다(타임아웃이 너무 짧거나, 우리 응답이 느림). DuplicateRequestStorm 알림의 근거.
        log.info("멱등 재요청 → 저장된 결과 반환 status={}", existing.status)
        metrics.replayed()
        metrics.request(PaymentMetrics.Result.REPLAYED)
        val code = when (existing.status) { "APPROVED" -> 201; "FAILED" -> 402; else -> 202 }
        return PayOutcome(code, existing, PaymentMetrics.Result.REPLAYED)
    }

    private fun outcome(code: Int, id: UUID, result: PaymentMetrics.Result): PayOutcome {
        metrics.request(result)
        return PayOutcome(code, find("id = ?", id), result)
    }

    private fun recordPg(outcome: String, startedNanos: Long) {
        val elapsed = Duration.ofNanos(System.nanoTime() - startedNanos)
        metrics.recordPgCall(outcome, elapsed)
        log.info("PG 응답 outcome={} duration_ms={}", outcome, elapsed.toMillis())
    }

    private fun updateStatus(id: UUID, status: String, pgTxId: String?) {
        jdbc.update(
            "UPDATE payments SET status = ?, pg_tx_id = coalesce(?, pg_tx_id), updated_at = now() WHERE id = ?",
            status, pgTxId, id,
        )
    }

    private fun find(where: String, arg: Any): PaymentView = jdbc.queryForObject(
        "SELECT id, order_id, amount, status, pg_tx_id FROM payments WHERE $where",
        { rs, _ -> PaymentView(rs.getString("id"), rs.getString("order_id"), rs.getLong("amount"), rs.getString("status"), rs.getString("pg_tx_id")) },
        arg,
    )!!
}
