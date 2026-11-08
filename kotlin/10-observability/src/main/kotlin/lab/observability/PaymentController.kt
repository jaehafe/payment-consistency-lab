package lab.observability

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RestController

data class PaymentRequest(val orderId: String, val amount: Long)

data class PgFaults(val latencyMs: Long? = null, val timeoutAfterCharge: Boolean? = null, val declineCharge: Boolean? = null)

@RestController
class PaymentController(
    private val service: PaymentService,
    private val pg: FakePaymentGateway,
) {
    @PostMapping("/payments")
    fun pay(
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
        @RequestBody request: PaymentRequest,
    ): ResponseEntity<Any> {
        if (idempotencyKey.isNullOrBlank()) return ResponseEntity.badRequest().body(mapOf("error" to "Idempotency-Key 헤더가 필요합니다"))
        val outcome = service.pay(idempotencyKey, request.orderId, request.amount)
        return ResponseEntity.status(outcome.httpStatus).body(outcome.payment)
    }

    /** 장애 시나리오 주입용 (실습 전용 — 실서비스에 이런 엔드포인트를 열어 두면 안 된다) */
    @PostMapping("/admin/pg-faults")
    fun faults(@RequestBody f: PgFaults): Map<String, Any> {
        f.latencyMs?.let { pg.latencyMs = it }
        f.timeoutAfterCharge?.let { pg.timeoutAfterCharge = it }
        f.declineCharge?.let { pg.declineCharge = it }
        return mapOf("latencyMs" to pg.latencyMs, "timeoutAfterCharge" to pg.timeoutAfterCharge, "declineCharge" to pg.declineCharge)
    }
}
