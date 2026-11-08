package lab.idempotency

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RestController

@RestController
class PaymentController(
    private val service: IdempotentPaymentService,
    private val objectMapper: ObjectMapper,
) {

    @PostMapping("/payments")
    fun pay(
        @RequestHeader("Idempotency-Key", required = false) idempotencyKey: String?,
        @RequestBody request: PaymentRequest,
    ): ResponseEntity<String> {
        // 결제처럼 "두 번 실행되면 안 되는" API는 멱등키를 필수로 받는다.
        if (idempotencyKey.isNullOrBlank()) return error(400, "Idempotency-Key 헤더가 필요합니다")

        val response = service.pay(idempotencyKey, "/payments", request)
        return ResponseEntity.status(response.code).contentType(MediaType.APPLICATION_JSON).body(response.body)
    }

    @ExceptionHandler(IdempotencyException::class)
    fun handle(e: IdempotencyException): ResponseEntity<String> = error(e.status, e.message ?: "")

    private fun error(status: Int, message: String): ResponseEntity<String> =
        ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON)
            .body(objectMapper.writeValueAsString(mapOf("error" to message)))
}
