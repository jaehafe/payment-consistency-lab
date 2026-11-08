package lab.idempotency

/** POST /payments 요청 바디 */
data class PaymentRequest(val orderId: String, val amount: Long)

/** 201 응답 바디 */
data class PaymentResponse(
    val paymentId: String,
    val orderId: String,
    val amount: Long,
    val pgTxId: String,
    val status: String,
)

/** idempotency_keys에 저장했다가 그대로 재생(replay)하는 HTTP 응답 */
data class StoredResponse(val code: Int, val body: String)

/** 멱등키 규칙 위반/일시 장애를 HTTP 상태코드로 표현하는 예외 */
class IdempotencyException(val status: Int, message: String) : RuntimeException(message)
