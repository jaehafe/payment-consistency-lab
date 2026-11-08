package idempotentconsumer

import "time"

// TopicPaymentEvents 는 결제 이벤트 토픽 이름 (docs/SPEC.md 공통).
const TopicPaymentEvents = "payment.events"

// PaymentCompleted 이벤트 payload. 03/04 의 프로듀서가 보내는 것과 같은 형식.
// `json:"..."` 는 JSON 필드 이름을 정하는 struct tag (Jackson의 @JsonProperty).
type PaymentCompleted struct {
	PaymentID  string    `json:"paymentId"`
	OrderID    string    `json:"orderId"`
	Amount     int64     `json:"amount"`
	PgTxID     string    `json:"pgTxId"`
	OccurredAt time.Time `json:"occurredAt"`
}
