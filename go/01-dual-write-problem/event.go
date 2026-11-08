package dualwrite

import "time"

// TopicPaymentEvents 는 결제 이벤트 토픽 이름 (docs/SPEC.md 공통).
const TopicPaymentEvents = "payment.events"

// PaymentCompleted 이벤트 payload.
//
// 필드 뒤의 `json:"paymentId"` 는 "struct tag". JSON 직렬화 시 필드 이름을 지정한다.
// Kotlin(Jackson)의 @JsonProperty("paymentId") 와 같은 역할.
// Go에서 대문자로 시작하는 필드/함수만 패키지 밖에서 보인다 (public). 소문자는 private.
type PaymentCompleted struct {
	PaymentID  string    `json:"paymentId"`
	OrderID    string    `json:"orderId"`
	Amount     int64     `json:"amount"`
	PgTxID     string    `json:"pgTxId"`
	OccurredAt time.Time `json:"occurredAt"` // time.Time 은 JSON 으로 RFC3339(ISO-8601) 문자열이 된다.
}

// Payment 는 payments 테이블의 한 행.
type Payment struct {
	ID      string
	OrderID string
	Amount  int64
	Status  string
	PgTxID  string
}
