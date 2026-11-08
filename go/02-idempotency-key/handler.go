package idempotency

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
)

// NewHandler 는 HTTP 라우팅을 만든다. 표준 라이브러리 net/http 만 사용한다.
// Go 1.22+ 부터 "POST /payments" 처럼 메서드+경로 패턴을 쓸 수 있다 (Spring의 @PostMapping("/payments") 역할).
func NewHandler(svc *PaymentService) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", func(w http.ResponseWriter, r *http.Request) { // 익명 함수 = Kotlin 람다
		createPayment(svc, w, r)
	})
	return mux
}

func createPayment(svc *PaymentService, w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	rawBody, err := io.ReadAll(r.Body) // 바디 원문. SHA-256 해시를 이 바이트로 계산한다.
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read body")
		return
	}
	var req PaymentRequest
	if err := json.Unmarshal(rawBody, &req); err != nil || req.OrderID == "" || req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "body must be {\"orderId\": string, \"amount\": positive number}")
		return
	}

	// r.Context(): 클라이언트 연결이 끊기면 취소되는 ctx. 모든 하위 호출(DB, PG)에 그대로 넘긴다.
	res, err := svc.Pay(r.Context(), key, r.URL.Path, rawBody, req)

	// switch { case 조건: ... } 는 if-else if 체인을 깔끔하게 쓰는 방법 (Kotlin의 인자 없는 when).
	switch {
	case errors.Is(err, ErrKeyReusedWithDifferentBody):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrRequestInProgress):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrPGTimeout), errors.Is(err, ErrPGFailed):
		// 재시도 가능한 실패. 잠금은 이미 풀려 있으므로 같은 key 로 다시 보내면 된다.
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case err != nil:
		log.Printf("payment failed: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(res.Body)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// map[string]string{...} : Kotlin의 mapOf("error" to msg)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
