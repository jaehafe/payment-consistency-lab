package observability

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewServer 는 라우팅 + 미들웨어를 조립한 http.Handler 를 돌려준다.
//
//	POST /payments   결제 (헤더 Idempotency-Key 필수, X-Request-Id 선택)
//	GET  /metrics    Prometheus 스크레이프 엔드포인트
func NewServer(svc *PaymentService, logger *slog.Logger) http.Handler {
	return RequestLogging(logger, Routes(svc))
}

// Routes 는 미들웨어 없이 라우팅만 한 mux 를 돌려준다.
// cmd/main.go 처럼 다른 경로(/admin/faults)를 더 붙인 뒤
// 전체를 RequestLogging 으로 한 번 감쌀 때 쓴다.
func Routes(svc *PaymentService) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments",
		func(w http.ResponseWriter, r *http.Request) {
			handlePay(svc, w, r)
		})
	// 전용 레지스트리의 메트릭만 노출한다
	// (promhttp.Handler() 는 전역 레지스트리용).
	mux.Handle("GET /metrics", promhttp.HandlerFor(
		svc.Metrics.Registry, promhttp.HandlerOpts{}))
	return mux
}

// safeRequestID 는 받아들일 X-Request-Id 모양 (Kotlin RequestIdFilter 와 같은 규칙).
// 헤더 값을 그대로 로그 필드에 넣으므로, 따옴표/공백/개행이 든 값이나
// 아주 긴 값은 버리고 새로 만든다 (로그 인젝션, 로그 폭탄 방지).
var safeRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// RequestLogging 은 미들웨어다: 요청 앞뒤로 공통 처리를 끼워 넣는다 (Spring의 OncePerRequestFilter / HandlerInterceptor).
//
// Go 미들웨어의 모양은 "http.Handler 를 받아 http.Handler 를 돌려주는 함수"다.
// 여기서는 (1) X-Request-Id 를 받거나 만들고 (2) ctx 로그 필드에 넣고 (3) 응답 헤더로 돌려주고 (4) access log 를 남긴다.
func RequestLogging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-Id")
		// 안 줬거나 모양이 이상하면 여기서 만든다
		if !safeRequestID.MatchString(requestID) {
			requestID = uuid.NewString()
		}
		// 이 ctx 를 r.WithContext 로 요청에 실어 보내면, 핸들러 → 서비스 → 로그까지 request_id 가 따라간다.
		ctx := AddLogAttrs(r.Context(), slog.String("request_id", requestID))
		w.Header().Set("X-Request-Id", requestID) // 클라이언트가 문의할 때 이 값을 알려주면 바로 grep 가능

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(ctx))

		// access log: 핸들러가 ctx 에 추가한 payment_id 등도 같은 묶음에 있으므로 여기에도 찍힌다.
		logger.InfoContext(ctx, "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

// statusRecorder 는 응답 상태 코드를 기록하려고 ResponseWriter 를 감싼다.
// http.ResponseWriter 를 임베딩해서 Write/Header 는 그대로 위임하고, WriteHeader 만 가로챈다.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

type payRequest struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

func handlePay(svc *PaymentService, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := r.Header.Get("Idempotency-Key")
	var req payRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || key == "" || req.OrderID == "" || req.Amount <= 0 {
		svc.Log.WarnContext(ctx, "bad request", "has_idempotency_key", key != "")
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header and {orderId, amount>0} are required"})
		return
	}

	p, result, err := svc.Pay(ctx, key, req.OrderID, req.Amount)
	if err != nil {
		svc.Log.ErrorContext(ctx, "payment failed with system error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	if result == ResultConflict {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "a request with this idempotency key is in progress"})
		return
	}
	writeJSON(w, statusCodeFor(p.Status), p)
}

// statusCodeFor: 승인 201, 거절 402, 결과 모름 202(접수됨 — 나중에 확정)
func statusCodeFor(status string) int {
	switch status {
	case StatusApproved:
		return http.StatusCreated
	case StatusFailed:
		return http.StatusPaymentRequired
	default:
		return http.StatusAccepted
	}
}

// writeJSON 의 v any: 아무 타입이나 받는다 (Kotlin의 Any). any 는 interface{} 의 별칭.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
