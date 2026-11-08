package observability

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// syncBuffer 는 여러 goroutine 이 동시에 써도 안전한 로그 캡처 버퍼.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines 는 JSON 로그를 한 줄씩 map 으로 파싱한다.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(b.buf.Bytes()))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("로그가 JSON 이 아니다: %s", sc.Text())
		}
		out = append(out, m)
	}
	return out
}

type app struct {
	logs    *syncBuffer
	gateway *FakePaymentGateway
	metrics *Metrics
	handler http.Handler
}

func newApp(t *testing.T) *app {
	t.Helper()
	exec(t, `TRUNCATE payments`)
	logs := &syncBuffer{}
	logger := NewLogger(logs)
	gw := NewFakePaymentGateway()
	m := NewMetrics(testPool, logger) // 테스트마다 새 레지스트리 → 메트릭 값이 다른 테스트와 섞이지 않는다
	svc := &PaymentService{DB: testPool, Gateway: gw, Metrics: m, Log: logger, PgTimeout: 300 * time.Millisecond}
	return &app{logs: logs, gateway: gw, metrics: m, handler: NewServer(svc, logger)}
}

// do 는 httptest.NewRecorder 로 핸들러를 직접 호출한다 (네트워크 없이, 동기적으로).
func (a *app) do(t *testing.T, method, path string, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	return rec
}

func (a *app) pay(t *testing.T, key, orderID string) *httptest.ResponseRecorder {
	t.Helper()
	return a.do(t, http.MethodPost, "/payments", map[string]string{"Idempotency-Key": key},
		`{"orderId":"`+orderID+`","amount":10000}`)
}

func Test_request_id_propagated_to_all_logs(t *testing.T) {
	a := newApp(t)
	rec := a.do(t, http.MethodPost, "/payments",
		map[string]string{"Idempotency-Key": "key-1", "X-Request-Id": "req-abc-123"},
		`{"orderId":"order-1","amount":10000}`)
	if rec.Code != http.StatusCreated || rec.Header().Get("X-Request-Id") != "req-abc-123" {
		t.Fatalf("201 + X-Request-Id 응답 헤더 기대: %d %q", rec.Code, rec.Header().Get("X-Request-Id"))
	}

	lines := a.logs.lines(t)
	if len(lines) < 4 { // payment requested, pg call finished, payment completed, http request
		t.Fatalf("로그 4줄 이상 기대: %d", len(lines))
	}
	var paymentID string
	for _, l := range lines {
		t.Logf("%v", l)
		// 모든 줄에 같은 request_id, 그리고 결제 컨텍스트 필드
		if l["request_id"] != "req-abc-123" {
			t.Fatalf("request_id 누락/불일치: %v", l)
		}
		if l["idempotency_key"] != "key-1" || l["order_id"] != "order-1" {
			t.Fatalf("idempotency_key / order_id 누락: %v", l)
		}
		if l["msg"] != "payment requested" { // 결제 행이 생긴 뒤의 로그에는 payment_id 도
			id, _ := l["payment_id"].(string) // 타입 단언: any → string. 실패하면 "" (두 번째 값을 _ 로 버림)
			if id == "" || (paymentID != "" && id != paymentID) {
				t.Fatalf("payment_id 누락/불일치: %v", l)
			}
			paymentID = id
		}
	}

	// X-Request-Id 없이 오면 서버가 생성해서 응답 헤더와 로그에 쓴다
	rec2 := a.pay(t, "key-2", "order-2")
	generated := rec2.Header().Get("X-Request-Id")
	if generated == "" || generated == "req-abc-123" {
		t.Fatalf("새 request_id 가 생성돼야 한다: %q", generated)
	}
	found := 0
	for _, l := range a.logs.lines(t) {
		if l["request_id"] == generated {
			found++
		}
	}
	if found < 4 {
		t.Fatalf("생성된 request_id 가 그 요청의 모든 로그에: %d줄", found)
	}
}

func Test_pg_timeout__unknown_counter_and_gauge_increase(t *testing.T) {
	a := newApp(t)
	a.gateway.Hang.Store(true) // PG 가 응답하지 않음 → 300ms 타임아웃

	for i, key := range []string{"k1", "k2", "k3"} {
		rec := a.pay(t, key, "order-"+key)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("%d번째: UNKNOWN 은 202 기대: %d", i, rec.Code)
		}
	}
	if v := testutil.ToFloat64(a.metrics.PaymentRequests.WithLabelValues(ResultUnknown)); v != 3 {
		t.Fatalf(`payment_requests_total{result="unknown"} = 3 기대: %v`, v)
	}
	if v := testutil.ToFloat64(a.metrics.UnknownCurrent); v != 3 {
		t.Fatalf("payment_unknown_current = 3 기대: %v", v)
	}

	// 다른 프로세스(07 의 janitor)가 확정하면 게이지는 DB 를 다시 세므로 바로 줄어든다
	exec(t, `UPDATE payments SET status = 'APPROVED' WHERE idempotency_key = 'k1'`)
	if v := testutil.ToFloat64(a.metrics.UnknownCurrent); v != 2 {
		t.Fatalf("DB 기준 게이지 2 기대: %v", v)
	}
}

func Test_duplicate_requests__replay_metric_increments(t *testing.T) {
	a := newApp(t)
	var bodies []string
	for range 3 { // Go 1.22+: 정수 range. 3번 반복
		rec := a.pay(t, "same-key", "order-1")
		if rec.Code != http.StatusCreated {
			t.Fatalf("201 기대: %d", rec.Code)
		}
		bodies = append(bodies, rec.Body.String())
	}
	if bodies[0] != bodies[1] || bodies[1] != bodies[2] {
		t.Fatalf("replay 응답은 같아야 한다: %v", bodies)
	}
	if v := testutil.ToFloat64(a.metrics.IdempotencyReplays); v != 2 {
		t.Fatalf("idempotency_replays_total = 2 기대: %v", v)
	}
	if v := testutil.ToFloat64(a.metrics.PaymentRequests.WithLabelValues(ResultReplayed)); v != 2 {
		t.Fatalf(`result="replayed" = 2 기대: %v`, v)
	}
	if v := testutil.ToFloat64(a.metrics.PaymentRequests.WithLabelValues(ResultApproved)); v != 1 {
		t.Fatalf(`result="approved" = 1 기대: %v`, v)
	}
	if a.gateway.ChargeCount() != 1 {
		t.Fatal("PG 호출은 1번")
	}
}

func Test_pg_latency__histogram_records_duration(t *testing.T) {
	a := newApp(t)
	a.gateway.SetLatency(150 * time.Millisecond) // PG 지연 급증 시나리오 (축소판)

	if rec := a.pay(t, "k1", "order-1"); rec.Code != http.StatusCreated {
		t.Fatalf("201 기대: %d", rec.Code)
	}

	// 레지스트리에서 히스토그램을 직접 꺼내 본다. dto 는 Prometheus 내부 데이터 모델 (protobuf 구조체).
	mfs, err := a.metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var h *dto.Histogram
	for _, mf := range mfs {
		if mf.GetName() == "pg_call_duration_seconds" {
			h = mf.GetMetric()[0].GetHistogram()
		}
	}
	if h == nil {
		t.Fatal("pg_call_duration_seconds 가 없다")
	}
	if h.GetSampleCount() != 1 || h.GetSampleSum() < 0.15 {
		t.Fatalf("샘플 1개, 합계 >= 0.15s 기대: count=%d sum=%f", h.GetSampleCount(), h.GetSampleSum())
	}
	// 누적 버킷: le=0.1 에는 0개, le=0.25 에는 1개 → 150ms 가 (0.1, 0.25] 구간에 들어감
	for _, b := range h.GetBucket() {
		switch b.GetUpperBound() {
		case 0.1:
			if b.GetCumulativeCount() != 0 {
				t.Fatalf("le=0.1 버킷은 0: %d", b.GetCumulativeCount())
			}
		case 0.25:
			if b.GetCumulativeCount() != 1 {
				t.Fatalf("le=0.25 버킷은 1: %d", b.GetCumulativeCount())
			}
		}
	}
}

func Test_metrics_endpoint_exposes_payment_metrics(t *testing.T) {
	a := newApp(t)
	a.pay(t, "k1", "order-1")

	rec := a.do(t, http.MethodGet, "/metrics", nil, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("200 기대: %d", rec.Code)
	}
	body := rec.Body.String()
	// infra/observability/alerts.yml 이 쓰는 이름들이 그대로 노출돼야 한다
	for _, want := range []string{
		`payment_requests_total{result="approved"} 1`,
		`payment_requests_total{result="unknown"} 0`, // 미리 0 으로 초기화된 라벨
		`pg_call_duration_seconds_bucket{le="3"}`,
		`pg_call_duration_seconds_count 1`,
		`payment_unknown_current 0`,
		`idempotency_replays_total 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics 에 %q 가 없다", want)
		}
	}
}
