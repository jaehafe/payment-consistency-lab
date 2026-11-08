package observability

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// 결과 라벨 값. infra/observability/alerts.yml 의 규칙이 이 값들을 쓴다.
const (
	ResultApproved = "approved"
	ResultFailed   = "failed"
	ResultUnknown  = "unknown"
	ResultReplayed = "replayed"
	ResultConflict = "conflict"
)

// Metrics 는 이 서비스의 Prometheus 메트릭 묶음.
//
// 전역 기본 레지스트리(prometheus.DefaultRegisterer) 대신 전용 *prometheus.Registry 를 쓴다:
//   - 테스트마다 새 레지스트리 → 카운터가 테스트끼리 섞이지 않는다
//   - 같은 메트릭을 두 번 등록하면 패닉하는데, 전역을 쓰면 테스트에서 NewMetrics 를 두 번 부를 수 없다
//
// Kotlin(Micrometer)의 MeterRegistry 를 직접 만들어 주입하는 것과 같다.
type Metrics struct {
	Registry *prometheus.Registry

	// payment_requests_total{result=...}: 요청 결과별 카운터. 실패율 알림(PaymentFailureRateHigh)의 재료.
	PaymentRequests *prometheus.CounterVec
	// pg_call_duration_seconds: PG 호출 시간 히스토그램. p99 알림(PgLatencyHigh)의 재료.
	PgCallDuration prometheus.Histogram
	// payment_unknown_current: 지금 UNKNOWN 상태인 결제 수 (PaymentUnknownPiling).
	UnknownCurrent prometheus.GaugeFunc
	// idempotency_replays_total: 같은 멱등키 재요청 수 (DuplicateRequestStorm).
	IdempotencyReplays prometheus.Counter
}

// NewMetrics 는 메트릭을 만들고 전용 레지스트리에 등록한다.
func NewMetrics(db *pgxpool.Pool, logger *slog.Logger) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		PaymentRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "payment_requests_total",
			Help: "결제 요청 수 (결과별)",
		}, []string{"result"}),
		PgCallDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "pg_call_duration_seconds",
			Help: "PG 승인 호출 소요 시간",
			// 버킷 경계는 알림 임계값을 포함하도록 고른다. PgLatencyHigh 가 p99 > 3s 이므로 3 이 경계에 있어야 정확하다.
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10},
		}),
		IdempotencyReplays: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "idempotency_replays_total",
			Help: "멱등키 재요청으로 저장된 응답을 돌려준 횟수",
		}),
	}

	// UNKNOWN 수는 "카운터 +1/-1" 로 관리하지 않고 스크레이프 때마다 DB 를 센다 (GaugeFunc).
	// 이유: 프로세스가 재시작하면 메모리 값은 0 으로 초기화되고, 07 의 janitor 같은 다른 프로세스가 상태를 바꾸면
	// 메모리 게이지는 그걸 모른다. 진실의 원천(DB)에서 직접 읽는 것이 가장 정확하다.
	m.UnknownCurrent = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "payment_unknown_current",
		Help: "현재 UNKNOWN 상태인 결제 수",
	}, func() float64 { // 스크레이프마다 호출되는 함수 (Micrometer 의 Gauge.builder("...", obj) { ... })
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var n int64
		if err := db.QueryRow(ctx, `SELECT count(*) FROM payments WHERE status = 'UNKNOWN'`).Scan(&n); err != nil {
			logger.Error("payment_unknown_current query failed", "error", err)
			return math.NaN() // 값을 모를 땐 0 이 아니라 NaN: "0건이라 괜찮다"는 착각을 막는다
		}
		return float64(n)
	})

	// 모든 result 라벨을 0 으로 미리 만들어 둔다.
	// 한 번도 증가하지 않은 라벨은 /metrics 에 아예 안 나오고, 그러면 rate() 비율 계산이나 알림이 "데이터 없음"이 된다.
	for _, r := range []string{ResultApproved, ResultFailed, ResultUnknown, ResultReplayed, ResultConflict} {
		m.PaymentRequests.WithLabelValues(r)
	}

	// MustRegister: 등록 실패(이름 중복 등)면 패닉. 프로그램 시작 시 설정 버그를 바로 드러낸다.
	reg.MustRegister(m.PaymentRequests, m.PgCallDuration, m.UnknownCurrent, m.IdempotencyReplays,
		collectors.NewGoCollector(), // go_goroutines, GC 등 런타임 메트릭 (JVM 메트릭에 해당)
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
