package observability

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var ErrPgDeclined = errors.New("pg: declined (simulated)")

// FakePaymentGateway 는 장애 시나리오(지연 급증, 타임아웃 급증)를 주입할 수 있는 Fake PG.
type FakePaymentGateway struct {
	latencyNanos atomic.Int64 // 응답 지연. time.Duration 은 int64 라서 atomic.Int64 에 담는다.
	Hang         atomic.Bool  // 응답을 아예 안 줌 → 호출자의 타임아웃까지 대기 (타임아웃 급증 시나리오)
	Decline      atomic.Bool  // 명확한 거절

	mu      sync.Mutex
	byKey   map[string]string
	charges int
}

func NewFakePaymentGateway() *FakePaymentGateway {
	return &FakePaymentGateway{byKey: map[string]string{}}
}

func (g *FakePaymentGateway) SetLatency(d time.Duration) { g.latencyNanos.Store(int64(d)) }
func (g *FakePaymentGateway) Latency() time.Duration     { return time.Duration(g.latencyNanos.Load()) }

// Charge 는 승인한다. 승인 자체는 먼저 일어나고, 응답이 늦거나 안 오는 상황을 흉내 낸다
// → 호출자가 타임아웃을 봐도 PG 에서는 승인됐을 수 있다 (그래서 UNKNOWN).
func (g *FakePaymentGateway) Charge(ctx context.Context, key, orderID string, amount int64) (string, error) {
	if g.Decline.Load() {
		return "", ErrPgDeclined
	}
	g.mu.Lock()
	txID, ok := g.byKey[key]
	if !ok {
		txID = "pg_" + uuid.NewString()
		g.byKey[key] = txID
		g.charges++
	}
	g.mu.Unlock()

	wait := g.Latency()
	if g.Hang.Load() {
		wait = time.Hour
	}
	select {
	case <-time.After(wait):
		return txID, nil
	case <-ctx.Done(): // 호출자 타임아웃 → context.DeadlineExceeded
		return "", ctx.Err()
	}
}

func (g *FakePaymentGateway) ChargeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.charges
}

// FaultsHandler 는 실행 중인 서버에 장애를 주입하는 관리용 엔드포인트 (런북 실습용, cmd/main.go 에서만 연결).
//
//	curl -X POST 'localhost:8091/admin/faults?latency=4s&hang=false&decline=false'
func FaultsHandler(g *FakePaymentGateway) http.Handler {
	// http.HandlerFunc(f): 함수 f 를 http.Handler 인터페이스로 바꿔주는 어댑터 (함수형 인터페이스 SAM 변환 같은 것)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if v := q.Get("latency"); v != "" {
			d, err := time.ParseDuration(v) // "250ms", "4s" 같은 문자열을 Duration 으로
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			g.SetLatency(d)
		}
		if v := q.Get("hang"); v != "" {
			b, _ := strconv.ParseBool(v)
			g.Hang.Store(b)
		}
		if v := q.Get("decline"); v != "" {
			b, _ := strconv.ParseBool(v)
			g.Decline.Store(b)
		}
		_, _ = w.Write([]byte("latency=" + g.Latency().String() +
			" hang=" + strconv.FormatBool(g.Hang.Load()) + " decline=" + strconv.FormatBool(g.Decline.Load()) + "\n"))
	})
}
