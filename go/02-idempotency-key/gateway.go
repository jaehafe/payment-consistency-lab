package idempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// 실패 주입용 에러들.
// var ( ... ) 는 변수 여러 개를 한 번에 선언하는 문법.
var (
	ErrPGFailed  = errors.New("pg: charge rejected (simulated)")
	ErrPGTimeout = errors.New("pg: timeout (simulated) - 승인됐는지 알 수 없음")
)

// FakePaymentGateway 는 외부 PG 를 흉내 낸다.
// 실제 PG(토스페이먼츠, Stripe)처럼 PG 자체도 idempotency key 를 지원한다:
// 같은 key 로 다시 오면 새로 승인하지 않고 이전 pgTxId 를 돌려준다.
type FakePaymentGateway struct {
	// 실패 주입 스위치. atomic.Bool 은 여러 goroutine 에서 동시에 읽고 써도 안전하다 (Kotlin AtomicBoolean).
	FailBeforeCharge   atomic.Bool // 승인 전에 에러 → 돈은 안 빠짐
	TimeoutAfterCharge atomic.Bool // 승인은 됐는데 응답이 유실 → 호출자는 타임아웃만 봄
	ChargeDelay        time.Duration

	mu      sync.Mutex // byKey, charged 를 보호하는 잠금
	byKey   map[string]string
	charged int
}

func NewFakePaymentGateway() *FakePaymentGateway {
	return &FakePaymentGateway{byKey: map[string]string{}}
}

// Charge 는 결제를 승인한다. 반환값은 (pgTxId, error) 두 개.
// (g *FakePaymentGateway) 는 리시버: 이 함수가 FakePaymentGateway 의 메서드이고 g 가 Kotlin의 this 라는 뜻.
func (g *FakePaymentGateway) Charge(ctx context.Context, idempotencyKey, orderID string, amount int64) (string, error) {
	if g.FailBeforeCharge.Load() {
		return "", ErrPGFailed
	}

	// 네트워크 지연 흉내. select 는 여러 채널 중 먼저 준비된 쪽을 실행한다.
	// ctx.Done() 은 요청이 취소되면 닫히는 채널 → 취소되면 기다리지 않고 바로 반환. (Kotlin의 delay() 가 취소에 반응하는 것과 같다)
	if g.ChargeDelay > 0 {
		select {
		case <-time.After(g.ChargeDelay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	g.mu.Lock()
	txID, ok := g.byKey[idempotencyKey] // ok = 키 존재 여부
	if !ok {
		txID = "pg_" + uuid.NewString()
		g.byKey[idempotencyKey] = txID
		g.charged++ // 실제로 돈이 빠지는 순간
	}
	g.mu.Unlock()

	// 승인은 끝났지만 응답이 유실된 상황: 호출자는 성공했는지 알 수 없다.
	if g.TimeoutAfterCharge.Load() {
		return "", ErrPGTimeout
	}
	return txID, nil
}

// ChargeCount 는 실제 승인 횟수 (테스트 검증용).
func (g *FakePaymentGateway) ChargeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock() // defer: 함수가 끝날 때 실행 (try-finally 의 finally)
	return g.charged
}
