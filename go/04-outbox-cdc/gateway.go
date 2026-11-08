package outboxcdc

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

// FakePaymentGateway 는 외부 PG(토스페이먼츠/Stripe 등)를 흉내 낸다.
// 04 단계의 주제는 CDC 이므로 PG는 항상 성공하는 최소 구현만 둔다.
// (실패 주입이 있는 완전한 버전은 02-idempotency-key/gateway.go 참고)
//
// struct = 필드만 가진 타입. Kotlin의 class 에서 메서드를 빼고 필드만 남긴 것이라고 보면 된다.
type FakePaymentGateway struct {
	mu      sync.Mutex        // 여러 goroutine(스레드 비슷한 것)이 동시에 map 에 접근하지 못하게 막는 잠금. Kotlin의 synchronized 블록 역할.
	byKey   map[string]string // idempotencyKey -> pgTxId
	charged int
}

// NewFakePaymentGateway 는 생성자 역할을 하는 평범한 함수다. Go에는 생성자 문법이 없어서 NewXxx 함수를 관례로 쓴다.
// *FakePaymentGateway 의 * 는 포인터. 객체를 복사하지 않고 "참조"를 넘긴다 (Kotlin 객체 참조와 같은 느낌).
func NewFakePaymentGateway() *FakePaymentGateway {
	return &FakePaymentGateway{byKey: map[string]string{}}
}

// Charge 는 결제를 승인하고 pgTxId 를 돌려준다.
//
// func (g *FakePaymentGateway) Charge(...) 처럼 이름 앞의 (g *FakePaymentGateway) 를 "리시버"라고 한다.
// 이 함수가 FakePaymentGateway 의 메서드라는 뜻이고, g 는 Kotlin의 this 에 해당한다.
//
// (string, error): Go 함수는 값을 여러 개 반환할 수 있다. "성공 값 + 에러"를 같이 돌려주는 것이 관례.
func (g *FakePaymentGateway) Charge(ctx context.Context, idempotencyKey, orderID string, amount int64) (string, error) {
	if amount <= 0 {
		return "", errors.New("invalid amount")
	}
	g.mu.Lock()
	// defer: 이 함수가 끝날 때(return 이든 panic 이든) 실행할 코드를 예약한다.
	// Kotlin의 try { ... } finally { unlock() } 과 같다.
	defer g.mu.Unlock()

	// PG 자체 멱등성: 같은 key 로 다시 오면 새로 승인하지 않고 이전 pgTxId 를 돌려준다.
	// "값, ok := map[key]" 는 키가 있는지(ok)까지 함께 알려주는 Go 문법.
	if txID, ok := g.byKey[idempotencyKey]; ok {
		return txID, nil
	}
	txID := "pg_" + uuid.NewString() // := 는 "선언 + 대입" (타입 추론). Kotlin의 val txID = ... 와 같다.
	g.byKey[idempotencyKey] = txID
	g.charged++
	return txID, nil
}

// ChargeCount 는 실제로 승인된 횟수 (테스트 검증용).
func (g *FakePaymentGateway) ChargeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.charged
}
