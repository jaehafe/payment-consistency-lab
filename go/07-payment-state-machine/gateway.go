package statemachine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// PgStatus 는 PG 조회 API 의 응답.
type PgStatus string

const (
	PgApproved PgStatus = "APPROVED"
	PgFailed   PgStatus = "FAILED"
	PgNotFound PgStatus = "NOT_FOUND" // PG 에 요청 기록 자체가 없음 (요청이 도달하지 못함)
)

var (
	ErrPgTimeout  = errors.New("pg: timeout (simulated)")
	ErrPgDeclined = errors.New("pg: declined (simulated)")
	ErrPgCancel   = errors.New("pg: cancel failed (simulated)")
	ErrPgQuery    = errors.New("pg: status query unavailable (simulated)")
)

// FakePaymentGateway 는 승인 / 조회 / 취소를 지원하는 Fake PG.
type FakePaymentGateway struct {
	// 실패 주입 스위치들 (atomic.Bool: goroutine 간에 안전한 bool)
	TimeoutBeforeCharge atomic.Bool // 요청이 PG 에 도달하지 못하고 타임아웃 → PG 엔 기록 없음
	TimeoutAfterCharge  atomic.Bool // PG 는 승인했는데 응답이 유실 → 호출자는 타임아웃만 봄
	DeclineCharge       atomic.Bool // 카드 한도 초과 등 명확한 거절
	FailCancel          atomic.Bool // 취소 API 실패
	FailStatusQuery     atomic.Bool // 조회 API 장애

	// 테스트 훅 (nil 이면 안 부름). 경쟁 상황을 결정적으로 재현한다.
	// 테스트가 호출 전에 한 번 넣고 바꾸지 않는다고 가정한다.
	OnBeforeCharge func(key string) // 승인 요청이 PG 에 닿기 전 (요청이 PG 앞에서 멈춘 상황)
	OnCharge       func(key string) // 승인이 PG 에 기록된 직후, 응답 전
	OnGetStatus    func(key string) // 조회 요청이 PG 에 도착한 순간

	mu      sync.Mutex
	byKey   map[string]*pgRecord // idempotencyKey → 기록
	byTxID  map[string]*pgRecord // pgTxId → 기록
	charges int
	cancels int
}

type pgRecord struct {
	txID     string
	status   PgStatus
	canceled bool
}

func NewFakePaymentGateway() *FakePaymentGateway {
	return &FakePaymentGateway{byKey: map[string]*pgRecord{}, byTxID: map[string]*pgRecord{}}
}

// Charge 는 승인. 같은 key 로 다시 오면 기존 결과를 돌려준다 (PG 멱등성).
func (g *FakePaymentGateway) Charge(ctx context.Context, key, orderID string, amount int64) (string, error) {
	if g.OnBeforeCharge != nil {
		g.OnBeforeCharge(key) // 아직 PG 에 기록 없음
	}
	if g.TimeoutBeforeCharge.Load() {
		return "", ErrPgTimeout // PG 는 아무것도 모른다
	}
	g.mu.Lock()
	rec, ok := g.byKey[key]
	if !ok {
		rec = &pgRecord{txID: "pg_" + uuid.NewString(), status: PgApproved}
		if g.DeclineCharge.Load() {
			rec.status = PgFailed
		} else {
			g.charges++
		}
		g.byKey[key] = rec
		g.byTxID[rec.txID] = rec
	}
	g.mu.Unlock()

	if g.OnCharge != nil {
		g.OnCharge(key) // 잠금을 푼 뒤에 부른다 (훅이 GetStatus 를 불러도 됨)
	}
	if g.TimeoutAfterCharge.Load() {
		return "", ErrPgTimeout // 승인은 됐지만 응답 유실
	}
	if rec.status == PgFailed {
		return "", ErrPgDeclined
	}
	return rec.txID, nil
}

// GetStatus 는 idempotency key 로 PG 측 상태를 조회한다. (상태, pgTxId, 에러)
func (g *FakePaymentGateway) GetStatus(ctx context.Context, key string) (PgStatus, string, error) {
	if g.OnGetStatus != nil {
		g.OnGetStatus(key)
	}
	if err := ctx.Err(); err != nil {
		return "", "", err // 호출자가 정한 시간 안에 응답하지 못함 (실제 HTTP 클라이언트처럼)
	}
	if g.FailStatusQuery.Load() {
		return "", "", ErrPgQuery
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	rec, ok := g.byKey[key]
	if !ok {
		return PgNotFound, "", nil
	}
	return rec.status, rec.txID, nil
}

// Cancel 은 승인 취소. 이미 취소된 건이면 성공으로 본다 (취소도 멱등해야 재시도가 안전하다).
func (g *FakePaymentGateway) Cancel(ctx context.Context, pgTxID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.FailCancel.Load() {
		return ErrPgCancel
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	rec, ok := g.byTxID[pgTxID]
	if !ok {
		return errors.New("pg: unknown pgTxId")
	}
	if !rec.canceled {
		rec.canceled = true
		g.cancels++
	}
	return nil
}

// ChargeCount / CancelCount / IsCanceled 는 테스트 검증용.
func (g *FakePaymentGateway) ChargeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.charges
}

func (g *FakePaymentGateway) CancelCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cancels
}

func (g *FakePaymentGateway) IsCanceled(pgTxID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	rec, ok := g.byTxID[pgTxID]
	return ok && rec.canceled
}
