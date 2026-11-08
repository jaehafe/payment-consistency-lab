package statemachine

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status 는 결제 상태. string 기반의 새 타입이라 아무 문자열이나 섞여 들어오는 실수를 줄인다.
type Status string

const (
	Requested       Status = "REQUESTED"
	Pending         Status = "PENDING" // PG 호출 중
	Approved        Status = "APPROVED"
	Failed          Status = "FAILED"
	Unknown         Status = "UNKNOWN"          // PG 결과를 모름 (타임아웃). 실패로 단정하면 안 된다!
	ManualReview    Status = "MANUAL_REVIEW"    // 자동 확인 포기 → 사람이 PG 어드민에서 확인
	CancelRequested Status = "CANCEL_REQUESTED" // 보상(취소) 진행 중
	Canceled        Status = "CANCELED"
)

// allowedTransitions 는 허용 전이표. 여기에 없는 전이는 전부 거부한다.
// map[Status][]Status = Kotlin의 Map<Status, List<Status>>
var allowedTransitions = map[Status][]Status{
	Requested:       {Pending},
	Pending:         {Approved, Failed, Unknown},
	Unknown:         {Approved, Failed, ManualReview},
	Approved:        {CancelRequested},
	CancelRequested: {Canceled},
	// Failed, ManualReview, Canceled 는 최종 상태 (나가는 전이 없음)
	// MANUAL_REVIEW 를 사람이 처리한 뒤의 전이는 운영 도구의 몫이라 여기선 다루지 않는다.
}

// CanTransition 은 전이표를 조회한다.
func CanTransition(from, to Status) bool {
	return slices.Contains(allowedTransitions[from], to) // 없는 key 를 조회하면 nil 슬라이스 → false
}

var (
	// ErrInvalidTransition: 전이표에 없는 전이 (코드 버그).
	ErrInvalidTransition = errors.New("invalid status transition")
	// ErrStaleStatus: compare-and-set 실패 — 그 사이 다른 누군가가 상태를 바꿨다 (동시성).
	ErrStaleStatus = errors.New("payment status changed concurrently")
)

// Repository 는 payments / payment_status_history 접근.
type Repository struct {
	DB *pgxpool.Pool
}

// Transition 은 전이 하나를 자체 트랜잭션으로 실행한다.
func (r *Repository) Transition(ctx context.Context, paymentID string, from, to Status, reason string) error {
	return withTx(ctx, r.DB, func(tx pgx.Tx) error {
		return TransitionTx(ctx, tx, paymentID, from, to, reason)
	})
}

// TransitionTx 는 호출자가 연 트랜잭션 안에서 전이한다 (pg_tx_id 같은 다른 컬럼 갱신과 원자적으로 묶기 위해).
//
//  1. 전이표 검사 → 없으면 ErrInvalidTransition
//  2. UPDATE ... WHERE id = ? AND status = <from>  (compare-and-set)
//     → 0행이면 그 사이 누가 상태를 바꾼 것 → ErrStaleStatus
//  3. history insert — 같은 트랜잭션이라 "상태는 바뀌었는데 이력은 없음" 이 불가능
func TransitionTx(ctx context.Context, tx pgx.Tx, paymentID string, from, to Status, reason string) error {
	if !CanTransition(from, to) {
		// %w 로 감싸면 호출자가 errors.Is(err, ErrInvalidTransition) 로 확인할 수 있다
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	// SELECT 해서 확인하고 UPDATE 하면 그 사이에 바뀔 수 있다. WHERE 절에 "기대하는 현재 상태"를 넣으면
	// 확인과 변경이 한 문장(원자적)이 된다. 06 의 조건부 UPDATE 와 같은 원리.
	tag, err := tx.Exec(ctx,
		`UPDATE payments SET status = $3, updated_at = now() WHERE id = $1 AND status = $2`,
		paymentID, string(from), string(to))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: expected %s", ErrStaleStatus, from)
	}
	return insertHistory(ctx, tx, paymentID, &from, to, reason)
}

// insertHistory 는 이력을 남긴다. from 이 nil 이면 최초 생성.
// *Status (포인터)는 "없을 수도 있는 값" — Kotlin의 Status? 와 같다. nil 이면 DB에 NULL 로 들어간다.
func insertHistory(ctx context.Context, tx pgx.Tx, paymentID string, from *Status, to Status, reason string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO payment_status_history (payment_id, from_status, to_status, reason) VALUES ($1, $2, $3, $4)`,
		paymentID, from, string(to), reason)
	return err
}

// withTx 는 "Begin → fn → Commit, 에러면 Rollback" 을 감싼 헬퍼.
// fn 은 함수 타입 파라미터 (Kotlin의 (Tx) -> Unit 람다). Spring의 transactionTemplate.execute { } 와 같은 모양.
func withTx(ctx context.Context, db *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Payment 는 payments 한 행.
type Payment struct {
	ID             string
	OrderID        string
	Amount         int64
	Status         Status
	IdempotencyKey string
	PgTxID         string
	AttemptCount   int
	LastError      string
}

// Get 은 결제 한 건을 읽는다.
func (r *Repository) Get(ctx context.Context, id string) (Payment, error) {
	var p Payment
	err := r.DB.QueryRow(ctx, `
		SELECT id::text, order_id, amount, status, idempotency_key, coalesce(pg_tx_id, ''), attempt_count, coalesce(last_error, '')
		  FROM payments WHERE id = $1`, id).
		Scan(&p.ID, &p.OrderID, &p.Amount, &p.Status, &p.IdempotencyKey, &p.PgTxID, &p.AttemptCount, &p.LastError)
	return p, err
}

// HistoryEntry 는 이력 한 줄. FromStatus 가 "" 이면 최초 생성.
type HistoryEntry struct {
	From, To Status // 같은 타입 필드는 한 줄에 선언할 수 있다
	Reason   string
}

// History 는 결제의 전이 이력을 시간순으로 돌려준다.
func (r *Repository) History(ctx context.Context, id string) ([]HistoryEntry, error) {
	rows, err := r.DB.Query(ctx, `
		SELECT coalesce(from_status, ''), to_status, coalesce(reason, '')
		  FROM payment_status_history WHERE payment_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	// pgx.CollectRows: rows 를 끝까지 읽어 슬라이스로 만든다. 콜백이 한 행을 값으로 바꾼다 (Kotlin의 map).
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (HistoryEntry, error) {
		var h HistoryEntry
		err := row.Scan(&h.From, &h.To, &h.Reason)
		return h, err
	})
}
