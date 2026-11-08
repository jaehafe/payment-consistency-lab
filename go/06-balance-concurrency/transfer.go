package balance

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TransferService 는 계좌 간 이체. 두 행을 모두 잠가야 하므로 "잠금 순서"가 문제가 된다.
type TransferService struct {
	DB *pgxpool.Pool
	// LockGap: 첫 번째 잠금과 두 번째 잠금 사이에 기다리는 시간 (데드락을 결정적으로 재현하기 위한 틈).
	LockGap time.Duration
}

// ErrSameAccount 는 자기 자신에게 이체할 때.
var ErrSameAccount = errors.New("from and to must differ")

// Transfer 는 두 계좌를 항상 id 오름차순으로 잠근다 → 데드락이 생기지 않는다.
//
// 왜? 데드락은 "A가 X를 쥐고 Y를 기다림 + B가 Y를 쥐고 X를 기다림" 의 순환이다.
// 모두가 같은 순서(X → Y)로만 잠그면 순환이 생길 수 없다. A→B 이체든 B→A 이체든 "작은 id 먼저".
func (s *TransferService) Transfer(ctx context.Context, from, to string, amount int64) (Result, error) {
	if from == to {
		return "", ErrSameAccount
	}
	ids := []string{from, to}
	slices.Sort(ids) // 표준 라이브러리 정렬 (Kotlin의 sorted())
	return s.transfer(ctx, from, to, amount, ids)
}

// TransferUnordered 는 잠금 순서를 정렬하지 않는 "잘못된" 버전: 항상 from 먼저, to 나중.
// A→B 와 B→A 가 동시에 오면 서로 상대가 쥔 행을 기다리다 데드락 → Postgres 가 한쪽을 40P01 로 죽인다.
func (s *TransferService) TransferUnordered(ctx context.Context, from, to string, amount int64) (Result, error) {
	if from == to {
		return "", ErrSameAccount
	}
	return s.transfer(ctx, from, to, amount, []string{from, to})
}

func (s *TransferService) transfer(ctx context.Context, from, to string, amount int64, lockOrder []string) (Result, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	// map[string]int64{} : 계좌 id → 잠그면서 읽은 잔액
	balances := map[string]int64{}
	for i, id := range lockOrder {
		if i > 0 {
			time.Sleep(s.LockGap) // 첫 번째 행만 쥔 채로 잠시 대기
		}
		var b int64
		if err := tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1 FOR UPDATE`, id).Scan(&b); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return "", ErrAccountNotFound
			}
			return "", err // 데드락이면 여기서 SQLSTATE 40P01 에러가 올라온다
		}
		balances[id] = b
	}

	if balances[from] < amount {
		return Insufficient, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance - $2 WHERE id = $1`, from, amount); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE accounts SET balance = balance + $2 WHERE id = $1`, to, amount); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return Success, nil
}
