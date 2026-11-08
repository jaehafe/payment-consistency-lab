package outboxpolling

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrSimulatedCrash 는 "발행 직후, published_at 갱신 전에 프로세스가 죽었다"를 흉내 낸다.
var ErrSimulatedCrash = errors.New("relay crashed after publish, before marking (simulated)")

// OutboxRelay 는 outbox 에서 미발행 행을 읽어 Kafka 로 보내고 published_at 을 찍는다.
type OutboxRelay struct {
	DB        *pgxpool.Pool
	Publisher EventPublisher

	CrashAfterPublish atomic.Bool // 실패 주입 스위치
}

// outboxRow 는 outbox 한 행에서 발행에 필요한 부분.
type outboxRow struct {
	ID          string
	AggregateID string
	Payload     []byte
}

// RelayOnce 는 최대 batchSize 개를 발행하고, 발행한 개수를 돌려준다.
func (r *OutboxRelay) RelayOnce(ctx context.Context, batchSize int) (int, error) {
	tx, err := r.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// FOR UPDATE SKIP LOCKED:
	//  - FOR UPDATE: 고른 행을 이 트랜잭션이 끝날 때까지 잠근다.
	//  - SKIP LOCKED: 다른 릴레이가 이미 잠근 행은 기다리지 않고 "건너뛴다".
	// → 릴레이 인스턴스를 여러 개 띄워도 같은 행을 두 번 집지 않고, 서로 기다리지도 않는다 (수평 확장 가능).
	rows, err := tx.Query(ctx, `
		SELECT id::text, aggregate_id, payload
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY created_at
		 LIMIT $1
		   FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return 0, err
	}
	var batch []outboxRow // nil 슬라이스로 시작해도 append 할 수 있다 (Kotlin의 mutableListOf())
	for rows.Next() {     // rows.Next() 가 false 가 될 때까지 = JDBC ResultSet.next() 와 같다
		var o outboxRow
		if err := rows.Scan(&o.ID, &o.AggregateID, &o.Payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	ids := make([]string, 0, len(batch)) // 길이 0, 용량 len(batch) 인 슬라이스
	for _, o := range batch {            // _ 는 인덱스를 안 쓴다는 뜻
		// 동기 발행 (acks=all). 브로커가 확실히 받은 것만 "발행됨"으로 표시하기 위해서다.
		// 실패하면 return → tx 롤백 → 잠금이 풀리고 published_at 은 여전히 NULL → 다음 주기에 재시도.
		if err := r.Publisher.Publish(ctx, o.AggregateID, o.ID, o.Payload); err != nil {
			return 0, err
		}
		ids = append(ids, o.ID)
	}

	// ★ at-least-once 의 근원: 여기(발행 완료 ~ 커밋 사이)에서 죽으면 브로커엔 이미 메시지가 있는데
	// published_at 은 NULL 로 남는다 → 재시작 후 같은 메시지를 또 보낸다. "정확히 한 번"은 보장할 수 없다.
	if r.CrashAfterPublish.Load() {
		return 0, ErrSimulatedCrash
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1::uuid[])`, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}

// Run 은 ctx 가 취소될 때까지 interval 마다 RelayOnce 를 반복한다 (cmd/main.go 에서 사용).
func (r *OutboxRelay) Run(ctx context.Context, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval) // interval 마다 신호를 보내는 채널 (Spring의 @Scheduled(fixedDelay) 역할)
	defer ticker.Stop()
	for {
		// 밀린 게 있으면 쉬지 않고 계속 비운다.
		for {
			n, err := r.RelayOnce(ctx, batchSize)
			if err != nil {
				log.Printf("relay error (다음 주기에 재시도): %v", err)
				break
			}
			if n < batchSize {
				break
			}
		}
		select {
		case <-ctx.Done(): // 종료 신호
			return
		case <-ticker.C: // 다음 주기
		}
	}
}
