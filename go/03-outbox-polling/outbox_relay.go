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
	// 중복을 막는 건 FOR UPDATE 다. SKIP LOCKED 만 빼면 기다릴 뿐 중복은 없고,
	// FOR UPDATE 까지 빼면 두 릴레이가 같은 행을 읽어 중복 발행한다.
	rows, err := tx.Query(ctx, `
		SELECT id::text, aggregate_id, event_type, payload
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY created_at
		 LIMIT $1
		   FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return 0, err
	}
	// nil 슬라이스로 시작해도 append 할 수 있다 (Kotlin의 mutableListOf())
	var batch []Message
	// rows.Next() 가 false 가 될 때까지 = JDBC ResultSet.next() 와 같다
	for rows.Next() {
		var o Message
		err := rows.Scan(&o.ID, &o.Key, &o.EventType, &o.Payload)
		if err != nil {
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
		if err := r.Publisher.Publish(ctx, o); err != nil {
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

// Run 은 ctx 가 취소될 때까지 RelayOnce 를 반복한다 (cmd/main.go 에서 사용).
//
// Kotlin 03 의 @Scheduled(fixedDelay) 와 같게: "이전 실행이 끝난 뒤"
// interval 만큼 쉰다. (time.Ticker 는 시작 시각 기준으로 interval 마다
// 신호를 보내므로 fixedRate 에 가깝다 — 오래 걸린 회차 뒤엔 바로 또 돈다.)
func (r *OutboxRelay) Run(
	ctx context.Context, interval time.Duration, batchSize int,
) {
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
		// select: 여러 채널 중 먼저 값이 오는 쪽을 실행 (Kotlin의 select { })
		// time.After(d): d 뒤에 값이 한 번 오는 채널 = "지금부터 d 쉬기"
		select {
		case <-ctx.Done(): // 종료 신호
			return
		case <-time.After(interval): // 다 비운 시점부터 interval 뒤
		}
	}
}
