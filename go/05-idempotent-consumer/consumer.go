package idempotentconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MessageHandler 는 메시지 한 건을 처리한다. 반환값 processed=false 는 "중복이라 건너뜀".
// NaiveConsumer, IdempotentConsumer 둘 다 이 메서드를 갖고 있으므로 자동으로 이 인터페이스를 만족한다
// (Go는 implements 선언이 없다).
type MessageHandler interface {
	Handle(ctx context.Context, rec *kgo.Record) (processed bool, err error)
}

// ---------------------------------------------------------------------------
// NaiveConsumer: 받을 때마다 그대로 처리한다. 같은 메시지가 두 번 오면 포인트도 두 번 쌓인다.
// ---------------------------------------------------------------------------

type NaiveConsumer struct {
	DB *pgxpool.Pool
}

func (c *NaiveConsumer) Handle(ctx context.Context, rec *kgo.Record) (bool, error) {
	ev, err := decode(rec)
	if err != nil {
		return false, err
	}
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	if err := applyPayment(ctx, tx, ev); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// IdempotentConsumer: processed_messages 로 "이미 처리한 message_id" 를 기록해서 중복을 건너뛴다.
// ---------------------------------------------------------------------------

type IdempotentConsumer struct {
	DB    *pgxpool.Pool
	Group string // 컨슈머 그룹마다 처리 여부를 따로 관리한다 (포인트 서비스와 알림 서비스는 각자 처리해야 하므로)
}

func (c *IdempotentConsumer) Handle(ctx context.Context, rec *kgo.Record) (bool, error) {
	msgID := header(rec, "message_id")
	if msgID == "" {
		return false, errors.New("message_id header is missing")
	}
	ev, err := decode(rec)
	if err != nil {
		return false, err
	}

	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	// ★ 핵심: "처리 기록"과 "비즈니스 처리"를 같은 트랜잭션에 넣는다.
	//  - 둘 다 커밋되거나 둘 다 롤백된다 → "처리했는데 기록 안 됨" / "기록했는데 처리 안 됨" 이 없다.
	//  - 같은 메시지를 두 컨슈머가 동시에 처리해도, 늦은 쪽 INSERT 는 PK 충돌로 앞선 tx 커밋을 기다렸다가 0행이 된다.
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_messages (consumer_group, message_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, c.Group, msgID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil // 이미 처리한 메시지 → 아무것도 하지 않음 (tx 는 defer 로 롤백)
	}

	if err := applyPayment(ctx, tx, ev); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// applyPayment 는 실제 비즈니스 처리.
//   - orders 갱신: "PAID 로, 금액은 X 로" 덮어쓰기라 여러 번 해도 결과가 같다 (자연적으로 멱등).
//   - point_ledger insert: 할 때마다 행이 늘어난다 (멱등하지 않음) ← 중복 처리의 피해가 드러나는 곳.
//
// pgx.Tx 는 인터페이스 타입이라 어떤 트랜잭션 구현이든 받을 수 있다.
func applyPayment(ctx context.Context, tx pgx.Tx, ev PaymentCompleted) error {
	if _, err := tx.Exec(ctx,
		`UPDATE orders SET status = 'PAID', paid_amount = $2 WHERE id = $1`, ev.OrderID, ev.Amount); err != nil {
		return err
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO point_ledger (order_id, points) VALUES ($1, $2)`, ev.OrderID, ev.Amount/100) // 1% 적립
	return err
}

// ---------------------------------------------------------------------------
// ConsumerLoop: Kafka 에서 가져와 Handler 로 처리하고, 그 "다음에" offset 을 커밋한다.
// ---------------------------------------------------------------------------

type ConsumerLoop struct {
	Client  *kgo.Client // kgo.ConsumerGroup + kgo.DisableAutoCommit 으로 만든 클라이언트여야 한다
	Handler MessageHandler

	CrashBeforeOffsetCommit bool // 실패 주입: DB 커밋 후 offset 커밋 전에 죽음
}

// ErrSimulatedCrash 는 offset 커밋 직전 크래시를 흉내 낸다.
var ErrSimulatedCrash = errors.New("consumer crashed before offset commit (simulated)")

// PollStats 는 한 번 poll 한 결과.
type PollStats struct {
	Processed int // 실제 처리
	Skipped   int // 중복이라 건너뜀
}

// PollOnce 는 한 번 가져온 레코드들을 처리하고 offset 을 커밋한다.
func (l *ConsumerLoop) PollOnce(ctx context.Context) (PollStats, error) {
	var stats PollStats
	fetches := l.Client.PollFetches(ctx) // 레코드가 올 때까지(또는 ctx 취소까지) 기다린다
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		return stats, fmt.Errorf("fetch: %v", errs[0].Err)
	}

	recs := fetches.Records()
	for _, rec := range recs {
		processed, err := l.Handler.Handle(ctx, rec)
		if err != nil {
			// offset 을 커밋하지 않고 반환 → 재시작하면 이 메시지부터 다시 받는다.
			return stats, err
		}
		if processed {
			stats.Processed++
		} else {
			stats.Skipped++
		}
	}

	// ★ 순서가 중요하다: DB 커밋(위 Handle) → offset 커밋(아래).
	// 반대로 offset 을 먼저 커밋하고 DB 처리 중 죽으면, 메시지는 "읽은 것"으로 처리되어 영영 유실된다 (at-most-once).
	// 이 순서면 둘 사이에서 죽었을 때 메시지가 "다시" 온다 (at-least-once) → 그 중복은 processed_messages 가 걸러낸다.
	if l.CrashBeforeOffsetCommit {
		return stats, ErrSimulatedCrash
	}
	if len(recs) > 0 {
		if err := l.Client.CommitRecords(ctx, recs...); err != nil { // recs... = 슬라이스를 가변 인자로 펼쳐 전달 (Kotlin의 *array)
			return stats, err
		}
	}
	return stats, nil
}

// ---- 헬퍼 ----

func decode(rec *kgo.Record) (PaymentCompleted, error) {
	var ev PaymentCompleted
	if err := json.Unmarshal(rec.Value, &ev); err != nil {
		return ev, fmt.Errorf("decode payload: %w", err)
	}
	return ev, nil
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
