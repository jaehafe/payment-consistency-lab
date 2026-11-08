package idempotentconsumer

// 테스트 인프라: Testcontainers 로 실제 Postgres + Redpanda 를 띄운다.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	testPool    *pgxpool.Pool
	testBrokers string
)

func TestMain(m *testing.M) { os.Exit(runTests(m)) }

func runTests(m *testing.M) int {
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("lab"), tcpostgres.WithUsername("lab"), tcpostgres.WithPassword("lab"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		log.Printf("postgres 컨테이너 시작 실패: %v", err)
		return 1
	}
	defer testcontainers.TerminateContainer(pg)

	rp, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err != nil {
		log.Printf("redpanda 컨테이너 시작 실패: %v", err)
		return 1
	}
	defer testcontainers.TerminateContainer(rp)

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Print(err)
		return 1
	}
	if testPool, err = pgxpool.New(ctx, dsn); err != nil {
		log.Print(err)
		return 1
	}
	defer testPool.Close()
	if err := ApplySchema(ctx, testPool); err != nil {
		log.Print(err)
		return 1
	}
	if testBrokers, err = rp.KafkaSeedBroker(ctx); err != nil {
		log.Print(err)
		return 1
	}
	return m.Run()
}

// fixture 는 테스트마다 새 토픽/그룹을 만들고, orders 에 PENDING 주문을 하나 넣는다.
type fixture struct {
	topic string
	group string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `TRUNCATE orders, point_ledger, processed_messages`); err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `INSERT INTO orders (id, status) VALUES ('order-1', 'PENDING')`); err != nil {
		t.Fatal(err)
	}

	suffix := uuid.NewString()[:8]
	f := fixture{topic: fmt.Sprintf("%s.%s", TopicPaymentEvents, suffix), group: "point-service-" + suffix}
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, f.topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// produce 는 같은 message_id 로 메시지를 times 번 보낸다. messageID 가 "" 이면 헤더를 뺀다.
// 03 단계에서 본 "릴레이가 발행 후 published_at 갱신 전에 죽어서 재발행" 상황을 그대로 재현한 것.
func (f fixture) produce(t *testing.T, messageID string, times int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	payload, _ := json.Marshal(PaymentCompleted{
		PaymentID: uuid.NewString(), OrderID: "order-1", Amount: 10000, PgTxID: "pg_1", OccurredAt: time.Now().UTC(),
	})
	for i := 0; i < times; i++ {
		rec := &kgo.Record{
			Topic: f.topic, Key: []byte("order-1"), Value: payload,
		}
		if messageID != "" { // "" 이면 헤더 없이 보낸다 (헤더 누락 테스트용)
			rec.Headers = []kgo.RecordHeader{
				{Key: "message_id", Value: []byte(messageID)},
			}
		}
		if err := cl.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// newGroupClient 는 컨슈머 그룹에 참여하는 클라이언트를 만든다. 자동 커밋은 끈다 (직접 커밋하기 위해).
func (f fixture) newGroupClient(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testBrokers),
		kgo.ConsumerGroup(f.group),
		kgo.ConsumeTopics(f.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // 커밋된 offset 이 없으면 처음부터
		kgo.DisableAutoCommit(),                           // ★ offset 은 DB 커밋 후에 우리가 직접 커밋한다
	)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// pollUntil 은 처리+스킵 건수가 want 에 도달할 때까지 PollOnce 를 반복한다.
func pollUntil(t *testing.T, loop *ConsumerLoop, want int) PollStats {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var total PollStats
	for total.Processed+total.Skipped < want {
		s, err := loop.PollOnce(ctx)
		total.Processed += s.Processed
		total.Skipped += s.Skipped
		if err != nil {
			t.Fatalf("poll 실패: %v (지금까지 %+v)", err, total)
		}
	}
	return total
}

func queryInt(t *testing.T, sql string) int64 {
	t.Helper()
	var n int64
	if err := testPool.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
