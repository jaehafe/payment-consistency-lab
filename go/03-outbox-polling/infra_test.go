package outboxpolling

// 테스트 인프라: Testcontainers 로 실제 Postgres + Redpanda 를 띄운다.
// _test.go 로 끝나는 파일은 `go test` 때만 컴파일된다 (Kotlin의 src/test 디렉토리 역할).

import (
	"context"
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

// 패키지 전체 테스트가 공유하는 컨테이너 연결 정보.
var (
	testPool    *pgxpool.Pool
	testBrokers string
)

// TestMain 은 이 패키지의 모든 테스트 전에 한 번 실행된다. JUnit의 @BeforeAll + @AfterAll 에 해당.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background() // 취소/타임아웃이 없는 "루트" context

	pg, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("lab"), tcpostgres.WithUsername("lab"), tcpostgres.WithPassword("lab"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		log.Printf("postgres 컨테이너 시작 실패: %v", err)
		return 1
	}
	defer testcontainers.TerminateContainer(pg) // runTests 가 끝날 때 컨테이너 정리

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
	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
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

// resetDB 는 테스트 간 데이터를 지운다.
func resetDB(t *testing.T) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), `TRUNCATE payments, outbox`); err != nil {
		t.Fatal(err)
	}
}

// newTopic 은 테스트마다 새 토픽을 만든다 (테스트 간 메시지가 섞이지 않게).
func newTopic(t *testing.T) string {
	t.Helper()
	topic := fmt.Sprintf("%s.%s", TopicPaymentEvents, uuid.NewString()[:8])
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).CreateTopic(context.Background(), 1, 1, nil, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		t.Fatal(err)
	}
	return topic
}

// newKafkaPublisher 는 실제 Redpanda 로 보내는 Publisher 를 만든다.
func newKafkaPublisher(t *testing.T, topic string) *KafkaPublisher {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close) // t.Cleanup: 이 테스트가 끝나면 실행 (defer 의 테스트 버전)
	return &KafkaPublisher{Client: cl, Topic: topic}
}

// consumeAll 은 토픽에 지금까지 쌓인 메시지를 "전부" 읽어온다.
// 먼저 끝 오프셋(high watermark)을 조회해서 정확히 그 개수만큼 읽으므로, "0건"도 기다림 없이 확정할 수 있다.
func consumeAll(t *testing.T, topic string) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // 15초 후 자동 취소되는 ctx
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(testBrokers), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	var want int64
	ends.Each(func(o kadm.ListedOffset) { want += o.Offset })

	var recs []*kgo.Record
	for int64(len(recs)) < want {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("메시지 %d개 중 %d개만 읽음", want, len(recs))
		}
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	}
	return recs
}
