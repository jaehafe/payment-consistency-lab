package outboxcdc

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// 이 단계의 자동 테스트는 "outbox 행이 Debezium 기대 형식으로 들어가는가" 까지만 확인한다.
// Debezium → Kafka 구간(E2E)은 README 의 절차대로 infra/docker-compose.yml 과 cmd/main.go 로 확인한다.

var testPool *pgxpool.Pool

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
	return m.Run()
}

func Test_pay__writes_outbox_row(t *testing.T) {
	ctx := context.Background()
	svc := &PaymentService{DB: testPool, Gateway: NewFakePaymentGateway()}

	p, err := svc.Pay(ctx, "order-1", 10000)
	if err != nil {
		t.Fatal(err)
	}

	var aggType, aggID, typ string
	var payload []byte
	err = testPool.QueryRow(ctx,
		`SELECT aggregatetype, aggregateid, type, payload FROM outbox WHERE aggregateid = $1`, "order-1").
		Scan(&aggType, &aggID, &typ, &payload)
	if err != nil {
		t.Fatal(err)
	}
	// Event Router 라우팅 규칙과 맞는지: aggregatetype 'payment' → 토픽 'payment.events'
	if aggType != "payment" || aggID != "order-1" || typ != "PaymentCompleted" {
		t.Fatalf("Debezium 라우팅용 컬럼이 기대와 다르다: %s/%s/%s", aggType, aggID, typ)
	}
	var ev PaymentCompleted
	if err := json.Unmarshal(payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.PaymentID != p.ID || ev.Amount != 10000 || ev.PgTxID == "" || ev.OccurredAt.IsZero() {
		t.Fatalf("payload 가 기대와 다르다: %+v", ev)
	}
}
