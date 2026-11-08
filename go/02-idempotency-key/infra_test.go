package idempotency

// 테스트 인프라: Testcontainers 로 실제 Postgres 를 띄운다 (이 단계는 Kafka 불필요).

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var testPool *pgxpool.Pool

// TestMain = JUnit @BeforeAll/@AfterAll. 패키지의 모든 테스트 전후로 한 번 실행된다.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

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

// testApp 은 테스트 하나에 필요한 것들(PG, HTTP 서버)을 묶는다.
type testApp struct {
	gateway *FakePaymentGateway
	server  *httptest.Server
}

func newTestApp(t *testing.T) *testApp {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), `TRUNCATE payments, idempotency_keys`); err != nil {
		t.Fatal(err)
	}
	gw := NewFakePaymentGateway()
	svc := &PaymentService{DB: testPool, Gateway: gw}
	srv := httptest.NewServer(NewHandler(svc)) // 실제 HTTP 서버를 임의 포트로 띄운다 (Spring의 MockMvc 보다는 RANDOM_PORT 테스트에 가깝다)
	t.Cleanup(srv.Close)
	return &testApp{gateway: gw, server: srv}
}

// post 는 POST /payments 를 보내고 (상태 코드, 바디)를 돌려준다. key 가 "" 이면 헤더를 생략한다.
func (a *testApp) post(t *testing.T, key, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, a.server.URL+"/payments", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err) // goroutine 안에서도 호출되므로 Fatal 대신 Error (Fatal 은 테스트 goroutine 에서만 써야 한다)
		return 0, nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func decodeResponse(t *testing.T, b []byte) PaymentResponse {
	t.Helper()
	var r PaymentResponse
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatalf("응답 파싱 실패: %v (%s)", err, b)
	}
	return r
}
