package statemachine

// 테스트 인프라: Testcontainers 로 실제 Postgres 16 을 띄운다 (이 단계는 Kafka 불필요).
// _test.go 파일은 `go test` 때만 컴파일된다 (Kotlin의 src/test).

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var testPool *pgxpool.Pool

// TestMain = JUnit @BeforeAll/@AfterAll. 패키지의 모든 테스트 전후로 한 번 실행된다.
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
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Print(err)
		return 1
	}
	// 동시성 테스트에서 goroutine 수보다 커넥션이 적으면 풀에서 줄을 서느라 "동시"가 아니게 된다.
	cfg.MaxConns = 40
	if testPool, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
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

// exec 는 테스트용 SQL 실행 헬퍼. 실패하면 테스트를 중단한다.
func exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// queryInt 는 숫자 하나를 돌려주는 쿼리 헬퍼.
func queryInt(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := testPool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}
