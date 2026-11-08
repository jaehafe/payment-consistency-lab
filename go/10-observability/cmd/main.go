// 10-observability 서버: :8091 에서 /payments, /metrics, /admin/faults 를 연다.
// infra/observability/prometheus.yml 이 host.docker.internal:8091/metrics 를 스크레이프한다.
//
//	docker compose -f infra/docker-compose.yml --profile obs up -d   # 저장소 루트에서 (postgres, prometheus, grafana)
//	cd go && go run ./10-observability/cmd
//
// 자세한 장애 시나리오 실습은 10-observability/README.md 의 런북 참고.
package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	observability "github.com/jaehafe/payment-consistency-lab/go/10-observability"
)

func main() {
	ctx := context.Background()
	logger := observability.NewLogger(os.Stdout) // JSON 로그를 표준 출력으로 → `| grep request_id` 나 `| jq` 로 본다

	// Kotlin 앱과 테이블이 섞이지 않게 별도 스키마(go_10)
	dsn := withSearchPath(getenv("DATABASE_URL", "postgres://lab:lab@localhost:15432/lab?sslmode=disable"), "go_10")
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fatal(logger, err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS go_10`); err != nil {
		fatal(logger, err)
	}
	if err := observability.ApplySchema(ctx, db); err != nil {
		fatal(logger, err)
	}

	gw := observability.NewFakePaymentGateway()
	gw.SetLatency(80 * time.Millisecond) // 평상시 PG 지연
	svc := &observability.PaymentService{
		DB: db, Gateway: gw, Metrics: observability.NewMetrics(db, logger), Log: logger, PgTimeout: 5 * time.Second,
	}

	mux := observability.Routes(svc)
	// 장애 주입 (실습용). 같은 mux 에 붙여서 아래 미들웨어를 함께 탄다
	// → 장애를 주입한 요청도 request_id 가 붙은 access log 로 남는다.
	mux.Handle("POST /admin/faults", observability.FaultsHandler(gw))
	handler := observability.RequestLogging(logger, mux)

	addr := ":" + getenv("PORT", "8091")
	logger.Info("listening", "addr", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		fatal(logger, err)
	}
}

func fatal(logger *slog.Logger, err error) {
	logger.Error("fatal", "error", err)
	os.Exit(1)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// withSearchPath 는 DSN 에 search_path 를 붙인다.
// 문자열에 "&search_path=" 를 그냥 이어 붙이면 DATABASE_URL 에
// "?" 가 없을 때(예: postgres://u:p@h/db) 잘못된 DSN 이 된다
// → net/url 로 파싱해서 쿼리 파라미터로 넣는다.
func withSearchPath(dsn, schema string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		log.Fatalf("DATABASE_URL 파싱 실패: %v", err)
	}
	q := u.Query() // url.Values = map[string][]string
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
