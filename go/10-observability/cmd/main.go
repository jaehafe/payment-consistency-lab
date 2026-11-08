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
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	observability "github.com/jaehafe/payment-consistency-lab/go/10-observability"
)

func main() {
	ctx := context.Background()
	logger := observability.NewLogger(os.Stdout) // JSON 로그를 표준 출력으로 → `| grep request_id` 나 `| jq` 로 본다

	// Kotlin 앱과 테이블이 섞이지 않게 별도 스키마(go_10)
	dsn := getenv("DATABASE_URL", "postgres://lab:lab@localhost:15432/lab?sslmode=disable") + "&search_path=go_10"
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

	mux := http.NewServeMux()
	mux.Handle("/", observability.NewServer(svc, logger))
	mux.Handle("POST /admin/faults", observability.FaultsHandler(gw)) // 장애 주입 (실습용)

	addr := ":" + getenv("PORT", "8091")
	logger.Info("listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
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
