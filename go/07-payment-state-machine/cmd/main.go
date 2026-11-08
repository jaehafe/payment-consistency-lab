// 07 janitor 를 프로세스로 띄워 주기 실행한다.
// (Kotlin 에선 JanitorScheduler 가 앱 안에서 @Scheduled 로 돈다)
//
//	docker compose -f ../infra/docker-compose.yml up -d postgres
//	go run ./07-payment-state-machine/cmd
//	JANITOR_INTERVAL=2s go run ./07-payment-state-machine/cmd
//
// 다른 터미널에서 UNKNOWN 결제를 넣으면 다음 주기에 처리된다
// (README 의 "스케줄 실행" 참고). Ctrl+C 로 종료.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	statemachine "github.com/jaehafe/payment-consistency-lab/go/07-payment-state-machine"
)

func main() {
	// signal.NotifyContext: Ctrl+C(SIGINT)/SIGTERM 이 오면 ctx 가 취소된다.
	// Spring 의 graceful shutdown 처럼 janitor 루프가 끝나고 main 이 반환한다.
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Kotlin 과 같은 DB 를 써도 테이블이 섞이지 않게 별도 스키마(go_07)
	dsn := getenv("DATABASE_URL",
		"postgres://lab:lab@localhost:15432/lab?sslmode=disable") +
		"&search_path=go_07"
	// time.ParseDuration: "10s", "500ms" 같은 문자열 → Duration
	// (Spring 이 "10s" 를 Duration 으로 바인딩하는 것과 같다)
	interval, err := time.ParseDuration(getenv("JANITOR_INTERVAL", "10s"))
	if err != nil || interval <= 0 {
		log.Fatalf("JANITOR_INTERVAL 이 잘못됨: %q",
			os.Getenv("JANITOR_INTERVAL"))
	}

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx,
		`CREATE SCHEMA IF NOT EXISTS go_07`); err != nil {
		log.Fatal(err)
	}
	if err := statemachine.ApplySchema(ctx, db); err != nil {
		log.Fatal(err)
	}

	// Fake PG 는 메모리에만 기록을 들고 있다. 이 프로세스는 결제를
	// 승인하지 않으므로, 밖에서 넣은 UNKNOWN 은 PG 조회 시 NOT_FOUND
	// → grace 안이면 재예약, 지났으면 FAILED 가 된다.
	j := &statemachine.Janitor{
		DB:            db,
		Gateway:       statemachine.NewFakePaymentGateway(),
		MaxAttempts:   5,
		BaseBackoff:   10 * time.Second,
		NotFoundGrace: 5 * time.Minute,
	}
	log.Printf("janitor started (interval=%s, schema=go_07)", interval)
	j.Run(ctx, interval) // ctx 가 취소될 때까지 블록
	log.Print("janitor stopped")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
