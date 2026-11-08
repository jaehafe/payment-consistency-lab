// 02-idempotency-key 를 HTTP 서버로 직접 띄워 curl 로 실습한다.
//
//	docker compose -f ../infra/docker-compose.yml up -d postgres
//	go run ./02-idempotency-key/cmd
//	curl -i -X POST localhost:8081/payments -H 'Idempotency-Key: k1' -d '{"orderId":"o-1","amount":10000}'
//
// package main + func main() = 실행 파일의 진입점 (Kotlin의 fun main()).
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	idempotency "github.com/jaehafe/payment-consistency-lab/go/02-idempotency-key" // 디렉토리 이름에 '-' 가 있어서 별칭을 붙여 import
)

func main() {
	ctx := context.Background()
	// Kotlin 쪽 구현과 같은 DB 를 쓰더라도 테이블이 섞이지 않게 별도 스키마(go_02)를 쓴다.
	dsn := getenv("DATABASE_URL", "postgres://lab:lab@localhost:15432/lab?sslmode=disable") + "&search_path=go_02"

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal(err) // log.Fatal = 로그 출력 후 프로세스 종료
	}
	defer db.Close()
	if _, err := db.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS go_02`); err != nil {
		log.Fatal(err)
	}
	if err := idempotency.ApplySchema(ctx, db); err != nil {
		log.Fatal(err)
	}

	svc := &idempotency.PaymentService{DB: db, Gateway: idempotency.NewFakePaymentGateway()}
	addr := ":" + getenv("PORT", "8081") // Kotlin(Spring) 앱이 보통 8080 을 쓰므로 Go 는 8081
	log.Printf("listening on %s  (POST /payments)", addr)
	log.Fatal(http.ListenAndServe(addr, idempotency.NewHandler(svc)))
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
