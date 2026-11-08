// 04-outbox-cdc E2E 데모: 결제 1건 저장(outbox insert) → Debezium 이 WAL 을 읽어 payment.events 로 발행 → 출력.
// 절차는 04-outbox-cdc/README.md 참고. (compose 의 postgres/redpanda/connect + 커넥터 등록이 필요)
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	outboxcdc "github.com/jaehafe/payment-consistency-lab/go/04-outbox-cdc"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Debezium 커넥터가 public.outbox 를 보므로 이 단계는 public 스키마를 쓴다.
	dsn := getenv("DATABASE_URL", "postgres://lab:lab@localhost:15432/lab?sslmode=disable")
	brokers := getenv("KAFKA_BROKERS", "localhost:19092")

	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	must(outboxcdc.ApplySchema(ctx, db))

	// 결제 저장 "전에" 토픽의 현재 끝 위치부터 읽기 시작한다 → 이번에 넣은 이벤트만 보게 된다.
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics(outboxcdc.TopicPaymentEvents),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.AllowAutoTopicCreation())
	must(err)
	defer consumer.Close()
	warmCtx, warmCancel := context.WithTimeout(ctx, 2*time.Second)
	consumer.PollFetches(warmCtx) // 시작 offset(현재 끝)을 확정하기 위한 짧은 poll
	warmCancel()

	svc := &outboxcdc.PaymentService{DB: db, Gateway: outboxcdc.NewFakePaymentGateway()}
	orderID := fmt.Sprintf("order-%d", time.Now().UnixNano())
	p, err := svc.Pay(ctx, orderID, 10000)
	must(err)
	log.Printf("결제 저장 (+outbox): paymentId=%s orderId=%s — 애플리케이션은 Kafka 를 호출하지 않았다", p.ID, orderID)

	for {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			log.Fatal("timeout: Debezium 커넥터가 등록돼 있는지 확인 (infra/debezium/register.sh)")
		}
		found := false
		fetches.EachRecord(func(r *kgo.Record) {
			log.Printf("Debezium 이 보낸 메시지: key=%s headers=%s value=%s", r.Key, fmtHeaders(r.Headers), r.Value)
			if string(r.Key) == orderID {
				found = true
			}
		})
		if found {
			return
		}
	}
}

// fmtHeaders 는 헤더 값([]byte)을 사람이 읽을 수 있는 문자열로 바꾼다.
func fmtHeaders(hs []kgo.RecordHeader) string {
	out := ""
	for _, h := range hs {
		out += h.Key + "=" + string(h.Value) + " "
	}
	return out
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
