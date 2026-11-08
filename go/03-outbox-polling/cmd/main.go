// 03-outbox-polling 데모: 결제 몇 건 → 릴레이가 주기적으로 outbox 를 비워 Kafka 로 보냄 → 컨슈머가 출력.
//
//	docker compose -f ../infra/docker-compose.yml up -d postgres redpanda
//	go run ./03-outbox-polling/cmd
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	outboxpolling "github.com/jaehafe/payment-consistency-lab/go/03-outbox-polling"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 별도 스키마(go_03)를 써서 04(Debezium, public.outbox)와 테이블 구조가 충돌하지 않게 한다.
	dsn := getenv("DATABASE_URL", "postgres://lab:lab@localhost:15432/lab?sslmode=disable") + "&search_path=go_03"
	brokers := getenv("KAFKA_BROKERS", "localhost:19092")
	topic := outboxpolling.TopicPaymentEvents + ".go03" // 04 의 Debezium 토픽(payment.events)과 섞이지 않게

	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	_, err = db.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS go_03`)
	must(err)
	must(outboxpolling.ApplySchema(ctx, db))

	kc, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	must(err)
	defer kc.Close()
	if _, err := kadm.NewClient(kc).CreateTopic(ctx, 1, 1, nil, topic); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		must(err)
	}

	// 릴레이를 백그라운드 goroutine 으로 실행 (1초마다 폴링)
	relay := &outboxpolling.OutboxRelay{DB: db, Publisher: &outboxpolling.KafkaPublisher{Client: kc, Topic: topic}}
	go relay.Run(ctx, time.Second, 100)

	svc := &outboxpolling.PaymentService{DB: db, Gateway: outboxpolling.NewFakePaymentGateway()}
	for i := 0; i < 3; i++ {
		p, err := svc.Pay(ctx, fmt.Sprintf("order-%d", time.Now().UnixNano()), 10000)
		must(err)
		log.Printf("결제 저장 (+outbox): paymentId=%s orderId=%s", p.ID, p.OrderID)
	}

	// 컨슈머: 토픽 끝에서 3건을 기다린다.
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd().Relative(-3)))
	must(err)
	defer consumer.Close()
	for got := 0; got < 3; {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			log.Fatal("timeout: 메시지를 받지 못함")
		}
		fetches.EachRecord(func(r *kgo.Record) {
			got++
			log.Printf("수신 key=%s headers=%s value=%s", r.Key, fmtHeaders(r.Headers), r.Value)
		})
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
