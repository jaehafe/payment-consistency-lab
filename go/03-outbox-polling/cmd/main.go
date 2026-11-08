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
	"net/url"
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
	dsn := withSearchPath(getenv("DATABASE_URL", "postgres://lab:lab@localhost:15432/lab?sslmode=disable"), "go_03")
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
	mine := map[string]bool{} // 이번 실행이 만든 주문 (= Kafka key)
	for i := 0; i < 3; i++ {
		p, err := svc.Pay(ctx, fmt.Sprintf("order-%d", time.Now().UnixNano()), 10000)
		must(err)
		mine[p.OrderID] = true
		log.Printf("결제 저장 (+outbox): paymentId=%s orderId=%s", p.ID, p.OrderID)
	}

	// 컨슈머: 이번에 만든 3건이 토픽에 나타날 때까지 기다린다.
	// "끝에서 3건"(AtEnd().Relative(-3))으로 읽으면, 토픽에 이전 실행의 메시지가 있을 때 릴레이가 아직
	// 이번 결제를 보내기도 전에 옛 메시지 3건을 받고 끝나 버린다. 그래서 처음부터 읽되 key 로 골라낸다.
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	must(err)
	defer consumer.Close()
	for want, got := len(mine), 0; got < want; {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			log.Fatal("timeout: 메시지를 받지 못함")
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if !mine[string(r.Key)] {
				return
			}
			delete(mine, string(r.Key)) // 재발행(at-least-once)으로 같은 key 가 또 와도 한 번만 센다
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
