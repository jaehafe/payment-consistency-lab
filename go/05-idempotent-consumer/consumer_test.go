package idempotentconsumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

func Test_naive_consumer__duplicate_message__points_doubled(t *testing.T) {
	f := newFixture(t)
	f.produce(t, "msg-1", 2) // 같은 이벤트가 두 번 도착 (at-least-once)

	cl := f.newGroupClient(t)
	defer cl.Close()
	loop := &ConsumerLoop{Client: cl, Handler: &NaiveConsumer{DB: testPool}}

	stats := pollUntil(t, loop, 2)
	if stats.Processed != 2 {
		t.Fatalf("나이브 컨슈머는 둘 다 처리한다: %+v", stats)
	}
	// orders 갱신은 덮어쓰기라 괜찮지만…
	if amt := queryInt(t, `SELECT paid_amount FROM orders WHERE id = 'order-1'`); amt != 10000 {
		t.Fatalf("paid_amount=10000 기대: %d", amt)
	}
	// 포인트는 두 번 쌓였다: 10000원 결제의 1% = 100점이어야 하는데 200점
	if pts := queryInt(t, `SELECT coalesce(sum(points), 0) FROM point_ledger`); pts != 200 {
		t.Fatalf("중복 적립으로 200점이 돼야 한다 (버그 재현): %d", pts)
	}
}

func Test_idempotent_consumer__duplicate_message__processed_once(t *testing.T) {
	f := newFixture(t)
	f.produce(t, "msg-1", 2)

	cl := f.newGroupClient(t)
	defer cl.Close()
	loop := &ConsumerLoop{Client: cl, Handler: &IdempotentConsumer{DB: testPool, Group: f.group}}

	stats := pollUntil(t, loop, 2)
	if stats.Processed != 1 || stats.Skipped != 1 {
		t.Fatalf("1건 처리 + 1건 스킵 기대: %+v", stats)
	}
	if pts := queryInt(t, `SELECT coalesce(sum(points), 0) FROM point_ledger`); pts != 100 {
		t.Fatalf("포인트는 100점이어야 한다: %d", pts)
	}
	if n := queryInt(t, `SELECT count(*) FROM processed_messages`); n != 1 {
		t.Fatalf("processed_messages 1행 기대: %d", n)
	}
}

func Test_idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped(t *testing.T) {
	f := newFixture(t)
	f.produce(t, "msg-1", 1) // 메시지는 딱 1번만 발행
	handler := &IdempotentConsumer{DB: testPool, Group: f.group}

	// 1) 컨슈머 A: DB 처리(커밋)까지 하고, offset 커밋 직전에 죽는다.
	clA := f.newGroupClient(t)
	loopA := &ConsumerLoop{Client: clA, Handler: handler, CrashBeforeOffsetCommit: true}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var statsA PollStats
	for statsA.Processed == 0 {
		var err error
		statsA, err = loopA.PollOnce(ctx)
		if err != nil && !errors.Is(err, ErrSimulatedCrash) {
			t.Fatal(err)
		}
	}
	clA.Close() // 프로세스 종료. offset 은 커밋되지 않았다.

	if pts := queryInt(t, `SELECT coalesce(sum(points), 0) FROM point_ledger`); pts != 100 {
		t.Fatalf("A 가 DB 처리는 끝냈다: %d", pts)
	}

	// 2) 컨슈머 B (같은 그룹, 재시작한 인스턴스): 커밋된 offset 이 없으니 같은 메시지를 "다시" 받는다.
	clB := f.newGroupClient(t)
	defer clB.Close()
	loopB := &ConsumerLoop{Client: clB, Handler: handler}
	statsB := pollUntil(t, loopB, 1)

	if statsB.Skipped != 1 || statsB.Processed != 0 {
		t.Fatalf("재전달된 메시지는 스킵돼야 한다: %+v", statsB)
	}
	if pts := queryInt(t, `SELECT coalesce(sum(points), 0) FROM point_ledger`); pts != 100 {
		t.Fatalf("포인트는 여전히 100점이어야 한다: %d", pts)
	}

	// B 는 스킵 후 offset 을 커밋했으므로, 그룹의 커밋 offset 이 1 (= 메시지 다음 위치)이 된다.
	offsets, err := kadm.NewClient(clB).FetchOffsets(ctx, f.group)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := offsets.Lookup(f.topic, 0); !ok || o.At != 1 {
		t.Fatalf("커밋된 offset 1 기대: %+v (ok=%v)", o, ok)
	}
}
