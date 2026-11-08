package idempotentconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// failFirstN 은 message_id 가 failID 인 메시지를 처음 n 번 실패시킨다.
// n < 0 이면 항상 실패 (poison message). calls 는 그 메시지를 받은 횟수.
// failID 가 "" 이면 message_id 헤더가 없는 메시지가 대상이다.
type failFirstN struct {
	inner  MessageHandler
	failID string
	n      int32
	calls  atomic.Int32
}

func (h *failFirstN) Handle(
	ctx context.Context, rec *kgo.Record,
) (bool, error) {
	if header(rec, "message_id") == h.failID {
		c := h.calls.Add(1) // 1, 2, 3, ...
		if h.n < 0 || c <= h.n {
			return false, errInjected
		}
	}
	return h.inner.Handle(ctx, rec)
}

const testMaxAttempts = 3

// dltFixture 는 newFixture + DLT 토픽 + DLT 로 보낼 프로듀서.
func dltFixture(t *testing.T) (fixture, *kgo.Client) {
	t.Helper()
	f := newFixture(t)
	createTopic(t, DLTTopic(f.topic))
	p, err := kgo.NewClient(kgo.SeedBrokers(testBrokers))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close) // 테스트가 끝나면 닫는다 (defer 의 테스트판)
	return f, p
}

// drive 는 cond 가 참이 될 때까지 PollOnce 를 반복한다.
// Run 과 같지만 재시도 대기는 없다. 처리 실패 에러는 "다음
// poll 에서 재시도" 라는 뜻이라 모아 두기만 한다.
func drive(
	t *testing.T, loop *ConsumerLoop, cond func(PollStats) bool,
) (PollStats, []error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second)
	defer cancel()
	var total PollStats
	var errs []error
	for !cond(total) {
		s, err := loop.PollOnce(ctx)
		if ctx.Err() != nil {
			t.Fatalf("20초 안에 조건 미충족: %+v, 에러 %d개 (마지막: %v)",
				total, len(errs), last(errs))
		}
		total.Processed += s.Processed
		total.Skipped += s.Skipped
		total.DeadLettered += s.DeadLettered
		if err != nil {
			errs = append(errs, err)
		}
	}
	return total, errs
}

func last(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return errs[len(errs)-1]
}

// dltRecords 는 DLT 토픽을 처음부터 읽어 want 건을 돌려준다.
func dltRecords(t *testing.T, f fixture, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testBrokers),
		kgo.ConsumeTopics(DLTTopic(f.topic)), // 그룹 없이 읽기만
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < want && ctx.Err() == nil {
		out = append(out, cl.PollFetches(ctx).Records()...)
	}
	if len(out) != want {
		t.Fatalf("DLT 레코드 %d건 기대: %d건", want, len(out))
	}
	return out
}

// dltSize 는 DLT 토픽에 쌓인 레코드 수 (끝 offset).
func dltSize(t *testing.T, cl *kgo.Client, f fixture) int64 {
	t.Helper()
	ends, err := kadm.NewClient(cl).ListEndOffsets(
		context.Background(), DLTTopic(f.topic))
	if err != nil {
		t.Fatal(err)
	}
	o, _ := ends.Lookup(DLTTopic(f.topic), 0)
	return o.Offset
}

func processedIDs(t *testing.T) string {
	t.Helper()
	var s string
	err := testPool.QueryRow(context.Background(), `
		SELECT coalesce(string_agg(message_id, ',' ORDER BY 1), '')
		FROM processed_messages`).Scan(&s)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func wantHeader(t *testing.T, rec *kgo.Record, key, want string) {
	t.Helper()
	if got := header(rec, key); got != want {
		t.Errorf("헤더 %s = %q, %q 기대", key, got, want)
	}
}

func Test_dead_letter__always_failing_message__sent_to_dlt_after_max_attempts(t *testing.T) {
	f, p := dltFixture(t)
	f.produce(t, "msg-x", 1) // offset 0: 항상 실패 (poison)
	f.produce(t, "msg-y", 1) // offset 1: 정상

	h := &failFirstN{
		inner:  &IdempotentConsumer{DB: testPool, Group: f.group},
		failID: "msg-x", n: -1,
	}
	cl := f.newGroupClient(t)
	defer cl.Close()
	loop := &ConsumerLoop{Client: cl, Handler: h, DeadLetter: &DeadLetter{
		Producer: p, MaxAttempts: testMaxAttempts, Group: f.group,
	}}

	stats, errs := drive(t, loop, func(s PollStats) bool {
		return s.Processed == 1
	})

	// msg-x 는 정확히 maxAttempts 번 시도됐고, 그 전 실패는 재시도였다
	if c := h.calls.Load(); c != testMaxAttempts {
		t.Fatalf("msg-x 시도 %d번 기대: %d", testMaxAttempts, c)
	}
	if len(errs) != testMaxAttempts-1 {
		t.Fatalf("재시도 에러 %d개 기대: %v", testMaxAttempts-1, errs)
	}
	if stats.DeadLettered != 1 {
		t.Fatalf("DLT 1건 기대: %+v", stats)
	}

	dl := dltRecords(t, f, 1)[0]
	if string(dl.Key) != "order-1" {
		t.Errorf("원래 key 유지 기대: %q", dl.Key)
	}
	wantHeader(t, dl, "message_id", "msg-x") // 원래 헤더 유지
	wantHeader(t, dl, HeaderDLTOriginalTopic, f.topic)
	wantHeader(t, dl, HeaderDLTOriginalPartition, "0")
	wantHeader(t, dl, HeaderDLTOriginalOffset, "0")
	wantHeader(t, dl, HeaderDLTOriginalGroup, f.group)
	wantHeader(t, dl, HeaderDLTAttempts, "3")
	if !strings.Contains(header(dl, HeaderDLTExceptionMessage),
		"injected") {
		t.Errorf("예외 메시지 헤더: %q",
			header(dl, HeaderDLTExceptionMessage))
	}
	if header(dl, HeaderDLTExceptionFQCN) == "" {
		t.Error("예외 타입 헤더가 비었다")
	}

	// 뒤의 msg-y 는 처리됐고, offset 은 x 를 넘어 2 까지 커밋
	if ids := processedIDs(t); ids != "msg-y" {
		t.Fatalf("msg-y 만 처리 기대: %q", ids)
	}
	if at := committedOffset(t, cl, f); at != 2 {
		t.Fatalf("커밋된 offset 2 기대: %d", at)
	}
}

func Test_dead_letter__missing_message_id__sent_to_dlt_without_retry(t *testing.T) {
	f, p := dltFixture(t)
	f.produce(t, "", 1)      // offset 0: message_id 헤더 없음
	f.produce(t, "msg-y", 1) // offset 1

	// failID "" + n 0 = 헤더 없는 메시지를 세기만 하고 실패 주입은 안 함
	h := &failFirstN{
		inner:  &IdempotentConsumer{DB: testPool, Group: f.group},
		failID: "",
	}
	cl := f.newGroupClient(t)
	defer cl.Close()
	loop := &ConsumerLoop{Client: cl, Handler: h, DeadLetter: &DeadLetter{
		Producer: p, MaxAttempts: testMaxAttempts, Group: f.group,
	}}

	_, errs := drive(t, loop, func(s PollStats) bool {
		return s.Processed == 1
	})
	if c := h.calls.Load(); c != 1 {
		t.Fatalf("재시도 없이 1번만 시도 기대: %d", c)
	}
	if len(errs) != 0 {
		t.Fatalf("재시도 에러가 없어야 한다: %v", errs)
	}

	dl := dltRecords(t, f, 1)[0]
	wantHeader(t, dl, HeaderDLTAttempts, "1")
	wantHeader(t, dl, HeaderDLTOriginalOffset, "0")
	wantHeader(t, dl, HeaderDLTExceptionFQCN,
		"idempotentconsumer.ErrMissingMessageID")
	if at := committedOffset(t, cl, f); at != 2 {
		t.Fatalf("커밋된 offset 2 기대: %d", at)
	}
}

func Test_dead_letter__transient_failure__processed_without_dlt(t *testing.T) {
	f, p := dltFixture(t)
	f.produce(t, "msg-x", 1) // 한 번 실패 후 성공
	f.produce(t, "msg-y", 1)

	h := &failFirstN{
		inner:  &IdempotentConsumer{DB: testPool, Group: f.group},
		failID: "msg-x", n: 1,
	}
	cl := f.newGroupClient(t)
	defer cl.Close()
	loop := &ConsumerLoop{Client: cl, Handler: h, DeadLetter: &DeadLetter{
		Producer: p, MaxAttempts: testMaxAttempts, Group: f.group,
	}}

	stats, _ := drive(t, loop, func(s PollStats) bool {
		return s.Processed == 2
	})
	if stats.DeadLettered != 0 || h.calls.Load() != 2 {
		t.Fatalf("x 2번 시도, DLT 0건 기대: %+v calls=%d",
			stats, h.calls.Load())
	}
	if ids := processedIDs(t); ids != "msg-x,msg-y" {
		t.Fatalf("둘 다 처리 기대: %q", ids)
	}
	if n := dltSize(t, cl, f); n != 0 {
		t.Fatalf("DLT 는 비어 있어야 한다: %d", n)
	}
	if at := committedOffset(t, cl, f); at != 2 {
		t.Fatalf("커밋된 offset 2 기대: %d", at)
	}
}

func Test_dead_letter__dlt_publish_fails__offset_not_committed_past_message(t *testing.T) {
	f, p := dltFixture(t)
	f.produce(t, "msg-x", 1)
	f.produce(t, "msg-y", 1)

	h := &failFirstN{
		inner:  &IdempotentConsumer{DB: testPool, Group: f.group},
		failID: "msg-x", n: -1,
	}
	cl := f.newGroupClient(t)
	defer cl.Close()
	dead := &DeadLetter{
		Producer: p, MaxAttempts: testMaxAttempts, Group: f.group,
	}
	loop := &ConsumerLoop{Client: cl, Handler: h, DeadLetter: dead}

	// 브로커 장애 대신: 닫힌 클라이언트로 발행하면 ErrClientClosed
	p.Close()
	ctx, cancel := context.WithTimeout(
		context.Background(), 20*time.Second)
	defer cancel()
	var pubErr error
	for pubErr == nil {
		_, err := loop.PollOnce(ctx)
		if ctx.Err() != nil {
			t.Fatalf("DLT 발행 실패 에러를 기대했다: %v", err)
		}
		if errors.Is(err, kgo.ErrClientClosed) {
			pubErr = err
		}
	}
	t.Logf("DLT 발행 실패: %v", pubErr)

	if at := committedOffset(t, cl, f); at > 0 {
		t.Fatalf("x(offset 0)를 넘어 커밋됐다 = 유실: %d", at)
	}
	if ids := processedIDs(t); ids != "" {
		t.Fatalf("뒤 메시지도 처리되면 안 된다: %q", ids)
	}
	if n := dltSize(t, cl, f); n != 0 {
		t.Fatalf("DLT 에 아무것도 없어야 한다: %d", n)
	}

	// 프로듀서가 복구되면 x 는 DLT 로, y 는 처리되고 offset 2
	p2, err := kgo.NewClient(kgo.SeedBrokers(testBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	dead.Producer = p2
	drive(t, loop, func(s PollStats) bool { return s.Processed == 1 })
	dltRecords(t, f, 1)
	if at := committedOffset(t, cl, f); at != 2 {
		t.Fatalf("커밋된 offset 2 기대: %d", at)
	}
}

// dbDownFirstN 은 message_id 가 failID 인 메시지를 처음 n 번은 "죽은 DB"
// 로 처리한다. down 은 아무도 듣지 않는 포트를 가리키는 풀이라 Begin 이
// 진짜 연결 에러(*pgconn.ConnectError ← net.OpError)를 낸다. 가짜 에러가
// 아니라 Postgres 가 잠깐 내려갔을 때와 같은 에러 체인이다.
type dbDownFirstN struct {
	up, down MessageHandler
	failID   string
	n        int32
	calls    atomic.Int32
	errs     []error // 실패한 시도의 에러 (분류 확인용)
}

func (h *dbDownFirstN) Handle(
	ctx context.Context, rec *kgo.Record,
) (bool, error) {
	if header(rec, "message_id") == h.failID && h.calls.Add(1) <= h.n {
		_, err := h.down.Handle(ctx, rec)
		h.errs = append(h.errs, err)
		return false, err
	}
	return h.up.Handle(ctx, rec)
}

// deadPool 은 연결이 항상 실패하는 풀 (127.0.0.1:1 에는 아무도 없다).
// pgxpool.New 는 바로 연결하지 않으므로 여기서는 에러가 안 난다.
func deadPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(),
		"postgres://lab:lab@127.0.0.1:1/lab?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func Test_dead_letter__db_connection_error__retried_past_max_attempts_without_dlt(t *testing.T) {
	f, p := dltFixture(t)
	f.produce(t, "msg-x", 1) // offset 0: DB 가 5번 연속 죽어 있다가 살아남
	f.produce(t, "msg-y", 1)

	const outages = 5 // > testMaxAttempts(3)
	h := &dbDownFirstN{
		up:     &IdempotentConsumer{DB: testPool, Group: f.group},
		down:   &IdempotentConsumer{DB: deadPool(t), Group: f.group},
		failID: "msg-x", n: outages,
	}
	dead, err := NewDeadLetter(p, testMaxAttempts, f.group)
	if err != nil {
		t.Fatal(err)
	}
	// 리밸런스 훅도 실제 앱처럼 건다 (재시도 중에 횟수를 지우지 않는지도 확인)
	cl := f.newGroupClient(t,
		kgo.OnPartitionsRevoked(dead.ForgetPartitions),
		kgo.OnPartitionsLost(dead.ForgetPartitions))
	defer cl.Close()
	loop := &ConsumerLoop{Client: cl, Handler: h, DeadLetter: dead}

	// x 가 처리되든 DLT 로 가든 두 건이 끝나면 멈춘다
	stats, errs := drive(t, loop, func(s PollStats) bool {
		return s.Processed+s.DeadLettered == 2
	})
	for _, err := range h.errs {
		t.Logf("주입된 DB 장애 (%s): %v", classify(err), err)
		if classify(err) != classTransient {
			t.Errorf("연결 에러는 transient 여야 한다: %v", err)
		}
	}
	if n := dead.attemptsOf(f.topic, 0, 0); n != 0 {
		t.Errorf("transient 실패는 세지 않는다: %d", n)
	}

	if stats.DeadLettered != 0 {
		t.Fatalf("DB 장애는 DLT 로 보내면 안 된다: %+v", stats)
	}
	if n := dltSize(t, cl, f); n != 0 {
		t.Fatalf("DLT 는 비어 있어야 한다: %d", n)
	}
	if c := h.calls.Load(); c != outages+1 {
		t.Fatalf("x 는 장애 %d번 + 성공 1번 = %d번 시도 기대: %d",
			outages, outages+1, c)
	}
	if len(errs) != outages {
		t.Fatalf("재시도 에러 %d개 기대: %d", outages, len(errs))
	}
	if ids := processedIDs(t); ids != "msg-x,msg-y" {
		t.Fatalf("둘 다 처리 기대: %q", ids)
	}
	if at := committedOffset(t, cl, f); at != 2 {
		t.Fatalf("커밋된 offset 2 기대: %d", at)
	}
}

// countCalls 는 message_id 가 id 인 메시지를 받은 횟수를 센다.
type countCalls struct {
	inner MessageHandler
	id    string
	calls atomic.Int32
}

func (h *countCalls) Handle(
	ctx context.Context, rec *kgo.Record,
) (bool, error) {
	if header(rec, "message_id") == h.id {
		h.calls.Add(1)
	}
	return h.inner.Handle(ctx, rec)
}

func Test_dead_letter__malformed_json__sent_to_dlt_after_max_attempts(t *testing.T) {
	f, p := dltFixture(t)
	f.produceRaw(t, "msg-bad", "{not json") // offset 0: 몇 번을 읽어도 깨져 있다
	f.produce(t, "msg-y", 1)                // offset 1

	h := &countCalls{
		inner: &IdempotentConsumer{DB: testPool, Group: f.group},
		id:    "msg-bad",
	}
	cl := f.newGroupClient(t)
	defer cl.Close()
	loop := &ConsumerLoop{Client: cl, Handler: h, DeadLetter: &DeadLetter{
		Producer: p, MaxAttempts: testMaxAttempts, Group: f.group,
	}}

	stats, _ := drive(t, loop, func(s PollStats) bool {
		return s.Processed+s.DeadLettered == 2
	})
	if stats.DeadLettered != 1 || h.calls.Load() != testMaxAttempts {
		t.Fatalf("깨진 JSON 은 %d번 시도 후 DLT 기대: %+v calls=%d",
			testMaxAttempts, stats, h.calls.Load())
	}
	dl := dltRecords(t, f, 1)[0]
	wantHeader(t, dl, "message_id", "msg-bad")
	wantHeader(t, dl, HeaderDLTAttempts, "3")
	if string(dl.Value) != "{not json" {
		t.Errorf("원래 value 유지 기대: %q", dl.Value)
	}
	if m := header(dl, HeaderDLTExceptionMessage); !strings.Contains(
		m, "decode payload") {
		t.Errorf("예외 메시지 헤더: %q", m)
	}
	if ids := processedIDs(t); ids != "msg-y" {
		t.Fatalf("msg-y 만 처리 기대: %q", ids)
	}
	if at := committedOffset(t, cl, f); at != 2 {
		t.Fatalf("커밋된 offset 2 기대: %d", at)
	}
}

// ---- 설정 검증 / 분류 / 백오프 / 리밸런스 (브로커 없이 도는 단위 테스트) ----

func Test_new_dead_letter__invalid_config__rejected(t *testing.T) {
	p, err := kgo.NewClient(kgo.SeedBrokers(testBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for name, tc := range map[string]struct {
		producer *kgo.Client
		max      int
	}{
		"nil producer":     {nil, 3},
		"zero attempts":    {p, 0},
		"negative attempt": {p, -1},
	} {
		if _, err := NewDeadLetter(tc.producer, tc.max, "g"); !errors.Is(
			err, ErrInvalidDeadLetter) {
			t.Errorf("%s: ErrInvalidDeadLetter 기대: %v", name, err)
		}
	}
	if d, err := NewDeadLetter(p, 1, "g"); err != nil || d == nil {
		t.Errorf("MaxAttempts=1 은 유효: %v", err)
	}
}

func Test_consumer_loop__invalid_dead_letter_literal__fails_before_polling(t *testing.T) {
	f := newFixture(t)
	f.produce(t, "msg-1", 1)
	cl := f.newGroupClient(t)
	defer cl.Close()
	// 리터럴로 만들면 NewDeadLetter 를 건너뛴다 → PollOnce/Run 이 잡는다
	loop := &ConsumerLoop{
		Client:     cl,
		Handler:    &IdempotentConsumer{DB: testPool, Group: f.group},
		DeadLetter: &DeadLetter{MaxAttempts: 3}, // Producer nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := loop.PollOnce(ctx); !errors.Is(err, ErrInvalidDeadLetter) {
		t.Fatalf("PollOnce: ErrInvalidDeadLetter 기대: %v", err)
	}
	if err := loop.Run(ctx, time.Second); !errors.Is(err, ErrInvalidDeadLetter) {
		t.Fatalf("Run: ErrInvalidDeadLetter 기대: %v", err)
	}
	if ids := processedIDs(t); ids != "" {
		t.Fatalf("아무것도 처리하면 안 된다: %q", ids)
	}
}

func Test_classify__errors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want errorClass
	}{
		{"message_id 없음", fmt.Errorf("%w: t-0@1", ErrMissingMessageID), classNotRetryable},
		{"깨진 JSON", fmt.Errorf("decode payload: %w", &json.SyntaxError{}), classRetryable},
		{"모르는 에러", errors.New("boom"), classRetryable},
		{"테스트 주입 에러", errInjected, classRetryable},
		{"제약 위반 23505", &pgconn.PgError{Code: "23505"}, classRetryable},
		{"admin_shutdown 57P01", fmt.Errorf("exec: %w", &pgconn.PgError{Code: "57P01"}), classTransient},
		{"connection_failure 08006", &pgconn.PgError{Code: "08006"}, classTransient},
		{"too_many_connections 53300", &pgconn.PgError{Code: "53300"}, classTransient},
		{"deadlock 40P01", &pgconn.PgError{Code: "40P01"}, classTransient},
		{"ctx 타임아웃", fmt.Errorf("begin: %w", context.DeadlineExceeded), classTransient},
		{"TCP 거부", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, classTransient},
		{"연결 끊김", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), classTransient},
	}
	for _, tc := range tests {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("%s: %s 기대, %s", tc.name, tc.want, got)
		}
	}
	// 진짜 연결 에러 (죽은 포트)
	_, err := deadPool(t).Begin(context.Background())
	if got := classify(err); got != classTransient {
		t.Errorf("실제 연결 실패(%v): transient 기대, %s", err, got)
	}
}

func Test_retry_backoff__doubles_up_to_cap(t *testing.T) {
	const max = 30 * time.Second
	want := map[int]time.Duration{
		1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second,
		5: 16 * time.Second, 6: max, 7: max, 1000: max,
	}
	for failures, w := range want {
		if got := retryBackoff(time.Second, max, failures); got != w {
			t.Errorf("failures=%d: %v 기대, %v", failures, w, got)
		}
	}
}

func Test_dead_letter__partitions_revoked__attempts_forgotten(t *testing.T) {
	d := &DeadLetter{MaxAttempts: 3}
	r0 := &kgo.Record{Topic: "t", Partition: 0, Offset: 7}
	r1 := &kgo.Record{Topic: "t", Partition: 1, Offset: 7}
	d.failed(r0, errInjected)
	d.failed(r1, errInjected)

	d.ForgetPartitions(context.Background(), nil, map[string][]int32{"t": {0}})

	if n := d.attemptsOf("t", 0, 7); n != 0 {
		t.Errorf("빠진 파티션 0 은 지워져야 한다: %d", n)
	}
	if n := d.attemptsOf("t", 1, 7); n != 1 {
		t.Errorf("남은 파티션 1 은 유지돼야 한다: %d", n)
	}
}
