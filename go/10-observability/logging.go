package observability

import (
	"context"
	"io"
	"log/slog"
	"sync"
)

// ---------------------------------------------------------------------------
// 구조화 로그 (log/slog, JSON)
//
// 목표: 한 요청에서 나온 모든 로그 줄에 request_id, payment_id, order_id, idempotency_key 가 자동으로 붙게 한다.
// 그래야 장애 때 `grep '"request_id":"abc"'` 한 번으로 그 요청의 전체 흐름을 볼 수 있다.
//
// Kotlin/Spring 에서는 MDC(스레드 로컬)에 넣으면 끝이다. Go 에는 스레드 로컬이 없으므로(goroutine 이 스레드를 옮겨 다님)
// context.Context 에 담아서 함수 호출 체인으로 넘기고, 로그 핸들러가 ctx 에서 꺼내 붙인다.
// ---------------------------------------------------------------------------

// ctxKey 는 context 에 값을 넣을 때 쓰는 키 타입.
// 문자열 키 대신 패키지 전용 타입을 쓰면 다른 패키지의 키와 절대 충돌하지 않는다 (Go 관례).
type ctxKey struct{}

// logFields 는 요청 하나에 누적되는 로그 필드 묶음 (= 요청 단위 MDC).
// 포인터로 context 에 넣어 두고 여러 곳에서 추가한다. goroutine 에서 동시에 접근할 수 있어 Mutex 로 보호.
type logFields struct {
	mu    sync.Mutex
	attrs []slog.Attr
}

// AddLogAttrs 는 ctx 의 로그 필드에 값을 추가하고 ctx 를 돌려준다. 항상 `ctx = AddLogAttrs(ctx, ...)` 로 쓴다.
//   - ctx 에 이미 필드 묶음이 있으면(HTTP 미들웨어가 만들어 둠) 거기에 추가 → 미들웨어의 access log 에도 보인다.
//   - 없으면 새 묶음을 만들어 context.WithValue 로 감싼 새 ctx 를 돌려준다.
//
// context.WithValue(parent, key, value): 부모 ctx 에 key=value 를 얹은 "새" ctx 를 만든다 (ctx 는 불변).
func AddLogAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	// ctx.Value(key).(*logFields) 는 "타입 단언": interface{} 값을 *logFields 로 꺼낸다.
	// 두 번째 값 ok 는 성공 여부 (Kotlin의 `as? LogFields`).
	f, ok := ctx.Value(ctxKey{}).(*logFields)
	if !ok {
		f = &logFields{}
		ctx = context.WithValue(ctx, ctxKey{}, f)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range attrs {
		f.set(a)
	}
	return ctx
}

// set 은 같은 키가 있으면 바꾸고 없으면 추가한다.
func (f *logFields) set(a slog.Attr) {
	for i := range f.attrs {
		if f.attrs[i].Key == a.Key {
			f.attrs[i] = a
			return
		}
	}
	f.attrs = append(f.attrs, a)
}

func (f *logFields) snapshot() []slog.Attr {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]slog.Attr(nil), f.attrs...) // 복사본 (로그 쓰는 중에 다른 goroutine 이 바꿔도 안전)
}

// contextHandler 는 slog.Handler 를 감싸서, 로그를 쓸 때 ctx 의 필드를 붙여준다 (데코레이터 패턴).
//
// 구조체 안에 타입 이름만 적으면(slog.Handler) "임베딩": 감싼 핸들러의 메서드(Enabled, WithAttrs ...)를
// 그대로 물려받는다. 필요한 Handle 만 다시 정의한다. Kotlin의 `class X(h: Handler) : Handler by h` 위임과 같다.
type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if f, ok := ctx.Value(ctxKey{}).(*logFields); ok {
		r.AddAttrs(f.snapshot()...)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs / WithGroup 도 감싼 채로 돌려줘야 logger.With(...) 로 만든 로거에서도 ctx 필드가 붙는다.
func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}

// NewLogger 는 JSON 한 줄 = 로그 한 건 으로 w 에 쓰는 로거를 만든다.
// 로그는 반드시 slog.InfoContext(ctx, ...) 처럼 ctx 를 넘기는 메서드로 남겨야 필드가 붙는다.
func NewLogger(w io.Writer) *slog.Logger {
	return slog.New(contextHandler{slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})})
}
