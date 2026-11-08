# Go 구현

`docs/SPEC.md` 의 계약(스키마, 시나리오, 테스트 이름)을 Go 로 구현한 것. Kotlin 구현(`../kotlin`)과 나란히 놓고 비교하며 읽도록 만들었다.
각 단계 디렉토리는 **혼자 읽어도 이해되도록** 작은 공용 조각(Fake PG, 이벤트 구조체, Publisher)을 일부러 복사해 두었다.

## 실행

필요한 것: Go 1.26+, Docker (테스트가 Testcontainers 로 Postgres 16 / Redpanda 를 자동 기동).

```bash
cd go
go vet ./...
go test ./...                      # 전체 (처음엔 이미지 pull 때문에 오래 걸림)
go test ./03-outbox-polling/ -v    # 단계 하나, 로그와 함께
go test ./02-idempotency-key/ -v -run Test_concurrent_same_key__only_one_charge   # 테스트 하나
```

테스트 이름은 SPEC 의 Kotlin 테스트 이름에 `Test_` 를 붙인 것이다.
(`commit_then_publish__broker_failure__payment_saved_but_event_lost` → `Test_commit_then_publish__broker_failure__payment_saved_but_event_lost`)

## 모듈 구성

모듈: `github.com/jaehafe/payment-consistency-lab/go`

| 디렉토리 | 패키지 | 내용 | 주요 파일 |
|---|---|---|---|
| `01-dual-write-problem` | `dualwrite` | DB + Kafka 따로 쓰기의 3가지 실패 재현 | `dual_write.go`, `publisher.go` |
| `02-idempotency-key` | `idempotency` | 멱등키 + recovery point, `net/http` 서버 | `service.go`, `handler.go`, `gateway.go`, `cmd/main.go` |
| `03-outbox-polling` | `outboxpolling` | Transactional Outbox + `SKIP LOCKED` 폴링 릴레이 | `payment_service.go`, `outbox_relay.go`, `cmd/main.go` |
| `04-outbox-cdc` | `outboxcdc` | Debezium Outbox Event Router 용 outbox | `payment_service.go`, `cmd/main.go` |
| `05-idempotent-consumer` | `idempotentconsumer` | `processed_messages` 기반 멱등 컨슈머 | `consumer.go` |

각 단계 공통: `schema.sql`(`//go:embed` 로 `schema.go` 에 포함), `*_test.go`(테스트), `infra_test.go`(Testcontainers 기동), `README.md`.

디렉토리 이름에는 `-` 와 숫자가 들어가서 Go 식별자가 될 수 없으므로 패키지 이름은 따로 짓는다.
import 할 때는 별칭을 붙인다: `outboxpolling "github.com/jaehafe/payment-consistency-lab/go/03-outbox-polling"`.

### 라이브러리

| 용도 | 라이브러리 |
|---|---|
| Postgres | `github.com/jackc/pgx/v5` (`pgxpool`) — ORM 없이 SQL 직접, 트랜잭션 경계가 코드에 그대로 보이게 |
| Kafka | `github.com/twmb/franz-go` (`kgo`, `kadm`) |
| 테스트 인프라 | `github.com/testcontainers/testcontainers-go` + `modules/postgres`, `modules/redpanda` |
| HTTP | 표준 라이브러리 `net/http` (Go 1.22+ 라우팅 패턴 `"POST /payments"`) |

### 수동 실행 (`infra/docker-compose.yml`)

- `go run ./02-idempotency-key/cmd` — `:8081` HTTP 서버 (스키마 `go_02`)
- `go run ./03-outbox-polling/cmd` — 결제 3건 → 릴레이 → 수신 출력 (스키마 `go_03`, 토픽 `payment.events.go03`)
- `go run ./04-outbox-cdc/cmd` — Debezium E2E (스키마 `public`, 토픽 `payment.events`). 절차는 `04-outbox-cdc/README.md`

02/03 이 별도 스키마를 쓰는 이유: 같은 DB 에서 03 의 `outbox`(폴링용 컬럼)와 04 의 `outbox`(Debezium 컬럼)가 충돌하지 않게, 그리고 Kotlin 앱과 테이블이 섞이지 않게.
환경 변수 `DATABASE_URL`(기본 `postgres://lab:lab@localhost:15432/lab?sslmode=disable`), `KAFKA_BROKERS`(기본 `localhost:19092`)로 바꿀 수 있다.

---

## Go 문법 빠르게 읽기 (Kotlin 개발자용)

| Go | Kotlin 으로 치면 | 메모 |
|---|---|---|
| `x := 1` | `val x = 1` | 선언 + 대입 + 타입 추론. 이미 있는 변수에 다시 넣을 땐 `=` |
| `var x int` | `var x: Int = 0` | 선언만 하면 "제로 값"(0, "", nil, false)으로 초기화 |
| `func f(a string) (int, error)` | `fun f(a: String): Pair<Int, Error?>` | **반환값 여러 개**. 마지막은 보통 `error` |
| `if err != nil { return err }` | `?: return` / 예외 throw 대신 | Go 에는 예외가 없다. 에러는 값이고, 매번 확인해서 위로 돌려준다 |
| `fmt.Errorf("저장 실패: %w", err)` | `RuntimeException("저장 실패", cause)` | `%w` 로 감싸면 `errors.Is(err, ErrX)` 로 원인을 찾을 수 있다 |
| `defer tx.Rollback(ctx)` | `try { } finally { rollback() }` | 함수가 끝날 때 실행. 역순으로 실행된다 |
| `type Foo struct { A int }` | `class Foo(var a: Int)` | 필드만. 메서드는 밖에서 붙인다 |
| `func (f *Foo) Bar() {}` | `fun Foo.bar() {}` / 멤버 함수 | `(f *Foo)` = **리시버**. `f` 가 `this` |
| `*Foo`, `&Foo{...}` | 객체 참조 | 포인터. `&` 로 주소를 얻고, 메서드/필드 접근은 `.` 그대로 |
| `type I interface { M() }` | `interface I { fun m() }` | 구현체가 `implements` 를 **선언하지 않는다**. 메서드만 맞으면 자동으로 만족 |
| 대문자 `Foo` / 소문자 `foo` | `public` / `private`(패키지 내부) | 이름의 첫 글자가 가시성을 결정 |
| `[]int`, `append(s, x)` | `MutableList<Int>`, `add` | 슬라이스. `make([]int, n)` 은 길이 n |
| `map[string]int{}` | `mutableMapOf<String, Int>()` | `v, ok := m[k]` 로 존재 여부까지 |
| `for i, v := range xs {}` | `xs.forEachIndexed { i, v -> }` | `for {}` 는 `while (true)` |
| `switch { case a > 1: ... }` | `when { a > 1 -> ... }` | case 는 자동 break |
| `ctx context.Context` | `CoroutineContext` + `Job` 취소 | 취소/타임아웃 신호를 함수 체인으로 전달. 첫 인자로 넘기는 게 관례 |
| `context.WithTimeout(ctx, 5*time.Second)` | `withTimeout(5000) { }` | `cancel()` 을 꼭 `defer` 로 호출 |
| `go f()` | `launch { f() }` | goroutine = 경량 스레드 |
| `sync.WaitGroup` (`Add`/`Done`/`Wait`) | `coroutineScope { }` / `joinAll()` | goroutine 여러 개 끝날 때까지 대기 |
| `ch := make(chan T)`, `<-ch`, `close(ch)` | `Channel<T>`, `receive()`, `close()` | `close` 하면 기다리던 모두가 깨어남 (테스트의 "동시 출발" 신호) |
| `select { case <-a: case <-b: }` | `select { a.onReceive {} ... }` | 먼저 준비된 채널 처리 |
| `sync.Mutex`, `atomic.Bool` | `synchronized`, `AtomicBoolean` | |
| `` `json:"orderId"` `` | `@JsonProperty("orderId")` | struct tag |
| `//go:embed schema.sql` | `resources/` 파일 읽기 | 컴파일 시 파일을 변수에 박아 넣음 |
| `_` | `_` (안 쓰는 값) | `_, err := f()` |
| `xs...` | `*array` (spread) | 슬라이스를 가변 인자로 펼침 |

### pgx 트랜잭션 패턴 (모든 단계에서 반복)

```go
tx, err := db.Begin(ctx)          // Kotlin: transactionTemplate.execute { ... } 의 시작
if err != nil { return err }
defer tx.Rollback(ctx)            // 중간에 return 하면 롤백. Commit 성공 후 호출되면 아무 일도 안 함

if _, err := tx.Exec(ctx, `INSERT ...`, a, b); err != nil {
    return err                    // → defer 가 롤백
}
return tx.Commit(ctx)             // 여기까지 와야 커밋
```

### 테스트 읽는 법

- `func TestXxx(t *testing.T)` 는 자동으로 발견되는 테스트. `t.Fatalf(...)` = 실패 후 즉시 중단(`fail()`), `t.Logf` = 로그 (`-v` 로 보임).
- `TestMain(m *testing.M)` = 패키지 전체의 `@BeforeAll/@AfterAll`. 여기서 컨테이너를 한 번 띄우고 테스트끼리 공유한다.
- `t.Cleanup(f)` = 테스트 끝나면 `f` 실행. `t.Helper()` = 실패 위치를 호출한 쪽 줄로 표시.
