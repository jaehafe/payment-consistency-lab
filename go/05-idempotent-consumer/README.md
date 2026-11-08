# 05. 멱등 컨슈머 (Idempotent Consumer)

## 문제

03/04 의 outbox 는 **at-least-once** 다. 같은 이벤트가 두 번 올 수 있다. 컨슈머 쪽에서도:
- DB 처리 후 offset 커밋 전에 죽으면 → 재시작 후 같은 메시지를 다시 받는다.
- 반대로 offset 을 먼저 커밋하고 DB 처리 중 죽으면 → 메시지가 **유실**된다 (at-most-once). 결제 시스템에서는 더 나쁘다.

`orders.status = 'PAID'` 같은 덮어쓰기는 여러 번 해도 결과가 같지만(자연적 멱등),
**포인트 적립(`point_ledger` INSERT)** 처럼 할 때마다 쌓이는 부작용은 중복 처리되면 그대로 두 배가 된다.

## 해결

1. **offset 은 DB 커밋 "후에" 커밋한다** → 유실 대신 중복을 택한다 (at-least-once).
2. 중복은 **`processed_messages` 테이블로 걸러낸다**: 비즈니스 처리와 **같은 트랜잭션**에서
   `INSERT INTO processed_messages (consumer_group, message_id) ... ON CONFLICT DO NOTHING` →
   영향 행이 0 이면 이미 처리한 메시지이므로 건너뛴다.

```
IdempotentConsumer.Handle   tx { INSERT processed_messages ON CONFLICT DO NOTHING
                                 └ 0행 → skip
                                 UPDATE orders; INSERT point_ledger }
ConsumerLoop.PollOnce       poll → Handle(각 레코드) → (DB 커밋 끝난 뒤) CommitRecords
```

결과적으로 "전달은 at-least-once, 효과는 exactly-once" 가 된다.

3. **계속 실패하는 메시지(poison message)는 DLT 로 치운다** (`ConsumerLoop.DeadLetter` 를 설정했을 때).
   실패한 메시지를 건너뛰면 유실이고, 무한 재시도하면 그 파티션이 멈춘다. 그래서 횟수를 세다가 넘기면
   **다른 토픽(`<topic>-dlt`)에 옮겨 적고 나서** 넘어간다. 단, **메시지 탓인 실패만** 센다 —
   DB 가 3초 내려갔다고 멀쩡한 결제 이벤트를 DLT 로 보내면 안 되기 때문이다 (DLT 를 되돌리는 도구는 없다).

```
Handle 실패 ─ deadLetterOrRetry ─ DeadLetter.failed ─ classify(err 체인)
   ├ transient (DB·네트워크 장애)             → 세지 않음, rewindFrom → Run 이 백오프(1s,2s,4s…30s) 후 다시 (끝없이)
   ├ retryable, 횟수 < MaxAttempts           → rewindFrom (그 메시지부터 다시, 횟수 +1)          ← 기존 흐름
   ├ retryable 횟수 도달 or not-retryable    → DLT 발행 ─ ack ─→ continue (다음 레코드)
   │                                                        └ 나중에 CommitRecords 가 이 offset 을 넘어 커밋
   └ DLT 발행 실패                            → rewindFrom (커밋 안 함, 다음 poll 에서 다시)
```

- **에러 분류** (`errclass.go` `classify`. `errors.Is/As` 가 `%w` 로 감싼 원인까지 보므로 Spring 의 `setTraverseCauses(true)` 와 같다):

  | 종류 | 에러 | 처리 |
  |---|---|---|
  | not-retryable | `ErrMissingMessageID` | 1번 만에 DLT |
  | transient | `*pgconn.ConnectError`, `pgconn.SafeToRetry`/`pgconn.Timeout`, `*pgconn.PgError` 중 SQLSTATE `08xxx`·`53xxx`·`57P01~03`·`57014`·`40001`·`40P01`, `net.Error`, `context.DeadlineExceeded`/`Canceled`, `io.EOF`/`io.ErrUnexpectedEOF` | **DLT 안 감, 횟수 안 셈**, 지수 백오프로 무한 재시도 |
  | retryable | 깨진 JSON(`decode payload: ...`), 검증 실패, 결정적 DB 에러(`23xxx` 제약 위반 등), **그 밖의 모든 에러** | `MaxAttempts` 번째에 DLT |

  - DB 가 내려갔을 때 `pgxpool.Begin` 이 실제로 돌려주는 건 `*pgconn.ConnectError`(← `*net.OpError` connection refused)다.
    테스트도 죽은 포트(`127.0.0.1:1`)를 가리키는 풀로 이 진짜 에러를 만든다.
  - **모르는 에러 = retryable (기본값)**. Spring `ExceptionClassifier` 의 기본값(`defaultValue=true`, 모르는 건 재시도 후 recoverer)과 같은 선택이다.
    transient 로 두면 payload 하나에서만 터지는 버그가 파티션을 영원히 멈춘다 — DLT 가 막으려던 바로 그 상황.
    retryable 이면 최악이 "분류 못 한 인프라 에러로 멀쩡한 메시지가 DLT 에 감" 인데, DLT 에 원본과 원래 위치가 남으니 유실은 아니다.
    그런 에러를 발견하면 `isTransient` 에 추가한다.
  - Spring 과 다른 점: Spring 은 `DeserializationException` 을 not-retryable 로 두지만, 여기서는 JSON 디코딩이 핸들러 안에 있으므로
    깨진 JSON 은 retryable (`MaxAttempts` 번 시도 후 DLT). 결과는 같고 시도가 두 번 더 많을 뿐이다.
- **재시도 간격 — 지수 백오프**: `Run(ctx, retryDelay)` 이 연속 실패마다 `retryDelay` × 2ⁿ 을 기다리고 `ConsumerLoop.MaxRetryDelay`
  (0 이면 `DefaultMaxRetryDelay` = 30s) 에서 멈춘다. `PollOnce` 가 성공하면 `retryDelay` 로 돌아간다. (Spring 이라면 `setBackOffFunction` 으로
  예외별 `ExponentialBackOff` 를 주는 것과 같다 — [`FailedRecordTracker.java#L114-L116`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L114-L116))
- **`MaxAttempts` 는 컨슈머 인스턴스별이다**: 횟수는 이 프로세스의 `DeadLetter` 메모리에만 있다. **재시작하거나 리밸런스로 파티션이
  다른 인스턴스로 넘어가면 0 부터 다시 센다.** 넘어간 파티션의 기록은 `DeadLetter.ForgetPartitions` 가 지운다. 시그니처가 franz-go 콜백과
  같으므로 그룹 클라이언트를 만들 때 건다 (`ConsumerLoop` 는 이미 만들어진 `*kgo.Client` 를 받으므로 대신 걸 수 없다):

  ```go
  dead, err := NewDeadLetter(producer, 3, group)       // 설정 검증
  cl, err := kgo.NewClient(..., kgo.DisableAutoCommit(),
      kgo.OnPartitionsRevoked(dead.ForgetPartitions),
      kgo.OnPartitionsLost(dead.ForgetPartitions))
  loop := &ConsumerLoop{Client: cl, Handler: h, DeadLetter: dead}
  ```

  이 콜백은 franz-go 내부 고루틴에서 불리므로 `attempts` map 은 `sync.Mutex` 로 지킨다. Go 의 `ConsumerLoop` 는 실패해도 클라이언트를
  닫지 않으므로(`SetOffsets` 로 되감기) 재시도 중에 이 훅이 불려 횟수가 리셋되는 일은 없다 (Kotlin 은 컨슈머를 재생성하므로 `close()` 중 revoke 를 무시한다).
- **설정 검증**: `NewDeadLetter` 가 `Producer == nil`(첫 DLT 발행에서 panic) 과 `MaxAttempts <= 0`(첫 실패에 곧장 DLT) 을
  `ErrInvalidDeadLetter` 로 거부한다. 구조체 리터럴로 만들었다면 `PollOnce`/`Run` 이 시작할 때 같은 검사를 한다 (poll 하기 전에 에러).

- **이름**: `payment.events-dlt`. Spring Kafka 3.3+ 의 기본값(`<topic>-dlt`)을 그대로 따랐다.
  원래 토픽에서 기계적으로 만들어지므로 테스트·e2e 처럼 토픽이 여러 개여도 설정이 필요 없고,
  나중에 Spring Kafka 로 옮겨도 DLT 이름이 바뀌지 않는다. (3.2 까지는 `.DLT` 였다 — 아래 비교 참고)
- **DLT 레코드**: 원래 key·value·헤더(`message_id` 포함)를 그대로 두고 아래 헤더를 덧붙인다. 이름은 Spring 과 같다.

  | 헤더 | 값 |
  |---|---|
  | `kafka_dlt-original-topic` / `-partition` / `-offset` | 원래 위치 → 고친 뒤 원래 토픽으로 다시 보낼 때, 또는 원본을 찾을 때 |
  | `kafka_dlt-original-consumer-group` | 실패한 컨슈머 그룹 |
  | `kafka_dlt-exception-fqcn` | 에러 종류. Go 에는 예외 클래스가 없어서 sentinel 이면 그 이름(`idempotentconsumer.ErrMissingMessageID`), 아니면 `%T` |
  | `kafka_dlt-exception-message` | `err.Error()` |
  | `kafka_dlt-attempts` | 시도 횟수 (lab 전용 — Spring 은 DLT 에 넣지 않는다) |

  값은 전부 UTF-8 문자열이다 (`rpk` 로 바로 읽히게). Spring 은 partition/offset 을 4/8바이트 big-endian 으로 넣으므로,
  Spring 컨슈머가 이 DLT 를 읽는다면 그 둘은 직접 파싱해야 한다.
- **순서 — DLT ack 후에 커밋**: `ProduceSync` 로 브로커 ack 를 받은 **뒤에만** 그 메시지를 커밋 대상에 넣는다.
  커밋을 먼저 하고 DLT 발행이 실패하면, 메시지는 원래 토픽에서도(이미 지나감) DLT 에서도(안 들어감) 사라진다 = 유실.
  이 순서면 최악이 "DLT 에 두 번" 이다 (ack 후 커밋 전에 죽으면 다시 받아 다시 실패 → 다시 DLT).

## 핵심 코드 위치

- `consumer.go:31` `NaiveConsumer.Handle` — 받을 때마다 처리 (중복 시 포인트 2배)
- `consumer.go:60` `IdempotentConsumer.Handle` — `:62` `message_id` 헤더 누락 시 `ErrMissingMessageID`, `:85` `ON CONFLICT DO NOTHING`, `:89` `RowsAffected() == 0` 이면 skip
- `consumer.go:107` `applyPayment` — 자연적으로 멱등한 처리(orders) vs 아닌 처리(point_ledger)
- `consumer.go:152` `ConsumerLoop.PollOnce` — `:155` `DeadLetter.Validate`, `:175` 실패 시 `deadLetterOrRetry`, `:176` DLT ack 받았으면 `continue`, `:195` **DB 커밋 → offset 커밋 순서**, `:202` `CommitRecords`
- `consumer.go:219` `deadLetterOrRetry` — transient 재시도 / retryable 재시도 / DLT / DLT 실패 (Spring `DefaultErrorHandler` 역할)
- `consumer.go:262` `rewindFrom` — 처리(또는 fetch) 실패 시 먼저 `SetOffsets` 로 실패한 메시지부터 다시 받게 되감고, 그다음 실패한 메시지 **앞까지만** 커밋 (커밋이 실패해도 되감기는 이미 끝나 있다)
- `consumer.go:303` `Run` — 백그라운드 루프 (Kotlin `ConsumerRunner`). 실패하면 지수 백오프로 쉬고 같은 클라이언트로 재시도, 잘못된 설정이면 바로 에러
- `errclass.go:64` `classify` — not-retryable → transient → retryable(기본값), `:76` `isTransient`, `:108` SQLSTATE, `:133` `retryBackoff`
- `deadletter.go:34` `DLTTopic` — `<topic>-dlt`
- `deadletter.go:59` `NewDeadLetter` / `:74` `Validate` — `Producer != nil`, `MaxAttempts >= 1`
- `deadletter.go:102` `failed` — 분류 후 (토픽, 파티션, offset) 별 실패 횟수를 메모리 map 에 센다 (transient 는 안 셈)
- `deadletter.go:149` `ForgetPartitions` — `kgo.OnPartitionsRevoked`/`OnPartitionsLost` 에 거는 리밸런스 훅
- `deadletter.go:172` `publish` — `:178` 원래 헤더 복사 + `kafka_dlt-*` 헤더, `:197` `ProduceSync` 로 ack 까지 대기
- `infra_test.go` `newGroupClient` — `kgo.DisableAutoCommit()` (자동 커밋을 끄고 직접 커밋)

## 테스트 실행

```bash
cd go
go test ./05-idempotent-consumer/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_naive_consumer__duplicate_message__points_doubled` | 같은 `message_id` 메시지 2건 → 100점이어야 할 포인트가 200점 |
| `Test_idempotent_consumer__duplicate_message__processed_once` | 같은 메시지 2건 → 1건 처리 + 1건 스킵, 포인트 100점 |
| `Test_idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped` | 컨슈머 A 가 DB 커밋 후 offset 커밋 전에 종료 → 같은 그룹의 B 가 같은 메시지를 **실제로 다시 받고** 스킵, 이후 offset=1 커밋 |
| `Test_idempotent_consumer__handler_failure__offset_not_committed_past_failed_message` | 메시지 처리 실패 후 같은 클라이언트로 계속 poll 해도, 뒤에 온 메시지를 처리하고 실패한 메시지 offset 을 넘어 커밋하지 않는다. 실패한 메시지부터 다시 받아 장애가 풀리면 둘 다 처리, offset=2 |
| `Test_consumer_loop__missing_message_id_header__fails_with_clear_error` | (DLT 없음) `message_id` 헤더 없는 메시지 → `ErrMissingMessageID`(토픽-파티션@offset 포함), 처리·커밋 안 함 |
| `Test_dead_letter__always_failing_message__sent_to_dlt_after_max_attempts` | 항상 실패하는 X → 정확히 3번 시도 후 DLT. key·`message_id`·`kafka_dlt-*` 헤더 확인, 뒤의 Y 는 처리, 커밋 offset=2 (X 를 넘음) |
| `Test_dead_letter__missing_message_id__sent_to_dlt_without_retry` | `message_id` 없는 메시지 → 1번 만에 DLT (`attempts=1`, `exception-fqcn=idempotentconsumer.ErrMissingMessageID`), 뒤 메시지 처리, offset=2 |
| `Test_dead_letter__transient_failure__processed_without_dlt` | 한 번 실패 후 성공하는 메시지 → 정상 처리, DLT 비어 있음 |
| `Test_dead_letter__dlt_publish_fails__offset_not_committed_past_message` | DLT 프로듀서가 닫혀 발행 실패(`kgo.ErrClientClosed`) → offset 은 X 를 넘지 않고, Y 도 처리 안 됨. 프로듀서를 바꾸면 X 는 DLT, Y 처리, offset=2 |
| `Test_dead_letter__db_connection_error__retried_past_max_attempts_without_dlt` | 죽은 포트 풀로 진짜 `*pgconn.ConnectError` 를 5번(> MaxAttempts 3) → 전부 transient, 횟수 0, DLT 비어 있음, X·Y 처리, offset=2. 리밸런스 훅을 건 클라이언트로 돈다 |
| `Test_dead_letter__malformed_json__sent_to_dlt_after_max_attempts` | 깨진 JSON → 3번 시도 후 DLT (원래 value 유지, `decode payload`), Y 처리, offset=2 |
| `Test_new_dead_letter__invalid_config__rejected` | `Producer` nil, `MaxAttempts` 0/-1 → `ErrInvalidDeadLetter` |
| `Test_consumer_loop__invalid_dead_letter_literal__fails_before_polling` | 리터럴로 만든 잘못된 `DeadLetter` → `PollOnce`/`Run` 이 panic 없이 `ErrInvalidDeadLetter`, 아무것도 처리 안 함 |
| `Test_classify__errors` | 분류표 (SQLSTATE, `net.OpError`, ctx 타임아웃, 깨진 JSON, 모르는 에러) + 실제 연결 실패 |
| `Test_retry_backoff__doubles_up_to_cap` | 1s → 2s → 4s → … → 30s 상한 (오버플로 없음) |
| `Test_dead_letter__partitions_revoked__attempts_forgotten` | `ForgetPartitions` 는 빠진 파티션 기록만 지운다 |

DLT 테스트는 `fixture` 마다 새 토픽 + `<topic>-dlt` 를 만든다. DLT 발행 실패는 별도 훅 없이 **닫힌 클라이언트**로 만든다.

중복 메시지는 03 에서 본 "릴레이가 발행 후 마킹 전에 죽음" 상황처럼 같은 `message_id` 로 두 번 produce 해서 만든다.

## 한계 / 다음 단계

- `processed_messages` 는 계속 쌓인다. 브로커 보관 기간(retention)보다 오래된 행은 지워도 된다.
- 이 방식은 **처리 결과가 같은 DB 에 있을 때**만 같은 트랜잭션으로 묶을 수 있다. 처리 중 외부 API 를 호출한다면
  그 API 에도 멱등키(02 단계)를 넘겨야 한다.
- `message_id` 는 프로듀서가 **재발행해도 같은 값**이어야 한다 (outbox.id 가 그 역할). 재발행 때마다 새 UUID 를 만들면 소용없다.
- **재시도 중에는 그 파티션이 멈춘다**: DLT 로 가기 전 `MaxAttempts` 번 동안, 그리고 **인프라 장애(transient) 동안은 무한히**
  뒤 메시지를 처리하지 않는다 (순서 보장 + "멀쩡한 메시지를 DLT 로 보내지 않음"의 대가). DB 장애가 길면 lag 이 쌓인다 — 알림은 lag 으로 건다.
  `DeadLetter` 를 안 주면 예전처럼 모든 실패를 무한 재시도.
- **`MaxAttempts` 는 컨슈머 인스턴스별, 메모리에만 있다**: 재시작하거나 리밸런스로 파티션이 다른 인스턴스로 넘어가면 0 부터 다시 센다 →
  한 메시지가 전체로는 `MaxAttempts` 보다 많이 시도될 수 있다. 정확히 세려면 DB/브로커에 저장해야 한다. (Spring 의 `FailedRecordTracker` 도 메모리다)
  `ForgetPartitions` 를 클라이언트에 걸지 않으면 넘어간 파티션의 기록이 map 에 남는다 (누수).
- **분류는 목록 기반이다**: 목록에 없는 인프라 에러는 retryable 로 떨어져 `MaxAttempts` 후 DLT 로 간다.
  DLT 에 원본이 남으므로 유실은 아니지만, 발견하면 `isTransient` 에 추가해야 한다.
- **DLT 발행도 at-least-once**: ack 후 커밋 전에 죽으면 같은 메시지가 DLT 에 두 번 들어간다. DLT 를 읽는 쪽은
  `kafka_dlt-original-topic/partition/offset` (또는 `message_id`) 로 중복을 걸러야 한다.
- **DLT 발행이 계속 실패하면** 다시 파티션 정지다 (유실보다 낫다). 그동안 시도 횟수는 계속 올라가고, 발행이 되살아나면
  그 횟수 그대로 DLT 에 적힌다. Spring 은 기본적으로 이때 횟수를 리셋하고 재시도 주기를 처음부터 다시 돈다.
- DLT 에 쌓인 메시지를 고쳐서 원래 토픽으로 되돌리는 도구(재처리)는 없다. 헤더에 원래 위치가 있으니 `rpk` 로 손으로 할 수 있다.
- 이 패키지에는 `cmd` 가 없어서 e2e(`e2e/05-idempotent-consumer.sh`)는 Kotlin 앱으로만 DLT 를 확인한다.
- 실패 시 되감기는 `kgo.Client.SetOffsets` 로 한다 (Kotlin 은 컨슈머를 새로 만든다). 그룹 리밸런스와 동시에 일어나는 경우는 다루지 않는다 —
  파티션이 다른 인스턴스로 넘어가면 그쪽은 마지막 커밋 offset(실패한 메시지)부터 읽으므로 유실은 없다.
- Kafka 트랜잭션(EOS)은 "Kafka → Kafka" 처리에서만 exactly-once 를 준다. 외부 DB 에 쓰는 컨슈머는 결국 이 패턴이 필요하다.

## Spring Kafka 와 비교 (DLT)

Spring Kafka 에서는 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` 를 컨테이너에 걸면 위 동작을 해 준다.
여기서는 의존성 없이 같은 동작을 손으로 짰다. 인용은 이 저장소가 쓰는 버전(Spring Boot 3.5.16 → spring-kafka **3.3.16**)의 태그 커밋
[`87c4b04`](https://github.com/spring-projects/spring-kafka/tree/87c4b047b658fa472201299e1cc4be7842c63c73) 기준이다.

| 개념 | Spring Kafka 3.3.16 | 여기 |
|---|---|---|
| DLT 이름 `<topic>-dlt`, 파티션은 원래 번호 | [`DeadLetterPublishingRecoverer.java#L79-L80`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L79-L80) — 3.2 까지는 `.DLT`, 3.3 에서 바뀜 ([GH-3291, `5529cbb`](https://github.com/spring-projects/spring-kafka/commit/5529cbb85b)) | `DLTTopic` = `<topic>-dlt`. 파티션은 key 로 정한다 (DLT 파티션 수가 달라도 됨) |
| 기본 시도 횟수 **10** (= 첫 시도 + 재시도 9, 간격 0) | [`SeekUtils.java#L57-L63`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/SeekUtils.java#L57-L63) `DEFAULT_MAX_FAILURES = 10`, `FixedBackOff(0, 9)` / [`DefaultErrorHandler.java#L62-L64`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DefaultErrorHandler.java#L62-L64) | `MaxAttempts` (테스트 3). 간격은 `Run` 의 지수 백오프 (`retryDelay` → `MaxRetryDelay` 30s) |
| 예외별 BackOff (인프라 장애는 끝없이) | [`FailedRecordTracker.java#L114-L116`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L114-L116) `setBackOffFunction`, [`#L216-L222`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L216-L222) `determineBackOff` | `classTransient` 는 횟수를 안 세고 `Run` 이 백오프로 무한 재시도 |
| 주의: 기본 생성자에는 DLT 가 없다 | 위 `DefaultErrorHandler()` 는 recoverer 가 `null` → 10번 실패하면 **로그만 남기고 건너뛴다**. DLT 를 원하면 `new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backOff)` | `DeadLetter` 가 nil 이면 건너뛰지 않고 무한 재시도 (유실보다 정지) |
| 실패 횟수 기록 (메모리) | [`FailedRecordTracker.java#L50`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L50) 스레드별 `Map<TopicPartition, FailedRecord>`, [`#L168-L195`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L168-L195) `recovered()` | `DeadLetter.attempts` map, `failed()` |
| 재시도할 가치가 없는 예외 | [`ExceptionClassifier.java#L59-L66`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/ExceptionClassifier.java#L59-L66) `DeserializationException`, `MessageConversionException`, `ClassCastException` 등 → 곧장 recoverer | `classify`: `ErrMissingMessageID` → `classNotRetryable` |
| 모르는 예외의 기본값 / 원인 체인 | [`ExceptionClassifier.java#L68-L71`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/ExceptionClassifier.java#L68-L71) 목록 밖은 `true`(재시도 후 recoverer), [`#L223-L228`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/ExceptionClassifier.java#L223-L228) `setTraverseCauses(true)` | 목록 밖은 `classRetryable`, `errors.Is/As` 로 `%w` 체인을 본다 |
| 재시도 = 실패한 레코드부터 seek, 복구됐으면 그 레코드는 seek 에서 뺀다 | [`SeekUtils.java#L104-L131`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/SeekUtils.java#L104-L131) `doSeeks` | `rewindFrom` / DLT 성공이면 `continue` |
| 원래 헤더 유지 | [`DeadLetterPublishingRecoverer.java#L529`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L529) `new RecordHeaders(record.headers().toArray())` | `publish` 에서 `rec.Headers` 복사 |
| 원래 topic/partition/offset 헤더 | [`DeadLetterPublishingRecoverer.java#L780-L788`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L780-L788) (partition/offset 은 `ByteBuffer` binary), 이름은 [`KafkaHeaders.java#L198-L216`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/support/KafkaHeaders.java#L198-L216) | 같은 이름, 값은 문자열 |
| 예외 헤더 | [`DeadLetterPublishingRecoverer.java#L812-L834`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L812-L834) fqcn, cause-fqcn, message, stacktrace / 이름 [`KafkaHeaders.java#L153-L171`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/support/KafkaHeaders.java#L153-L171) | fqcn, message 만 (+ lab 전용 `kafka_dlt-attempts`) |
| DLT ack 를 기다린 뒤에 넘어간다 | [`DeadLetterPublishingRecoverer.java#L108`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L108) `failIfSendResultIsError = true`, [`#L705-L706`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L705-L706) → [`#L726`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L726) `sendResult.get(timeout)`. 실패하면 예외 → 그 레코드는 다시 seek | `ProduceSync(...).FirstErr()`. 실패하면 `rewindFrom` |
| DLT 발행이 실패했을 때 실패 횟수 | [`FailedRecordTracker.java#L224-L237`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L224-L237) `resetStateOnRecoveryFailure` (기본 `true`, [`#L64`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L64)) → 리셋하고 재시도 주기를 처음부터 | **다름**: 리셋하지 않는다 → 다음 실패마다 바로 DLT 발행을 다시 시도 |

`main` 브랜치(4.x)에서는 헤더 처리가 `DeadLetterRecordManager` 로 옮겨졌지만 이름·기본값(`-dlt`, 10회)은 같다.
