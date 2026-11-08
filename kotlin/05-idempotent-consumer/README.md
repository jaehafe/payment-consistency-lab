# 05. 멱등 컨슈머 (Idempotent Consumer)

## 문제

03/04의 outbox는 **at-least-once**다. 같은 이벤트가 두 번 올 수 있다. 컨슈머 쪽에서도:
- DB 처리 후 offset 커밋 전에 죽으면 → 재시작 후 같은 메시지를 다시 받는다.
- 리밸런스 중에 다른 컨슈머가 같은 파티션을 이어받으면 → 커밋 안 된 메시지를 다시 받는다.

즉 **중복 수신은 버그가 아니라 정상 동작**이다. 그런데 "포인트 1% 적립"처럼 실행할 때마다 쌓이는 부작용은 두 번 실행하면 두 번 쌓인다.

## 해결

메시지마다 고유한 `message_id`(헤더, = `outbox.id`)를 `processed_messages`에 기록하고, **부작용과 같은 DB 트랜잭션**에서 처리한다.

```
tx {
  INSERT INTO processed_messages (consumer_group, message_id) ... ON CONFLICT DO NOTHING
  → 0행이면: 이미 처리한 메시지 → skip
  → 1행이면: orders 갱신 + point_ledger insert
} commit
→ 그 다음에 Kafka offset commit
```

- **같은 트랜잭션**: 부작용이 실패하면 표시도 롤백 → 재전송 시 다시 처리(유실 없음). 표시가 커밋됐으면 부작용도 커밋됨(중복 없음).
- **DB 커밋 후 offset 커밋**: 반대로 하면 DB 처리 중 죽었을 때 메시지가 영영 다시 안 온다(유실). 이 순서면 최악이 "재전송"이고, 재전송은 위의 표시로 걸러진다.
- **enable.auto.commit=false**: 자동 커밋은 처리 완료와 무관하게 주기적으로 offset을 커밋한다.

결과적으로 **at-least-once 전달 + 멱등 처리 = effectively-once 처리**.

### 계속 실패하는 메시지 → DLT (dead letter topic)

실패한 메시지를 건너뛰면 유실이고, 무한 재시도하면 그 파티션이 멈춘다. 그래서 횟수를 세다가 넘기면
**다른 토픽(`<topic>-dlt`)에 옮겨 적고 나서** 넘어간다. 단, **메시지 탓인 실패만** 센다 —
DB 가 3초 내려갔다고 멀쩡한 결제 이벤트를 DLT 로 보내면 안 되기 때문이다 (DLT 를 되돌리는 도구는 없다).

```
pollOnce: 레코드마다 try { 처리 }
  실패 → DeadLetterPublisher.recordFailure → FailureClassifier.classify(예외 + 원인 체인)
    ├ TRANSIENT (DB·네트워크 장애)          → 세지 않음, 실패 레코드 앞까지 커밋 후 throw
    │                                          → ConsumerRunner 가 백오프(1s,2s,4s…30s) 후 컨슈머 재생성 → 그 레코드부터 다시 (끝없이)
    ├ RETRYABLE, 횟수 < max-attempts        → 위와 같이 재시도 (횟수 +1)                         ← 기존 흐름
    ├ RETRYABLE 횟수 도달 or NOT_RETRYABLE  → DLT 발행, send().get() 으로 ack 대기 → 다음 레코드
    │                                          (배치 끝 commitSync 가 이 offset 을 넘어 커밋)
    └ DLT 발행 실패                          → 앞까지만 커밋 후 throw (이 레코드는 커밋 안 됨)
```

- **에러 분류** (`FailureClassifier`, Spring `ExceptionClassifier` 처럼 원인 체인 전체를 본다):

  | 종류 | 예외 | 처리 |
  |---|---|---|
  | `NOT_RETRYABLE` | `MissingMessageIdException` | 1번 만에 DLT |
  | `TRANSIENT` | `TransientDataAccessException`(쿼리 타임아웃, 데드락…), `RecoverableDataAccessException`, `DataAccessResourceFailureException`(`CannotGetJdbcConnectionException` 포함), `CannotCreateTransactionException`, `SQLTransientException`/`SQLRecoverableException`/`SQLNonTransientConnectionException`, `SQLException` 중 SQLSTATE `08xxx`·`53xxx`·`57P01~03`·`57014`·`40001`·`40P01`, `SocketException`/`SocketTimeoutException` | **DLT 안 감, 횟수 안 셈**, 지수 백오프로 무한 재시도 |
  | `RETRYABLE` | 깨진 JSON(`JsonParseException`), 검증 실패, 결정적 DB 에러(`DataIntegrityViolationException` 등), **그 밖의 모든 예외** | `max-attempts` 번째에 DLT |

  - DB 가 내려갔을 때 실제로 보이는 예외는 `DataAccessException` 이 아니라 `CannotCreateTransactionException` 이다
    (`TransactionTemplate` → `DataSourceTransactionManager.doBegin` 이 커넥션을 못 얻음, 원인은 `PSQLException` SQLSTATE `08001`).
    테스트도 죽은 포트(`127.0.0.1:1`)로 이 진짜 예외를 만든다.
  - `DataAccessException` 전체를 TRANSIENT 로 두지 않는다: 제약 위반·SQL 문법 오류는 몇 번을 해도 똑같이 실패한다.
  - **모르는 예외 = RETRYABLE (기본값)**. Spring `ExceptionClassifier` 의 기본값(`defaultValue=true`, 모르는 건 재시도 후 recoverer)과 같은 선택이다.
    TRANSIENT 로 두면 payload 하나에서만 터지는 버그(NPE, 범위 밖 값)가 파티션을 영원히 멈춘다 — DLT 가 막으려던 바로 그 상황.
    RETRYABLE 이면 최악이 "분류 못 한 인프라 예외로 멀쩡한 메시지가 DLT 에 감" 인데, DLT 에 원본과 원래 위치가 남으니 유실은 아니다.
    그런 예외를 발견하면 `isTransient` 에 추가한다.
  - Spring 과 다른 점: Spring 은 `DeserializationException` 을 not-retryable 로 두지만, 여기서는 JSON 파싱을 리스너 안에서 하므로
    깨진 JSON 은 RETRYABLE (`max-attempts` 번 시도 후 DLT). 결과는 같고 시도가 두 번 더 많을 뿐이다.
- **재시도 간격 — 지수 백오프**: `ConsumerRunner` 가 연속 실패마다 `lab.consumer.retry.initial-delay`(1s) × 2ⁿ 을 기다리고,
  `lab.consumer.retry.max-delay`(30s) 에서 멈춘다. 메시지를 하나라도 처리하면 1s 로 돌아간다. Postgres 가 몇 분 내려가도
  최대 30초 간격으로 두드리다 살아나면 그 메시지부터 처리한다. (Spring 이라면 `setBackOffFunction` 으로 예외별
  `ExponentialBackOff` 를 주는 것과 같다 — [`FailedRecordTracker.java#L114-L116`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L114-L116), [`#L216-L222`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L216-L222))
- **`max-attempts` 는 컨슈머 인스턴스별이다**: 횟수는 이 인스턴스의 `DeadLetterPublisher` 메모리에만 있다. **재시작하거나 리밸런스로
  파티션이 다른 인스턴스로 넘어가면 0 부터 다시 센다.** 넘어간 파티션의 기록은 `ConsumerLoop` 의 `ConsumerRebalanceListener` 가 지운다
  (revoked/lost → 지움, assigned → 지금 할당된 파티션만 남김). 단 `close()` 중의 revoke 는 무시한다 — `ConsumerRunner` 는 실패할 때마다
  컨슈머를 닫고 새로 만드는데 `KafkaConsumer.close()` 도 revoke 콜백을 부르므로, 그때 지우면 재시도마다 횟수가 0 이 되어 poison message 가
  영영 DLT 에 못 간다.
- **설정 검증**: `DeadLetterPublisher` 는 생성 시 `max-attempts >= 1` 을, `ConsumerRunner` 는 `retryDelay > 0`, `maxRetryDelay >= retryDelay` 를
  확인한다 (`IllegalArgumentException` → 앱 기동 실패). 0 이하면 첫 실패에 재시도 없이 DLT 로 가 버린다.

- **이름**: `payment.events-dlt`. Spring Kafka 3.3+ 의 기본값(`<topic>-dlt`)을 그대로 따랐다.
  원래 토픽에서 기계적으로 만들어지므로 테스트·e2e 처럼 토픽이 여러 개여도 설정이 필요 없고,
  나중에 Spring Kafka 의 `DeadLetterPublishingRecoverer` 로 바꿔도 DLT 이름이 그대로다. (3.2 까지는 `.DLT` 였다)
- **실패 횟수는 `DeadLetterPublisher` 에**: `ConsumerRunner` 는 실패할 때마다 `ConsumerLoop` 를 새로 만들기 때문에,
  횟수를 루프에 두면 매번 0 이 된다. 그래서 루프보다 오래 사는 빈 하나(`DeadLetterPublisher`)가 들고 있다.
- **실패하면 실패 레코드 앞까지는 커밋한다** (전에는 배치 전체를 커밋하지 않았다). 같은 배치 앞쪽에서 DLT 로 보낸 레코드가
  재전송되어 DLT 에 또 들어가는 것을 막는다. Go 의 `rewindFrom` 과 같은 규칙이 됐다.
- **DLT 레코드**: 원래 key·value·헤더(`message_id` 포함)를 그대로 두고 아래 헤더를 덧붙인다. 이름은 Spring 과 같다.

  | 헤더 | 값 |
  |---|---|
  | `kafka_dlt-original-topic` / `-partition` / `-offset` | 원래 위치 → 고친 뒤 원래 토픽으로 다시 보낼 때, 또는 원본을 찾을 때 |
  | `kafka_dlt-original-consumer-group` | 실패한 컨슈머 그룹 |
  | `kafka_dlt-exception-fqcn` | 예외 클래스 (`com.fasterxml.jackson.core.JsonParseException`, `lab.consumer.MissingMessageIdException` …) |
  | `kafka_dlt-exception-message` | 예외 메시지 |
  | `kafka_dlt-attempts` | 시도 횟수 (lab 전용 — Spring 은 DLT 에 넣지 않는다) |

  값은 전부 UTF-8 문자열이다 (`rpk` 로 바로 읽히게). Spring 은 partition/offset 을 4/8바이트 big-endian 으로 넣으므로,
  Spring 컨슈머가 이 DLT 를 읽는다면 그 둘은 직접 파싱해야 한다.
- **순서 — DLT ack 후에 커밋**: `send().get()` 으로 브로커 ack 를 받은 **뒤에만** 그 레코드를 커밋 대상에 넣는다.
  커밋을 먼저 하고 DLT 발행이 실패하면, 메시지는 원래 토픽에서도(이미 지나감) DLT 에서도(안 들어감) 사라진다 = 유실.
  이 순서면 최악이 "DLT 에 두 번" 이다 (ack 후 커밋 전에 죽으면 다시 받아 다시 실패 → 다시 DLT).

## 핵심 코드 위치

- `src/main/kotlin/lab/consumer/IdempotentConsumer.kt:14` — 트랜잭션 안에서 표시(`:19`) → 중복이면 skip(`:22`) → 부작용(`:28`)
- `src/main/kotlin/lab/consumer/NaiveConsumer.kt:15` — 비교용: 무조건 처리
- `src/main/kotlin/lab/consumer/MessageHandler.kt:11` — 부작용: 주문 갱신(자연적 멱등) vs 포인트 적립(멱등 아님)
- `src/main/kotlin/lab/consumer/ConsumerLoop.kt:83` — poll 루프: 레코드마다 처리(`:116` `handleRecord` — `message_id` 확인 `:123`, DB 커밋까지) → 실패 시 `:145` `deadLetterOrRethrow` / `:154` 실패 앞까지 커밋 → 크래시 주입(`:108`) → offset 커밋(`:112`). `:63` `rebalanceListener`(빠진 파티션의 실패 기록 정리, `close()` 중엔 무시)
- `src/main/kotlin/lab/consumer/FailureClassifier.kt` — `:57` `classify`(원인 체인, NOT_RETRYABLE → TRANSIENT → RETRYABLE), `:72` `isTransient`, `:94` SQLSTATE
- `src/main/kotlin/lab/consumer/DeadLetterPublisher.kt` — `:46` 설정 검증, `:71` `recordFailure`(TRANSIENT 는 안 셈), `:98`/`:104` `forgetPartitions`/`retainPartitions`, `:112` `publish`(`:114` 원래 헤더 복사 + `kafka_dlt-*`, `:133` ack 대기), `:168` `dltTopic`
- `src/main/kotlin/lab/consumer/ConsumerRunner.kt` — 앱 실행 시 백그라운드 루프. 실패 시 컨슈머를 재생성하는 이유, `:81` `backoff`(지수 백오프, 상한), `:47` 처리하면 리셋
- `src/main/kotlin/lab/consumer/ConsumerApplication.kt:26` — DLT 토픽 생성, `:38` `DeadLetterPublisher` 빈 (kafka-clients `KafkaProducer`), `:50` `ConsumerRunner`(재시도 간격 설정)
- `src/main/resources/application.yml` — `enable-auto-commit: false`, `lab.consumer.dlt.max-attempts: 3`, `lab.consumer.retry.initial-delay: 1s` / `max-delay: 30s`

## 테스트 실행

```bash
cd kotlin
./gradlew :05-idempotent-consumer:test
```

테스트는 같은 `message_id`로 메시지를 직접 두 번 발행해 03의 at-least-once 중복을 흉내낸다.

| 테스트 | 증명하는 것 |
|---|---|
| `naive_consumer__duplicate_message__points_doubled` | 중복 메시지 → 포인트 2번 적립 (200점) |
| `idempotent_consumer__duplicate_message__processed_once` | 두 번 받았지만 처리 [true, false], 포인트 1번 |
| `idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped` | DB 커밋 후 offset 커밋 전 크래시 → 재시작한 컨슈머가 **실제로 재전송받고** 건너뜀, 그 뒤 offset = 그 메시지 offset + 1 로 커밋 |
| `consumer_loop__missing_message_id_header__fails_with_clear_error` | (DLT 없음) `message_id` 헤더 없는 메시지 → NPE가 아니라 무엇이 빠졌는지(토픽-파티션@offset) 알려주는 `MissingMessageIdException`(`IllegalStateException`), 처리 안 함 |

`DeadLetterTest` 는 실제 `ConsumerRunner` 를 돌린다 (재시도 = 컨슈머 재생성이므로 `loopFactory` 호출 횟수 = 시도 횟수). 테스트마다 새 토픽 + `<topic>-dlt`.

| 테스트 | 증명하는 것 |
|---|---|
| `dead_letter__always_failing_message__sent_to_dlt_after_max_attempts` | 항상 실패하는 X → 3번 시도 후 DLT. key·`message_id`·`kafka_dlt-*` 헤더 확인, 뒤의 Y 는 처리, 커밋 offset=2 (X 를 넘음) |
| `dead_letter__missing_message_id__sent_to_dlt_without_retry` | `message_id` 없는 메시지 → 컨슈머 재생성 없이 1번 만에 DLT (`attempts=1`), 뒤 메시지 처리, offset=2 |
| `dead_letter__transient_failure__processed_without_dlt` | 한 번 실패 후 성공 → 정상 처리, DLT 비어 있음 |
| `dead_letter__dlt_publish_fails__offset_not_committed_past_message` | DLT 프로듀서를 닫아 발행 실패 → 시도 횟수가 3을 넘도록 계속 재시도, offset 은 X 를 넘지 않고 Y 도 처리 안 됨, DLT 비어 있음 |
| `dead_letter__db_connection_error__retried_past_max_attempts_without_dlt` | 죽은 포트로 진짜 `CannotCreateTransactionException`(← `PSQLException` 08001)을 5번(> max-attempts 3) → DLT 비어 있음, X·Y 처리, 컨슈머 6번 생성, offset=2 |
| `dead_letter__malformed_json__sent_to_dlt_after_max_attempts` | 깨진 JSON → 3번 시도 후 DLT (`JsonParseException`, 원래 value 유지), Y 처리, offset=2 |

DLT 테스트는 `processedIds()` 가 아니라 **커밋된 offset 을 기다린다**: DB 커밋 뒤 같은 `pollOnce` 안에서 `commitSync` 가 나중에 돌기 때문에,
DB 행을 본 직후 `committedOffset()` 을 읽으면 아직 예전 값일 수 있다.

`DeadLetterUnitTest` (브로커·DB 없음): 에러 분류표, `ConsumerRunner.backoff`(1s→2s→4s→…→30s), 잘못된 설정 거부,
`MockConsumer` 로 리밸런스 리스너(revoked/lost 는 지움, `close()` 중 revoke 는 무시, assigned 는 현재 할당만 남김).

## 한계 / 다음 단계

- `processed_messages`는 계속 쌓이므로 보관 기간을 정해 지워야 한다. 단, 브로커 재전송 가능 기간(retention, outbox 재발행 가능 기간)보다 길게 보관해야 한다.
- 부작용이 **우리 DB 밖**(이메일 발송, 외부 API 호출)이면 같은 트랜잭션으로 묶을 수 없다. 그 외부 API 자체에 멱등키(02)를 넘기거나, 그 작업을 또 outbox(03)로 보낸다.
- **재시도 중에는 그 파티션이 멈춘다**: DLT 로 가기 전 `max-attempts` 번 동안, 그리고 **인프라 장애(TRANSIENT) 동안은 무한히**
  뒤 메시지를 처리하지 않는다 (순서 보장 + "멀쩡한 메시지를 DLT 로 보내지 않음"의 대가). DB 장애가 길면 lag 이 쌓인다 — 알림은 lag 으로 건다.
  재시도 한 번 = 컨슈머 재생성 + 그룹 재참여라 느리다 (테스트에서 약 4초).
- **`max-attempts` 는 컨슈머 인스턴스별, 메모리에만 있다**: 재시작하거나 리밸런스로 파티션이 다른 인스턴스로 넘어가면 0 부터 다시 센다 →
  한 메시지가 전체로는 `max-attempts` 보다 많이 시도될 수 있다. 정확히 세려면 DB/브로커에 저장해야 한다. (Spring 의 `FailedRecordTracker` 도 메모리다)
- **분류는 목록 기반이다**: 목록에 없는 인프라 예외(예: 다른 드라이버의 연결 예외)는 RETRYABLE 로 떨어져 `max-attempts` 후 DLT 로 간다.
  DLT 에 원본이 남으므로 유실은 아니지만, 발견하면 `FailureClassifier.isTransient` 에 추가해야 한다.
- **DLT 발행도 at-least-once**: ack 후 커밋 전에 죽으면 같은 메시지가 DLT 에 두 번 들어간다. DLT 를 읽는 쪽은
  `kafka_dlt-original-topic/partition/offset` (또는 `message_id`) 로 중복을 걸러야 한다.
- **DLT 발행이 계속 실패하면** 다시 파티션 정지다 (유실보다 낫다). 그동안 시도 횟수는 계속 올라가고, 발행이 되살아나면
  그 횟수 그대로 DLT 에 적힌다. Spring 은 기본적으로 이때 횟수를 리셋하고 재시도 주기를 처음부터 다시 돈다.
- DLT 에 쌓인 메시지를 고쳐서 원래 토픽으로 되돌리는 도구(재처리)는 없다. 헤더에 원래 위치가 있으니 `rpk` 로 손으로 할 수 있다.
- 순서: 같은 주문의 이벤트는 key=orderId라 같은 파티션으로 오지만, 재전송이 섞이면 "오래된 이벤트가 나중에" 올 수 있다. 상태 전이(PENDING→PAID)를 검사하거나 버전 번호를 쓰는 방법이 있다.

## Spring Kafka 와 비교 (DLT)

Spring Kafka 에서는 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` 를 컨테이너에 걸면 위 동작을 해 준다.
여기서는 그 클래스들을 쓰지 않고 (kafka-clients 의 `KafkaConsumer`/`KafkaProducer` 만으로) 같은 동작을 손으로 짰다. 인용은 이 저장소가 쓰는 버전(Spring Boot 3.5.16 → spring-kafka **3.3.16**)의 태그 커밋
[`87c4b04`](https://github.com/spring-projects/spring-kafka/tree/87c4b047b658fa472201299e1cc4be7842c63c73) 기준이다.

| 개념 | Spring Kafka 3.3.16 | 여기 |
|---|---|---|
| DLT 이름 `<topic>-dlt`, 파티션은 원래 번호 | [`DeadLetterPublishingRecoverer.java#L79-L80`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L79-L80) — 3.2 까지는 `.DLT`, 3.3 에서 바뀜 ([GH-3291, `5529cbb`](https://github.com/spring-projects/spring-kafka/commit/5529cbb85b)) | `DeadLetterPublisher.dltTopic` = `<topic>-dlt`. 파티션은 key 로 정한다 (DLT 파티션 수가 달라도 됨) |
| 기본 시도 횟수 **10** (= 첫 시도 + 재시도 9, 간격 0) | [`SeekUtils.java#L57-L63`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/SeekUtils.java#L57-L63) `DEFAULT_MAX_FAILURES = 10`, `FixedBackOff(0, 9)` / [`DefaultErrorHandler.java#L62-L64`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DefaultErrorHandler.java#L62-L64) | `lab.consumer.dlt.max-attempts` (기본 3). 간격은 `ConsumerRunner` 의 지수 백오프(1s → 30s 상한) |
| 예외별 BackOff (인프라 장애는 끝없이) | [`FailedRecordTracker.java#L114-L116`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L114-L116) `setBackOffFunction`, [`#L216-L222`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L216-L222) `determineBackOff` — 예: 연결 예외면 `maxElapsedTime` 없는 `ExponentialBackOff` | `FailureKind.TRANSIENT` 는 횟수를 안 세고, `ConsumerRunner` 가 백오프로 무한 재시도 |
| 주의: 기본 생성자에는 DLT 가 없다 | 위 `DefaultErrorHandler()` 는 recoverer 가 `null` → 10번 실패하면 **로그만 남기고 건너뛴다**. DLT 를 원하면 `new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), backOff)` | `ConsumerLoop` 에 `deadLetter` 를 안 주면 건너뛰지 않고 무한 재시도 (유실보다 정지) |
| 실패 횟수 기록 (메모리) | [`FailedRecordTracker.java#L50`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L50) 스레드별 `Map<TopicPartition, FailedRecord>`, [`#L168-L195`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L168-L195) `recovered()` | `DeadLetterPublisher.attempts` (`ConcurrentHashMap`), `recordFailure()` |
| 재시도할 가치가 없는 예외 | [`ExceptionClassifier.java#L59-L66`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/ExceptionClassifier.java#L59-L66) `DeserializationException`, `MessageConversionException`, `ClassCastException` 등 → 곧장 recoverer | `FailureClassifier`: `MissingMessageIdException` → `NOT_RETRYABLE` |
| 모르는 예외의 기본값 / 원인 체인 | [`ExceptionClassifier.java#L68-L71`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/ExceptionClassifier.java#L68-L71) 목록 밖은 `true`(재시도 후 recoverer), [`#L223-L228`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/ExceptionClassifier.java#L223-L228) `setTraverseCauses(true)` | 목록 밖은 `RETRYABLE`(max-attempts 후 DLT), `cause` 체인을 끝까지 본다 |
| 재시도 = 실패한 레코드부터 seek, 복구됐으면 그 레코드는 seek 에서 뺀다 | [`SeekUtils.java#L104-L131`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/SeekUtils.java#L104-L131) `doSeeks` | 실패한 레코드 앞까지 커밋 + `ConsumerRunner` 가 컨슈머 재생성 / DLT 성공이면 다음 레코드로 |
| 원래 헤더 유지 | [`DeadLetterPublishingRecoverer.java#L529`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L529) `new RecordHeaders(record.headers().toArray())` | `publish` 에서 같은 `RecordHeaders(record.headers().toArray())` |
| 원래 topic/partition/offset 헤더 | [`DeadLetterPublishingRecoverer.java#L780-L788`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L780-L788) (partition/offset 은 `ByteBuffer` binary), 이름은 [`KafkaHeaders.java#L198-L216`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/support/KafkaHeaders.java#L198-L216) | 같은 이름, 값은 문자열 |
| 예외 헤더 | [`DeadLetterPublishingRecoverer.java#L812-L834`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L812-L834) fqcn, cause-fqcn, message, stacktrace / 이름 [`KafkaHeaders.java#L153-L171`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/support/KafkaHeaders.java#L153-L171) | fqcn, message 만 (+ lab 전용 `kafka_dlt-attempts`) |
| DLT ack 를 기다린 뒤에 넘어간다 | [`DeadLetterPublishingRecoverer.java#L108`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L108) `failIfSendResultIsError = true`, [`#L705-L706`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L705-L706) → [`#L726`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/DeadLetterPublishingRecoverer.java#L726) `sendResult.get(timeout)`. 실패하면 예외 → 그 레코드는 다시 seek | `producer.send(out).get(timeout)`. 실패하면 `DeadLetterPublishException` → 커밋 안 함 |
| DLT 발행이 실패했을 때 실패 횟수 | [`FailedRecordTracker.java#L224-L237`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L224-L237) `resetStateOnRecoveryFailure` (기본 `true`, [`#L64`](https://github.com/spring-projects/spring-kafka/blob/87c4b047b658fa472201299e1cc4be7842c63c73/spring-kafka/src/main/java/org/springframework/kafka/listener/FailedRecordTracker.java#L64)) → 리셋하고 재시도 주기를 처음부터 | **다름**: 리셋하지 않는다 → 다음 실패마다 바로 DLT 발행을 다시 시도 |

`main` 브랜치(4.x)에서는 헤더 처리가 `DeadLetterRecordManager` 로 옮겨졌지만 이름·기본값(`-dlt`, 10회)은 같다.
