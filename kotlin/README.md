# Kotlin + Spring Boot 구현

결제 정합성 패턴을 단계별 Spring Boot 앱으로 구현했다. 계약(스키마, 시나리오, 테스트 이름)은 [`../docs/SPEC.md`](../docs/SPEC.md)를 따른다.
Go 구현(`../go/`)과 같은 테스트 이름을 쓰므로 나란히 놓고 비교할 수 있다.

## 모듈 지도

| 모듈 | 패키지 | 주제 | 테스트가 증명하는 것 |
|---|---|---|---|
| [`01-dual-write-problem`](01-dual-write-problem/README.md) | `lab.dualwrite` | 이중 쓰기 문제 재현 | 이벤트 유실, 유령 이벤트, 트랜잭션 안 발행의 함정 |
| [`02-idempotency-key`](02-idempotency-key/README.md) | `lab.idempotency` | 멱등키 (atomic phases + recovery point) | 재시도/동시 요청/PG 타임아웃에도 승인 1번 |
| [`03-outbox-polling`](03-outbox-polling/README.md) | `lab.outbox.polling` | Transactional Outbox + Polling Relay | 원자적 저장, 장애 후 전달, SKIP LOCKED, at-least-once |
| [`04-outbox-cdc`](04-outbox-cdc/README.md) | `lab.outbox.cdc` | Outbox + Debezium CDC | outbox 행 형식 (E2E는 수동 절차) |
| [`05-idempotent-consumer`](05-idempotent-consumer/README.md) | `lab.consumer` | 멱등 컨슈머 | 중복 메시지/크래시 재전송에도 처리 1번 |

읽는 순서는 01 → 05. 각 모듈은 혼자 읽어도 이해되도록 Fake PG, 이벤트 DTO 같은 작은 코드를 공유 모듈 없이 **일부러 복사**해 두었다.

## 실행

필요한 것: **JDK 21**, **Docker** (Gradle은 wrapper가 내려받는다).

```bash
cd kotlin
./gradlew test                                  # 전체 (Testcontainers가 Postgres 16 / Redpanda를 자동 기동)
./gradlew :03-outbox-polling:test               # 모듈 하나
./gradlew :03-outbox-polling:test --tests '*two_relays*'   # 테스트 하나
```

테스트 리포트: `<모듈>/build/reports/tests/test/index.html`

앱을 직접 띄워 보려면 `../infra/docker-compose.yml`로 Postgres(localhost:15432)와 Redpanda(localhost:19092)를 띄운 뒤
`./gradlew :<모듈>:bootRun`. (예: `02-idempotency-key`는 8080 포트에 `POST /payments`)

```bash
curl -i -X POST localhost:8080/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: abc-123' \
  -d '{"orderId": "order-1", "amount": 10000}'
```

## 공통 설계 선택

- **JPA 없이 `JdbcTemplate` + `TransactionTemplate`**: `@Transactional` 대신 `tx.execute { ... }` 블록을 써서 트랜잭션이 어디서 시작하고 어디서 커밋되는지 코드에 그대로 보이게 했다.
- **스키마**: 각 모듈 `src/main/resources/schema.sql` (`spring.sql.init.mode=always`).
- **장애 주입**: 실제 장애를 일으키기 어려운 지점(브로커 다운, 커밋 직전 DB 실패, 발행 후 크래시, PG 응답 유실)은 Fake/래퍼 컴포넌트의 토글로 주입한다. 그 외(Postgres 트랜잭션, 행 잠금, Kafka 발행/소비, offset 커밋)는 전부 실제 인프라에서 일어난다.
- **이벤트 확인**: 테스트는 Kafka 토픽을 실제로 끝까지 읽어서 이벤트의 존재/개수/`message_id`를 확인한다 (`EventProbe`).

## 버전

Spring Boot 3.5.16 · Kotlin 2.2.21 · Gradle 8.14.3 (wrapper) · Java 21 · Testcontainers (Boot BOM 관리) · Postgres 16 · Redpanda v24.2.7
