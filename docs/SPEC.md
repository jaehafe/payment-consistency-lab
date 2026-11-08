# 공통 스펙 (Kotlin / Go 구현 계약)

두 언어 구현은 **같은 시나리오, 같은 스키마, 같은 테스트 이름**을 갖는다.
Kotlin 코드를 읽고 Go 코드를 나란히 놓고 비교할 수 있게 하는 것이 목표다.

## 공통 도메인

- **결제(payment)**: 주문(order)에 대해 외부 PG에 승인 요청 → 성공 시 DB에 저장 → `PaymentCompleted` 이벤트 발행.
- **Fake PG** (`PaymentGateway`): 외부 결제사를 흉내내는 in-process 구현.
  - `charge(idempotencyKey, orderId, amount) -> pgTxId`
  - PG 자체도 idempotency key를 지원한다 (토스페이먼츠/Stripe처럼). 같은 key로 다시 오면 **새로 결제하지 않고** 이전 pgTxId를 돌려준다.
  - 실패 주입: `failBeforeCharge`(승인 전 에러), `timeoutAfterCharge`(승인은 됐는데 응답이 유실 → 호출자는 타임아웃만 봄).
  - 테스트 검증용으로 `chargeCount()`(실제 승인 횟수)를 노출한다.
- **브로커**: Kafka (로컬은 Redpanda). 토픽 `payment.events`. 메시지 key = `orderId`(주문 단위 순서 보장), 헤더 `message_id` = 이벤트 고유 ID.
- **DB**: PostgreSQL 16. SQL은 ORM 없이 직접 작성 (Kotlin: `JdbcTemplate`/`TransactionTemplate`, Go: `pgx/v5`) — 트랜잭션 경계를 눈으로 보기 위해서.
- **테스트**: Testcontainers(Postgres, Redpanda)로 실제 인프라를 띄운다. 각 단계의 핵심 학습 포인트는 **테스트가 증명**한다.
- 이벤트 payload(JSON): `{"paymentId": "...", "orderId": "...", "amount": 10000, "pgTxId": "...", "occurredAt": "ISO-8601"}`

## 01-dual-write-problem — 문제 재현

스키마:
```sql
CREATE TABLE payments (
  id          UUID PRIMARY KEY,
  order_id    TEXT NOT NULL UNIQUE,
  amount      BIGINT NOT NULL,
  status      TEXT NOT NULL,          -- COMPLETED
  pg_tx_id    TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
구현: DB 저장과 Kafka 발행을 **각각** 한다 (나이브 구현 3가지).
- `commitThenPublish`: DB 커밋 → 발행. 발행 실패 시 DB엔 결제가 있는데 이벤트 없음.
- `publishThenCommit`: 발행 → DB 커밋. 커밋 실패 시 이벤트는 나갔는데 결제 없음(유령 이벤트).
- `publishInsideTx`: 트랜잭션 안에서 발행 후 롤백 → 이벤트는 이미 나감 (Kafka는 DB 트랜잭션에 참여하지 않음).

테스트:
- `commit_then_publish__broker_failure__payment_saved_but_event_lost`
- `publish_then_commit__db_failure__ghost_event_published`
- `publish_inside_tx__rollback__event_already_sent`

## 02-idempotency-key — 멱등키 (Brandur의 rocket-rides-atomic 방식)

스키마 (+ 01의 payments):
```sql
CREATE TABLE idempotency_keys (
  id              BIGSERIAL PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  request_path    TEXT NOT NULL,
  request_hash    TEXT NOT NULL,       -- 요청 바디 SHA-256
  locked_at       TIMESTAMPTZ,         -- 처리 중 표시 (NULL = 잠금 없음)
  recovery_point  TEXT NOT NULL,       -- started | pg_charged | finished
  response_code   INT,
  response_body   JSONB,
  pg_tx_id        TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_run_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
HTTP: `POST /payments`, 헤더 `Idempotency-Key`, 바디 `{"orderId": "...", "amount": 10000}`.
- 응답 201 `{"paymentId","orderId","amount","pgTxId","status"}`
- 헤더 없음 → 400, 같은 key 다른 바디 → 422, 다른 요청이 처리 중(잠금 유효) → 409.

처리 흐름 (atomic phase로 나눔):
1. **Phase 1 (tx)**: key upsert. 이미 있으면 hash 비교 → 다르면 422. `finished`면 저장된 응답 그대로 반환. 잠금이 유효(locked_at이 lockTimeout 이내)면 409. 아니면 `locked_at=now()`로 잠그고 진행.
2. **외부 호출 (트랜잭션 밖)**: PG `charge(idempotencyKey, ...)`. ← 외부 상태 변경은 DB 트랜잭션으로 롤백 불가. PG 쪽 멱등성에 기대야 함.
3. **Phase 2 (tx)**: payments insert + `recovery_point=pg_charged`, `pg_tx_id` 저장.
4. **Phase 3 (tx)**: 응답 저장, `recovery_point=finished`, `locked_at=NULL`.
- 재시도 시 `recovery_point`부터 이어서 실행. 실패 시 잠금 해제(재시도 가능한 에러) 처리.

테스트:
- `same_key_twice__charged_once__same_response`
- `same_key_different_body__422`
- `concurrent_same_key__only_one_charge`
- `pg_timeout_after_charge__retry_resumes__no_double_charge`
- `missing_header__400`

## 03-outbox-polling — Transactional Outbox + Polling Relay

스키마 (+ payments):
```sql
CREATE TABLE outbox (
  id              UUID PRIMARY KEY,         -- = message_id 헤더
  aggregate_type  TEXT NOT NULL,            -- 'payment'
  aggregate_id    TEXT NOT NULL,            -- orderId (= Kafka key)
  event_type      TEXT NOT NULL,            -- 'PaymentCompleted'
  payload         JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at    TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;
```
- `PaymentService.pay`: **한 트랜잭션**에서 payments insert + outbox insert.
- `OutboxRelay.relayOnce(batchSize)`: tx { `SELECT ... WHERE published_at IS NULL ORDER BY created_at LIMIT n FOR UPDATE SKIP LOCKED` → Kafka 동기 발행(acks=all) → `published_at=now()` } 커밋. 주기적으로 반복 실행.
- **at-least-once**: 발행 후 커밋 전에 죽으면 재발행 → 중복 가능 (05에서 해결).

테스트:
- `pay__writes_payment_and_outbox_atomically`
- `pay__db_failure__no_payment_no_outbox`
- `broker_down__payment_saved__event_delivered_after_recovery`
- `two_relays_concurrently__each_event_published_once` (SKIP LOCKED)
- `crash_after_publish_before_mark__event_published_twice` (at-least-once 증명)

## 04-outbox-cdc — Debezium Outbox Event Router

- 애플리케이션 코드는 outbox insert까지만 (relay 없음). 테이블은 Debezium Outbox Event Router 기본 컬럼명 사용:
```sql
CREATE TABLE outbox (
  id             UUID PRIMARY KEY,
  aggregatetype  TEXT NOT NULL,
  aggregateid    TEXT NOT NULL,
  type           TEXT NOT NULL,
  payload        JSONB NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
- Postgres `wal_level=logical` → Debezium(Kafka Connect)이 WAL을 읽어 `payment.events`로 라우팅. 커넥터 설정은 `infra/debezium/outbox-connector.json` (공통).
- 테스트: `pay__writes_outbox_row` (단위). E2E는 `infra/docker-compose.yml`로 띄워서 README 절차대로 확인 (+ 선택적 e2e 테스트).
- 학습 포인트: polling vs CDC 트레이드오프 (지연, DB 부하, 운영 복잡도, outbox 테이블 정리).

## 05-idempotent-consumer — 멱등 컨슈머

스키마:
```sql
CREATE TABLE orders (
  id          TEXT PRIMARY KEY,
  status      TEXT NOT NULL,      -- PENDING | PAID
  paid_amount BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE point_ledger (     -- 결제 금액의 1% 적립. 중복 처리되면 두 번 쌓인다 (자연적으로 멱등하지 않은 부작용)
  id          BIGSERIAL PRIMARY KEY,
  order_id    TEXT NOT NULL,
  points      BIGINT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE processed_messages (
  consumer_group TEXT NOT NULL,
  message_id     TEXT NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, message_id)
);
```
- `NaiveConsumer`: 메시지 받을 때마다 orders 갱신 + point_ledger insert.
- `IdempotentConsumer`: 같은 트랜잭션에서 `INSERT INTO processed_messages ... ON CONFLICT DO NOTHING` → 영향 행 0이면 skip. DB 커밋 **후** Kafka offset 커밋.
- 테스트:
  - `naive_consumer__duplicate_message__points_doubled`
  - `idempotent_consumer__duplicate_message__processed_once`
  - `idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped`

## 코드 스타일

- 주석·README는 한국어. "왜 이렇게 하는가"를 주석으로 남긴다.
- Go 코드는 사용자가 Go 문법에 익숙하지 않으므로 처음 등장하는 문법(에러 처리, defer, goroutine, context, 구조체 메서드, 인터페이스 등)에 짧은 설명 주석을 단다.
- 각 단계 디렉토리에 `README.md`: 문제 → 해결 → 핵심 코드 위치 → 테스트 실행법 → 한계/다음 단계.
