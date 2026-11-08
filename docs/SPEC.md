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

스키마 (+ payments). Kotlin 테이블 이름은 `outbox_polling` (04의 `public.outbox`와 같은 compose DB에서 충돌하지 않도록), Go는 스키마 `go_03` 로 격리:
```sql
CREATE TABLE outbox_polling (   -- Go: go_03.outbox
  id              UUID PRIMARY KEY,         -- = message_id 헤더
  aggregate_type  TEXT NOT NULL,            -- 'payment'
  aggregate_id    TEXT NOT NULL,            -- orderId (= Kafka key)
  event_type      TEXT NOT NULL,            -- 'PaymentCompleted'
  payload         JSONB NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at    TIMESTAMPTZ
);
CREATE INDEX outbox_polling_unpublished_idx ON outbox_polling (created_at) WHERE published_at IS NULL;
```
- `PaymentService.pay`: **한 트랜잭션**에서 payments insert + outbox insert.
- `OutboxRelay.relayOnce(batchSize)`: tx { `SELECT ... WHERE published_at IS NULL ORDER BY created_at LIMIT n FOR UPDATE SKIP LOCKED` → Kafka 동기 발행(acks=all, 헤더 `message_id`·`event_type`) → `published_at=now()` } 커밋. 주기적으로 반복 실행.
- **at-least-once**: 발행 후 커밋 전에 죽으면 재발행 → 중복 가능 (05에서 해결).

테스트:
- `pay__writes_payment_and_outbox_atomically`
- `pay__db_failure__no_payment_no_outbox`
- `broker_down__payment_saved__event_delivered_after_recovery`
- `two_relays_concurrently__each_event_published_once` (Kotlin 40건 / Go 30건, 모두 서로 다른 id로 정확히 1번·각 relay > 0. 중복을 막는 건 `FOR UPDATE`, `SKIP LOCKED`는 대기를 없앤다)
- `crash_after_publish_before_mark__event_published_twice` (at-least-once 증명)
- `schema__does_not_collide_with_04_cdc_outbox_table` (Kotlin만)

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
- 처리 실패 시: 실패한 레코드 **앞까지만** offset 커밋하고 실패 레코드부터 다시 받는다 (Kotlin: `ConsumerRunner`가 실패한 배치를 커밋하지 않고 컨슈머를 재생성 → 배치 전체 재전달, 성공분은 processed_messages가 skip. Go: `rewindFrom`이 먼저 되감고 실패 앞까지 커밋 + `ConsumerLoop.Run`. fetch 에러 시에도 받은 레코드 전부 되감기). 실패 메시지를 건너뛰고 뒤 메시지 offset을 커밋하면 유실.
- `message_id` 헤더가 없으면 명확한 에러(poison message, DLQ는 범위 밖).
- 테스트:
  - `naive_consumer__duplicate_message__points_doubled`
  - `idempotent_consumer__duplicate_message__processed_once`
  - `idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped` (재전달 skip 후 offset = 메시지 offset + 1 단언)
  - `consumer_loop__missing_message_id_header__fails_with_clear_error`
  - `idempotent_consumer__handler_failure__offset_not_committed_past_failed_message` (Go만)

## 코드 스타일

- 주석·README는 한국어. "왜 이렇게 하는가"를 주석으로 남긴다.
- Go 코드는 사용자가 Go 문법에 익숙하지 않으므로 처음 등장하는 문법(에러 처리, defer, goroutine, context, 구조체 메서드, 인터페이스 등)에 짧은 설명 주석을 단다.
- 각 단계 디렉토리에 `README.md`: 문제 → 해결 → 핵심 코드 위치 → 테스트 실행법 → 한계/다음 단계.

---

# Part 2 — 06~10단계

공통 규칙은 Part 1과 같다 (ORM 없이 SQL, Testcontainers, 한국어 주석, Go 문법 주석, 단계별 README).
**금액은 항상 정수(원, BIGINT/Long/int64)** — 부동소수점 금지.

## 06-balance-concurrency — 잔액 동시성 제어

스키마:
```sql
CREATE TABLE accounts (
  id       TEXT PRIMARY KEY,
  balance  BIGINT NOT NULL,          -- 의도적으로 CHECK(balance >= 0) 없음: 나이브 구현의 초과 인출을 보이기 위해
  version  BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE withdrawals (
  id          BIGSERIAL PRIMARY KEY,
  account_id  TEXT NOT NULL,
  amount      BIGINT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
`withdraw(accountId, amount) -> success/insufficient` 를 5가지 전략으로 구현 (성공 시 withdrawals에 1행):
- `NaiveWithdraw`: READ COMMITTED에서 SELECT로 읽고 → 앱에서 검사 → `UPDATE SET balance = <읽은 값 - amount>`. 읽기와 쓰기 사이 지연 주입 가능(레이스를 결정적으로 재현).
- `PessimisticWithdraw`: `SELECT ... FOR UPDATE`. (쓰기: Kotlin `balance = balance - ?`, Go `balance = <읽은 값 - amount>` — FOR UPDATE를 빼면 Kotlin은 초과 인출, Go는 lost update로 다르게 깨진다)
- `OptimisticWithdraw`: `UPDATE ... SET balance=?, version=version+1 WHERE id=? AND version=?`, 0행이면 재시도 (최대 N회 = 첫 시도 포함 총 시도 횟수. Kotlin `maxAttempts`, Go `MaxAttempts`, 기본 20).
- `SerializableWithdraw`: `SERIALIZABLE` 격리 수준, SQLSTATE `40001` 이면 재시도.
- `ConditionalUpdateWithdraw`: `UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?` 한 문장 (실무에서 가장 흔한 답).
- `transfer(from, to, amount)`: 두 계좌를 **id 순서로** 잠가 데드락 방지.

테스트 (잔액 1,000 계좌에 1,000원 인출 10건 동시 요청 등):
- `naive__concurrent_withdrawals__double_spending` (읽기-쓰기 지연으로 결정적: 10건 모두 성공, 출금 합계 10,000, 잔액 0)
- `pessimistic__concurrent_withdrawals__exactly_one_succeeds`
- `optimistic__concurrent_withdrawals__exactly_one_succeeds_with_retries` (재시도 정확히 9회)
- `serializable__concurrent_withdrawals__exactly_one_succeeds_with_retries` (40001 재시도 정확히 9회)
- `conditional_update__concurrent_withdrawals__exactly_one_succeeds`
- `transfer__opposite_directions_concurrently__no_deadlock` (A→B, B→A 동시 다수, 총액 보존)
- `transfer__unordered_locking__deadlock_detected` (잠금 순서 미정렬 버전은 Postgres `40P01` 발생을 보여줌)

README에 전략 비교표: 정합성 / 처리량 / 재시도 필요 / 언제 쓰나.

## 07-payment-state-machine — 상태 머신 · 보류 · 보상 · 재처리

스키마:
```sql
CREATE TABLE payments (
  id              UUID PRIMARY KEY,
  order_id        TEXT NOT NULL UNIQUE,
  amount          BIGINT NOT NULL,
  status          TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,   -- PG 호출용
  pg_tx_id        TEXT,
  attempt_count   INT NOT NULL DEFAULT 0, -- janitor 확인 시도 횟수
  next_retry_at   TIMESTAMPTZ,
  last_error      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE payment_status_history (
  id          BIGSERIAL PRIMARY KEY,
  payment_id  UUID NOT NULL,
  from_status TEXT,
  to_status   TEXT NOT NULL,
  reason      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
상태: `REQUESTED → PENDING → APPROVED | FAILED | UNKNOWN`, `UNKNOWN → APPROVED | FAILED | MANUAL_REVIEW`, `APPROVED → CANCEL_REQUESTED → CANCELED`.
- 허용 전이표를 코드에 명시, 그 외 전이는 예외.
- 전이는 `UPDATE ... SET status=? WHERE id=? AND status=?` (compare-and-set) + history insert 를 한 트랜잭션에서.
- Fake PG: `charge(key, orderId, amount)`, `getStatus(key) -> APPROVED|FAILED|NOT_FOUND`, `cancel(pgTxId)`. 실패 주입: timeoutAfterCharge, timeoutBeforeCharge(요청이 PG에 도달 못함), failCancel.
- PG 타임아웃 → `UNKNOWN` (실패로 단정하지 않는다! 승인됐을 수 있음).
- `Janitor.reconcileUnknown(now)`: `UNKNOWN AND next_retry_at <= now` 를 `FOR UPDATE SKIP LOCKED`로 집어 PG 조회 → APPROVED/FAILED 확정.
  - `attempt_count` = janitor 확인 횟수. 행을 집을 때 같은 문장에서 +1 한다 (집은 뒤 크래시해도 횟수가 남는다 — Go는 PG 호출이 트랜잭션 안이라 롤백되는 한계).
  - NOT_FOUND이고 `now < created_at + notFoundGrace`: "아직 판단 이르다" → 시도로 세지 않음(+1 취소), UNKNOWN 유지, `next_retry_at = created_at + notFoundGrace`.
  - NOT_FOUND이고 grace 경과: FAILED. → PG에 도달 못한 결제는 MANUAL_REVIEW가 아니라 FAILED로 끝난다.
  - 판단 불가(PG 조회 오류): 시도로 센다. `next_retry_at = now + base · 2^min(attempt-1, 20)`, `attempt_count >= maxAttempts`면 `MANUAL_REVIEW`(보류, 사람 확인).
- `CheckoutOrchestrator.checkout(order)`: 결제 승인 → 후속 단계(재고 확보 `InventoryClient.reserve`) 실패 시 **보상**: PG 취소 → `CANCELED`. 취소 실패 시 `CANCEL_REQUESTED`로 남기고(`attempt_count = 1`, `next_retry_at = now + 지연`) janitor가 재시도. janitor는 CAS 충돌 행은 건너뛰고 배치를 계속한다.

테스트:
- `invalid_transition__rejected`
- `every_transition_recorded_in_history`
- `concurrent_transition__only_one_wins`
- `pg_timeout_after_charge__unknown__janitor_confirms_approved`
- `pg_timeout_before_charge__unknown__janitor_marks_failed`
- `unknown_exceeds_max_attempts__moved_to_manual_review`
- `downstream_failure__compensated_by_pg_cancel`
- `cancel_failure__left_cancel_requested__janitor_retries_cancel`
- `not_found_within_grace__does_not_consume_attempts__ends_failed` (시계를 점프하지 않고 next_retry_at마다 janitor 실행)
- `backoff__large_attempt__capped_without_overflow` (Go만)
- `cancel_retry__stale_row_skipped__rest_of_batch_processed`, `janitor_crash_loop__claims_counted__moved_to_manual_review` (Kotlin만)

README에 상태 다이어그램(mermaid)과 운영 쿼리(상태별 건수, 오래된 UNKNOWN, MANUAL_REVIEW 목록).

## 08-ledger-settlement — 입금·출금·수수료·정산 (복식부기 원장)

스키마:
```sql
CREATE TABLE ledger_accounts (
  id       TEXT PRIMARY KEY,         -- 'pg:receivable', 'bank:cash', 'platform:fee_revenue', 'merchant:{id}:payable'
  type     TEXT NOT NULL,            -- ASSET | LIABILITY | REVENUE
  balance  BIGINT NOT NULL DEFAULT 0 -- 차변 +, 대변 - (부호 규칙: debit-positive)
);
CREATE TABLE journal_entries (
  id              UUID PRIMARY KEY,
  reference_type  TEXT NOT NULL,     -- PAYMENT | REFUND | PG_DEPOSIT | PAYOUT
  reference_id    TEXT NOT NULL,
  description     TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (reference_type, reference_id)   -- 같은 거래 두 번 전기 방지 (원장 멱등성)
);
CREATE TABLE ledger_lines (
  id          BIGSERIAL PRIMARY KEY,
  entry_id    UUID NOT NULL REFERENCES journal_entries(id),
  account_id  TEXT NOT NULL REFERENCES ledger_accounts(id),
  debit       BIGINT NOT NULL DEFAULT 0,
  credit      BIGINT NOT NULL DEFAULT 0,
  CHECK (debit >= 0 AND credit >= 0 AND (debit = 0) <> (credit = 0))
);
CREATE TABLE transactions (            -- 결제/환불 원천 거래
  id             UUID PRIMARY KEY,
  merchant_id    TEXT NOT NULL,
  type           TEXT NOT NULL,        -- PAYMENT | REFUND
  pg_tx_id       TEXT NOT NULL,
  amount         BIGINT NOT NULL,      -- 항상 양수
  fee            BIGINT NOT NULL,      -- 환불은 수수료 환급(음수 아님, 별도 표시)
  occurred_at    TIMESTAMPTZ NOT NULL,
  settlement_id  UUID
);
CREATE TABLE settlements (
  id               UUID PRIMARY KEY,
  merchant_id      TEXT NOT NULL,
  settlement_date  DATE NOT NULL,      -- KST 기준 거래일
  gross_amount     BIGINT NOT NULL,
  refund_amount    BIGINT NOT NULL,
  fee_amount       BIGINT NOT NULL,    -- 순수수료 (결제 수수료 - 환불 수수료 환급)
  net_amount       BIGINT NOT NULL,    -- gross - refund - fee
  status           TEXT NOT NULL,      -- CALCULATED | PAID
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (merchant_id, settlement_date)
);
```
- 수수료: `fee = (amount * rateBps + 5000) / 10000` (원 단위 HALF_UP), 기본 rateBps=300 (3%).
- 분개:
  - 결제 승인: Dr `pg:receivable` amount / Cr `merchant:{id}:payable` amount-fee / Cr `platform:fee_revenue` fee
  - 환불: 결제 분개의 역분개
  - PG 정산금 입금: Dr `bank:cash` / Cr `pg:receivable`
  - 가맹점 지급(출금): Dr `merchant:{id}:payable` / Cr `bank:cash` → settlement `PAID`
- `LedgerService.post(entry)`: 차변 합 ≠ 대변 합이면 거부. ledger_lines insert + balance 갱신을 한 트랜잭션.
- `SettlementService.settle(date)`: KST 거래일의 미정산 transactions 를 `FOR UPDATE`로 잠그고 **잠근 id 목록**으로만 집계·`settlement_id` 갱신 (날짜 범위로 다시 찾으면 집계 후 커밋된 늦은 거래가 집계 없이 정산됨 표시된다). 같은 날짜 재실행해도 중복 없음.
- 같은 결제를 두 번 기록해도 원장 전기 1회 + transactions 1행 (전기가 새로 생성됐을 때만 transactions insert).
- 알 수 없는 계정 id는 거부.
- `ReportQueries.dailyReport(date)`: 가맹점별 gross/refund/fee/net, 그리고 시산표(trial balance).

테스트:
- `payment_posting__debits_equal_credits`
- `unbalanced_entry__rejected`
- `fee_rounding__half_up_in_won` (Double 함정 예: `5750 × 0.022 = 126.49999999999999` → `Math.round` 126, 정수식 127)
- `same_reference_posted_twice__single_entry` (Kotlin은 transactions 1행·정산 gross 1배까지 단언)
- `same_payment_recorded_twice__single_transaction_row` (Go)
- `late_transaction_during_settle__not_marked_settled` (테스트 훅 `afterAggregate`로 결정적 재현)
- `unknown_account__rejected`
- `refund__reverses_payment_entries`
- `settle__net_equals_gross_minus_refund_minus_fee`
- `settle__uses_kst_day_boundary` (UTC 15:30 = 다음날 KST 00:30 → 다음날 정산)
- `settle_twice_same_date__no_duplicate_settlement`
- `payout__merchant_payable_zero_after_payout`
- `trial_balance__sum_of_balances_is_zero`

## 09-reconciliation — 대사 (누락·중복·금액 차이)

스키마 (+ 08의 transactions 간소화 버전 사용 가능):
```sql
CREATE TABLE internal_transactions (
  id          UUID PRIMARY KEY,
  pg_tx_id    TEXT NOT NULL,           -- 의도적으로 UNIQUE 아님: 내부 중복도 탐지 대상
  order_id    TEXT NOT NULL,
  type        TEXT NOT NULL,           -- PAYMENT | REFUND
  amount      BIGINT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE recon_files (             -- PG 정산 파일 import 이력 (같은 파일 두 번 import 방지)
  id         BIGSERIAL PRIMARY KEY,
  file_name  TEXT NOT NULL,
  file_hash  TEXT NOT NULL UNIQUE,     -- SHA-256 (정규화 내용: BOM 제거, CRLF·CR→LF, 줄 앞뒤 trim, 빈 줄 제거. 파싱도 같은 정규화 결과로)
  row_count  INT NOT NULL,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE pg_settlement_records (
  id          BIGSERIAL PRIMARY KEY,
  file_id     BIGINT NOT NULL REFERENCES recon_files(id),
  pg_tx_id    TEXT NOT NULL,
  order_id    TEXT NOT NULL,
  type        TEXT NOT NULL,
  amount      BIGINT NOT NULL,
  fee         BIGINT NOT NULL,
  transacted_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE recon_results (
  id              BIGSERIAL PRIMARY KEY,
  run_id          UUID NOT NULL,
  business_date   DATE NOT NULL,
  kind            TEXT NOT NULL,   -- MISSING_IN_INTERNAL | MISSING_IN_PG | AMOUNT_MISMATCH | DUPLICATE_IN_PG | DUPLICATE_IN_INTERNAL
  pg_tx_id        TEXT NOT NULL,
  internal_amount BIGINT,
  pg_amount       BIGINT,
  diff_amount     BIGINT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
- PG 정산 파일 CSV 헤더: `pg_tx_id,order_id,type,amount,fee,transacted_at` (테스트 fixture로 제공).
- 대사 SQL은 **별도 .sql 파일**(`recon_queries.sql`)에 이름 붙여 모아 둔다 — 사용자가 SQL만 따로 읽고 DB에서 직접 실행해볼 수 있게. 기법: `FULL OUTER JOIN ... ON pg_tx_id` (누락/금액차), `GROUP BY ... HAVING count(*) > 1` (중복). 대상 기간은 KST business_date를 **반열린 구간** `[KST 00:00, 다음날 KST 00:00)` 으로 (컬럼을 함수로 감싸지 않으므로 인덱스를 추가하면 쓸 수 있다 — 학습용 스키마엔 인덱스 없음).
- `ReconciliationService.run(businessDate)`: 결과를 recon_results 에 저장, `ReconReport` (종류별 건수, 차액 합계, 일치 건수) 반환 + Markdown 리포트 문자열 생성.

테스트:
- `matched_records__no_discrepancy`
- `missing_in_internal__detected`
- `missing_in_pg__detected`
- `amount_mismatch__detected_with_diff`
- `duplicate_in_pg_file__detected`
- `duplicate_internal__detected`
- `same_file_imported_twice__ignored`
- `same_file_with_different_line_endings__ignored`
- `kst_day_boundary__only_records_inside_day_counted`
- `quoted_csv_field_with_comma__parsed`, `ReconReportLocaleTest.markdown_report__independent_of_jvm_locale` (Kotlin만)
- `report__summarizes_counts_and_diff_amount`

## 10-observability — 로그 · 메트릭 · 장애 시나리오

07의 결제 흐름을 간소화해 재사용 (PG 호출 + UNKNOWN 처리 + 멱등키 재요청) 하고 관측성을 붙인다.
- **구조화 로그(JSON)**: 모든 로그에 `request_id`, `payment_id`, `order_id`, `idempotency_key`(있으면). `X-Request-Id` 헤더를 받아(없거나 `^[A-Za-z0-9._-]{1,64}$`에 안 맞으면 생성) 요청 전체에 전파. Kotlin: Spring Boot 구조화 로깅(`logging.structured.format.console`) + MDC, Go: `log/slog` JSON 핸들러 + context.
- **메트릭(Prometheus)**:
  - `payment_requests_total{result="approved|failed|unknown|replayed|conflict"}`
  - `pg_call_duration_seconds` (histogram)
  - `payment_unknown_current` (gauge, UNKNOWN 상태 결제 수)
  - `idempotency_replays_total`
  Kotlin: Micrometer + actuator `/actuator/prometheus`, 포트 8090. Go: `prometheus/client_golang`, `/metrics`, 포트 8091.
- **장애 시나리오** (Fake PG 실패 주입): PG 지연 급증, PG 타임아웃 급증, 중복 요청 폭주.
- `infra/` 에 Prometheus(+ alert rules), Grafana 가 `obs` 프로필로 준비되어 있다 (`infra/observability/`).
- README에 **런북**: 시나리오별 "어떤 알림이 뜨고 → 어떤 메트릭/PromQL을 보고 → 어떤 로그를 request_id로 grep 해서 → 원인 판단".

테스트:
- `request_id_propagated_to_all_logs` (로그 캡처해서 같은 request_id 확인)
- `pg_timeout__unknown_counter_and_gauge_increase`
- `duplicate_requests__replay_metric_increments`
- `pg_latency__histogram_records_duration`
- `metrics_endpoint_exposes_payment_metrics`
- `RequestIdFilterTest.mdc_cleared_between_requests_on_same_thread` (Kotlin), `invalid_request_id_header__replaced` (Go)
- Fake PG 타임아웃은 실제로 설정 시간만큼 대기 후 실패 (Kotlin `lab.pg.timeout-ms`, 기본 5000)
