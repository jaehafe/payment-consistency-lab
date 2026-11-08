# 07. 결제 상태 머신 · 보류 · 보상 · 재처리

## 문제

- 결제 상태를 `status = 'APPROVED'` 처럼 아무 데서나 바꾸면, "실패한 결제가 승인으로 되살아남" 같은 불가능한 전이가 생긴다.
- PG 호출이 **타임아웃**나면 승인됐는지 모른다. 실패로 단정하고 재결제를 유도하면 **이중 결제**.
- 결제는 승인됐는데 **다음 단계(재고 확보)가 실패**하면? PG 승인과 재고는 한 DB 트랜잭션으로 묶을 수 없다.
- 보상(취소)마저 실패하면?

## 해결

**1) 명시적 상태 머신**

```mermaid
stateDiagram-v2
    [*] --> REQUESTED
    REQUESTED --> PENDING: PG 호출 직전
    PENDING --> APPROVED: 승인
    PENDING --> FAILED: 명확한 거절
    PENDING --> UNKNOWN: 타임아웃 (결과 모름)
    UNKNOWN --> APPROVED: janitor - PG 조회 결과 승인
    UNKNOWN --> FAILED: janitor - 거절 또는 grace 후 NOT_FOUND
    UNKNOWN --> MANUAL_REVIEW: janitor - maxAttempts 초과 (보류)
    APPROVED --> CANCEL_REQUESTED: 후속 단계 실패 → 보상 시작
    CANCEL_REQUESTED --> CANCELED: PG 취소 성공 (즉시 또는 janitor 재시도)
    FAILED --> [*]
    CANCELED --> [*]
    MANUAL_REVIEW --> [*]
```

- 허용 전이표를 코드에 두고, 표에 없는 전이는 `ErrInvalidTransition`.
- 전이는 `UPDATE ... SET status = :to WHERE id = :id AND status = :from` (**compare-and-set**) + 이력 insert 를 한 트랜잭션에서. 0행이면 누군가 먼저 바꾼 것 → `ErrStaleStatus`.

**2) 타임아웃 = UNKNOWN, janitor 가 확정**

`Janitor.ReconcileUnknown(now)` 이 `UNKNOWN AND next_retry_at <= now` 를 `FOR UPDATE SKIP LOCKED` 로 집으면서 `attempt_count` 를 1 올리고(= 확인 1회), PG 조회 API(`GetStatus(idempotencyKey)`)를 부른다.
- APPROVED / FAILED → 확정
- NOT_FOUND → 요청이 PG 에 도달 못했을 가능성. 단, PG 반영이 늦을 수 있으니 `created_at + NotFoundGrace` 까지는 단정하지 않는다.
  - grace 안: **시도 횟수를 쓰지 않고**(올린 1 을 되돌림) `next_retry_at = created_at + NotFoundGrace` 로 재예약
  - grace 이후: FAILED
  - 왜? grace 안의 NOT_FOUND 를 "실패한 확인"으로 세면, 백오프 합(1분+2분)이 grace(5분)보다 짧아서 grace 가 끝나기도 전에 MANUAL_REVIEW 로 가 버린다 (돈이 안 빠진 결제가 사람 손으로 넘어감).
- 결론 못 냄(조회 장애 등) → 지수 백오프로 `next_retry_at` 갱신 (1분, 2분, 4분…, 지수 상한 20), `MaxAttempts` 회째에도 결론이 없으면 **MANUAL_REVIEW**(보류, 사람 확인)

**3) 보상 (Saga 의 가장 작은 형태)**

`CheckoutOrchestrator.Checkout`: 결제 승인 → 재고 확보 실패 시 **먼저 `CANCEL_REQUESTED` 를 기록하고** PG 취소를 부른다.
취소 성공 → `CANCELED`. 취소 실패 → `CANCEL_REQUESTED` 로 남고(`attempt_count = 1`, `next_retry_at = 지금 + CancelRetryDelay`) `Janitor.RetryCancels` 가 백오프하며 재시도 (취소는 포기할 수 없다).
`CANCEL_REQUESTED` 를 기록할 때부터 `next_retry_at` 을 "지금 + 지연"으로 둔다: 이 요청이 취소를 부르는 동안 janitor 가 같은 건을 집어 경쟁하지 않게.
"기록 먼저, 외부 호출 나중": 취소 호출 중 프로세스가 죽어도 기록이 남아 있으므로 janitor 가 이어서 처리한다.

## 스케줄 실행 (Janitor.Run + cmd)

`Janitor.Run(ctx, interval)` 은 ctx 가 취소될 때까지 `ReconcileUnknown(ctx, time.Now())` → `RetryCancels(ctx, time.Now())` 를 반복한다. `cmd/main.go` 가 이걸 프로세스로 띄운다 (Kotlin 은 `JanitorScheduler` 가 앱 안에서 `@Scheduled` 로 돈다).

```bash
docker compose -f ../infra/docker-compose.yml up -d postgres
cd go
JANITOR_INTERVAL=2s go run ./07-payment-state-machine/cmd
# 다른 터미널: 재확인 시각이 지난 UNKNOWN 한 건 (1시간 전 생성 → grace 지남)
psql postgres://lab:lab@localhost:15432/lab -c "SET search_path=go_07;
  INSERT INTO payments
  (id, order_id, amount, status, idempotency_key, next_retry_at, created_at)
  VALUES (gen_random_uuid(), 'o-1', 10000, 'UNKNOWN', 'idem-1',
          now() - interval '1 minute', now() - interval '1 hour')"
# 로그: janitor run: unknown{approved=0 failed=1 ...} cancel{...}
# Ctrl+C → Run 이 반환하고 "janitor stopped"
```

| 환경 변수 | 기본값 | 의미 |
|---|---|---|
| `DATABASE_URL` | `postgres://lab:lab@localhost:15432/lab?sslmode=disable` | 뒤에 `&search_path=go_07` 을 붙인다 (02/03 cmd 와 같은 방식, Kotlin 테이블과 안 섞이게). 그래서 `?` 가 들어 있어야 한다 |
| `JANITOR_INTERVAL` | `10s` | 한 바퀴가 **끝난 뒤** 쉬는 시간 (`time.ParseDuration` 형식: `500ms`, `2s`, `1m`). 0 이하는 거부 |

- Fake PG 는 프로세스 메모리에만 있다. cmd 는 결제를 승인하지 않으므로 밖에서 넣은 UNKNOWN 은 PG 조회 시 NOT_FOUND → grace 안이면 재예약, 지났으면 FAILED.
- 실행 요약: 무언가 처리한 바퀴만 `janitor run: ...` 한 줄 (표준 `log` 에 DEBUG 레벨이 없어서 빈 바퀴는 조용히 넘어간다).
- 에러가 나도 루프는 멈추지 않는다: 로그만 남기고 다음 주기에 다시 시도한다. UNKNOWN 쪽이 실패해도 취소 재시도는 실행한다.
- 종료: `signal.NotifyContext` 로 SIGINT/SIGTERM 이 ctx 를 취소 → `select` 의 `<-ctx.Done()` 이 interval 을 기다리지 않고 바로 반환.

**fixed delay 를 고른 이유.** 03 `OutboxRelay.Run` 과 같이 `time.After(interval)` 을 "다 돈 뒤" 기다린다. killbill 은 `scheduleAtFixedRate`(시작 시각 기준)를 쓰지만, PG 가 느려 한 바퀴가 길어지면 fixed rate 는 밀린 바퀴를 쉬지 않고 이어 붙여 장애 중인 PG 를 더 두드린다. 처리 대상은 `next_retry_at`(지수 백오프)이 정하므로 주기가 정확할 필요는 없다. (Go 에서 fixed rate 는 `time.Ticker` 다. Ticker 는 밀린 tick 을 하나만 버퍼에 남기고 버린다.)

**killbill 대응**

| killbill (`payment/core/janitor`) | 이 패키지 |
|---|---|
| [`Janitor.start()` `:110`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/Janitor.java#L110) `janitorExecutor.scheduleAtFixedRate(incompletePaymentAttemptTask, period, period, unit)` | `janitor.go:243` `Run` — `for` + `select { <-ctx.Done() / <-time.After(interval) }` (`:254`). 첫 바퀴는 바로 실행 |
| [`PaymentExecutors` `:102`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/PaymentExecutors.java#L102) `newSingleThreadScheduledExecutor("PaymentJanitor")` | `Run` 을 부르는 goroutine 하나 (`cmd/main.go:68` 은 main goroutine 에서 블록) |
| `PaymentConfig` `org.killbill.payment.janitor.rate` (기본 `1h`) | `JANITOR_INTERVAL` (기본 `10s`) |
| [`IncompletePaymentAttemptTask.run()` `:165`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L165), 항목별 `catch (Exception) → log.warn` `:184` | `janitor.go:261` `runOnce`: 에러는 `log.Printf` 후 계속 |
| `Janitor.stop()` `:113`: `isStopped` 플래그 + `shutdown()` + `awaitTermination(5s)` | ctx 취소 (`janitor.go:246` 시작 전 확인, `select` 에서 즉시 반환) |
| [`IncompletePaymentTransactionTask` `:63`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63) `TRANSACTION_STATUSES_TO_CONSIDER = PENDING, UNKNOWN` | `UNKNOWN` 과 `CANCEL_REQUESTED` 만. **PENDING 은 아직 안 본다** (한계 참고) |
| 재시도 시각: notification queue + [`getNextNotificationTime` `:402`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L402) (`unknown.retries` 기본 `5m,1h,1d,...`) | `next_retry_at` 컬럼 + 지수 백오프 |

주의: killbill 에서 fixed rate 로 도는 건 `IncompletePaymentAttemptTask`(INIT 상태로 남은 *attempt*)다. UNKNOWN/PENDING *transaction* 재확인은 `IncompletePaymentTransactionTask` 가 notification queue 예약 시각에 처리한다. 여기선 둘을 "주기 실행 + 행마다 `next_retry_at`" 하나로 합쳤다.

## 핵심 코드 위치

- `state.go:29` 허용 전이표, `state.go:69` `TransitionTx` — `:77` compare-and-set UPDATE, 이력 insert
- `payment_service.go:19` `Pay` — `:63` **타임아웃 → UNKNOWN** (FAILED 가 아닌 이유)
- `janitor.go:57` `ReconcileUnknown` — `:71` `FOR UPDATE SKIP LOCKED` + attempt_count +1, `:101` NOT_FOUND + grace(`:118` grace 안은 시도 안 씀), `:134` MANUAL_REVIEW, `:149` 지수 백오프(`janitor.go:39`, 지수 상한 `:33`, Base 가 커도 int64 최댓값에서 멈춤)
- `janitor.go:175` `RetryCancels` — 취소 재시도
- `janitor.go:243` `Run` — 스케줄 루프 (fixed delay `:254`, 취소 확인 `:246`), `:261` `runOnce` — 에러 로그 후 계속, 요약 로그 `:275`
- `cmd/main.go` — `DATABASE_URL` + `search_path=go_07`(`:35`), `JANITOR_INTERVAL`(`:38`), SIGINT/SIGTERM → ctx 취소(`:28`), `j.Run`(`:68`)
- `orchestrator.go:67` `Checkout` — `:89` 보상: 기록 먼저, 외부 호출 나중, `:106` 취소 실패 시 지연 후 재시도 예약
- `gateway.go` Fake PG — `TimeoutBeforeCharge`, `TimeoutAfterCharge`, `FailCancel`, `FailStatusQuery`

## 테스트 실행

```bash
cd go
go test ./07-payment-state-machine/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_invalid_transition__rejected` | REQUESTED→APPROVED, FAILED→APPROVED 거부, 상태/이력 변화 없음 |
| `Test_every_transition_recorded_in_history` | 성공 결제의 이력 = (∅→REQUESTED, REQUESTED→PENDING, PENDING→APPROVED) |
| `Test_concurrent_transition__only_one_wins` | goroutine 10개가 PENDING 에서 동시 전이 → 1개만 성공, 9개 `ErrStaleStatus`, 이력 1줄 |
| `Test_pg_timeout_after_charge__unknown__janitor_confirms_approved` | 승인 후 응답 유실 → UNKNOWN → janitor 가 APPROVED 확정, 재결제 없음 |
| `Test_pg_timeout_before_charge__unknown__janitor_marks_failed` | PG 미도달 → UNKNOWN → grace 내에는 보류(attempt_count 0 유지) → grace 후 FAILED |
| `Test_not_found_within_grace__does_not_consume_attempts__ends_failed` | `next_retry_at` 을 그대로 따라가며 janitor 실행 → MANUAL_REVIEW 가 아니라 FAILED, attempt_count = 1 (grace 후 확정 1회만 셈) |
| `Test_backoff__large_attempt__capped_without_overflow` | attempt 1~100 에서 백오프가 0 이하로 오버플로하지 않고 단조 증가, 상한 = Base << 20 |
| `Test_unknown_exceeds_max_attempts__moved_to_manual_review` | 조회 장애 지속 → 1분/2분 백오프 확인 → 3회째 MANUAL_REVIEW, 이후 janitor 가 건드리지 않음 |
| `Test_downstream_failure__compensated_by_pg_cancel` | 재고 실패 → PG 취소 → CANCELED |
| `Test_cancel_failure__left_cancel_requested__janitor_retries_cancel` | 취소 실패 → CANCEL_REQUESTED(attempt_count 1) → 지연 전엔 janitor 가 안 집음 → (장애 중 재예약) → 복구 후 janitor 가 CANCELED |
| `Test_run__reconciles_due_unknown_on_schedule` | `Run(ctx, 50ms)` 을 goroutine 으로. PG 엔 승인 기록, DB 엔 기한 지난 UNKNOWN → ReconcileUnknown 을 직접 부르지 않아도 APPROVED (폴링, 상한 10초). cancel() 후 Run 반환 |
| `Test_run__returns_promptly_when_ctx_cancelled` | interval 1시간이어도 ctx 취소 시 바로 반환 |

janitor 는 `now` 를 인자로 받으므로 테스트가 시간을 직접 진행시킨다 (sleep 없음). `Run` 테스트만 실제 시계(`time.Now()`)로 돌고, 고정 sleep 대신 조건이 맞을 때까지 폴링한다.

## 운영 쿼리

```sql
-- 상태별 건수
SELECT status, count(*) FROM payments GROUP BY status ORDER BY status;

-- 오래된 UNKNOWN (janitor 가 멈췄거나 PG 조회 API 장애 의심)
SELECT id, order_id, amount, attempt_count, next_retry_at, last_error, now() - created_at AS age
  FROM payments
 WHERE status = 'UNKNOWN' AND created_at < now() - interval '10 minutes'
 ORDER BY created_at;

-- MANUAL_REVIEW 목록 (사람이 PG 어드민에서 idempotency_key 로 조회해 처리)
SELECT id, order_id, amount, idempotency_key, attempt_count, last_error, updated_at
  FROM payments WHERE status = 'MANUAL_REVIEW' ORDER BY updated_at;

-- 취소가 오래 안 끝나는 건 (돈을 돌려줘야 하는데 못 돌려준 상태)
SELECT id, order_id, pg_tx_id, attempt_count, last_error, next_retry_at
  FROM payments WHERE status = 'CANCEL_REQUESTED' AND updated_at < now() - interval '30 minutes';

-- 한 결제의 전체 이력
SELECT from_status, to_status, reason, created_at
  FROM payment_status_history WHERE payment_id = :id ORDER BY id;
```

## 한계 / 다음 단계

- `REQUESTED → PENDING` 후 PG 호출 전에 프로세스가 죽으면 PENDING 에 남는다. **다음 단계**: `Run` 이 "오래된 PENDING" 도 UNKNOWN 처럼 PG 조회로 정리한다. killbill 은 [`TRANSACTION_STATUSES_TO_CONSIDER = PENDING, UNKNOWN`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63) 으로 둘 다 보고, PENDING 은 별도 재시도 간격(`org.killbill.payment.janitor.pending.retries`, 기본 `1h, 1d`)을 쓴다. PENDING 엔 `next_retry_at` 이 없으므로 `updated_at < now - pendingGrace` 로 집는 조건이 필요하다.
- janitor 는 PG 조회를 트랜잭션(행 잠금) 안에서 한다. 조회는 상태를 바꾸지 않아서 괜찮지만, PG 가 느리면 잠금을 오래 쥔다. 대량이면 "집기(claim) → 커밋 → 조회 → 확정" 으로 나눈다 (Kotlin 버전이 이 방식).
- 같은 이유로, janitor 가 처리 도중 죽으면 트랜잭션이 롤백되어 집을 때 올린 `attempt_count` 도 사라진다. 처리할 때마다 죽는 행은 MANUAL_REVIEW 로 가지 않고 계속 재시도된다. Kotlin 은 claim 을 먼저 커밋하므로 이 경우도 시도로 세어 `maxAttempts` 초과 시 보류한다.
- UNKNOWN 상태에서는 다음 단계(재고)로 가지 않는다. janitor 가 APPROVED 로 확정한 뒤 이어서 처리하는 후속 트리거가 필요하다 (outbox 이벤트, 03).
- MANUAL_REVIEW 이후의 전이(사람이 승인/실패 확정)는 운영 도구의 몫으로 남겼다.
- 대사(09)는 이 모든 로직이 놓친 것을 마지막으로 잡는 안전망이다.
