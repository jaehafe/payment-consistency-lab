# 07. 결제 상태 머신 · 보류 · 보상 · 재처리

## 문제

- 결제 상태를 `status = 'APPROVED'` 처럼 아무 데서나 바꾸면, "실패한 결제가 승인으로 되살아남" 같은 불가능한 전이가 생긴다.
- PG 호출이 **타임아웃**나면 승인됐는지 모른다. 실패로 단정하고 재결제를 유도하면 **이중 결제**.
- 결제는 승인됐는데 **다음 단계(재고 확보)가 실패**하면? PG 승인과 재고는 한 DB 트랜잭션으로 묶을 수 없다.
- 보상(취소)마저 실패하면?
- `PENDING` 을 커밋한 뒤 PG 응답을 기록하기 전에 프로세스가 죽으면? 결제가 `PENDING` 에 영원히 남는다.

## 해결

**1) 명시적 상태 머신**

```mermaid
stateDiagram-v2
    [*] --> REQUESTED
    REQUESTED --> PENDING: PG 호출 직전
    PENDING --> APPROVED: 승인 / janitor - 오래된 PENDING, PG 조회 승인
    PENDING --> FAILED: 명확한 거절 / janitor - 오래된 PENDING, 거절 또는 grace 후 NOT_FOUND
    PENDING --> UNKNOWN: 타임아웃 (결과 모름) / janitor - 오래된 PENDING, grace 안 NOT_FOUND 또는 조회 불가
    UNKNOWN --> APPROVED: janitor - PG 조회 결과 승인
    UNKNOWN --> FAILED: janitor - 거절 또는 grace 후 NOT_FOUND
    UNKNOWN --> MANUAL_REVIEW: janitor - maxAttempts 초과 (보류)
    APPROVED --> CANCEL_REQUESTED: 후속 단계 실패 → 보상 시작
    CANCEL_REQUESTED --> CANCELED: PG 취소 성공 (즉시 또는 janitor 재시도)
    FAILED --> CANCEL_REQUESTED: janitor 가 FAILED 로 확정한 뒤 PG 승인 도착 → 망취소
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

**3) 오래된 PENDING 정리 (`Janitor.SweepPending`)**

프로세스가 PG 호출 도중 죽은 결제를 정리한다.
- `created_at <= now - PendingAfter`(cmd 기본 2분)인 PENDING 만 집는다. 그보다 젊은 PENDING 은 요청이 아직 PG 응답을 기다리는 중일 수 있다. killbill 이 `createdDateBefore` cutoff 로 "충분히 오래된 것"만 보는 것과 같다 (아래 비교). `PendingAfter <= 0` 이면 진행 중인 요청까지 집게 되므로 에러로 거부한다.
- claim 은 `ReconcileUnknown` 과 같다 (`FOR UPDATE SKIP LOCKED` + `attempt_count` +1, 트랜잭션 안에서 PG 조회). PENDING 엔 `next_retry_at` 이 없어서 `created_at` 으로 고른다.
- `APPROVED` → APPROVED(+`pg_tx_id`), `FAILED` → FAILED.
- `NOT_FOUND` → UNKNOWN 과 **같은 grace 규칙**. `created_at + NotFoundGrace` 가 지났으면 FAILED (PG 에 도달하기 전에 죽음, 돈 안 빠짐). 아직이면 UNKNOWN 으로 넘기고 `next_retry_at = grace 끝`, 시도로 세지 않는다.
- 판단 불가(조회 장애) → **UNKNOWN 으로 넘긴다** (`next_retry_at = now + 백오프`, 1회로 셈). 이후 재시도·MANUAL_REVIEW 는 `ReconcileUnknown` 하나가 맡는다. 이유는 아래 비교 참고.
- PG 호출(조회) 1회에는 `PgCallTimeout`(기본 5초) 제한이 걸린다. janitor 는 행 잠금을 쥔 채 PG 를 부르므로, PG 가 응답하지 않으면 그 행을 기다리는 요청까지 같이 멈춘다. 시간이 지나면 판단 불가로 보고 UNKNOWN 으로 넘긴다 (`RetryCancels` 의 취소 호출도 같은 제한, 실패로 보고 재예약).
- 요청과의 경쟁. 요청이 먼저 커밋했으면 janitor 의 `WHERE status = 'PENDING'` 에 안 걸리므로 집지 않는다. janitor 가 먼저면, 요청의 CAS 는 janitor 가 커밋할 때까지 행 잠금을 기다렸다가 0행(`ErrStaleStatus`)이 된다. 문제는 janitor 가 본 PG 기록이 **그 시각의** 것이라는 점이다. 요청이 PG 앞에서 멈춰 있었다면 janitor 는 `NOT_FOUND` 를 보고 UNKNOWN(grace 안)이나 FAILED(grace 후)로 정한다. 그 뒤 요청이 깨어나 PG 승인을 받으면 **결론이 다르다.** 예전 코드는 이때 `ErrStaleStatus` 를 삼키고 FAILED 를 돌려줬다. 돈은 빠졌는데 고객은 "실패"를 보고 재결제 → 이중 결제.
- 그래서 `Pay` 는 `ErrStaleStatus` 면 **다시 읽고, 요청이 PG 에서 받은 결과와 맞춘다** (`reconcileAfterStale`):

  | 요청이 아는 것 | 저장된 상태 | 처리 |
  |---|---|---|
  | 승인(`pgTxID`) | APPROVED | 그대로 (같은 결론, 추가 전이 없음) |
  | 승인(`pgTxID`) | UNKNOWN | `UNKNOWN → APPROVED` + `pg_tx_id` |
  | 승인(`pgTxID`) | FAILED | **망취소**: `FAILED → CANCEL_REQUESTED` 커밋 → PG 취소 → `CANCELED`. 취소 실패면 `CANCEL_REQUESTED` 로 남아 `RetryCancels` 가 재시도. ERROR 로그 |
  | 거절 | UNKNOWN | `UNKNOWN → FAILED` |
  | 거절 | FAILED | 그대로 |
  | 모름(타임아웃) | 무엇이든 | 그대로 (janitor 가 더 많이 안다) |
  | 그 외 (MANUAL_REVIEW 등) | | 그대로 + WARN 로그 (사람 몫) |

  맞추는 전이도 `ReconcileUnknown` 과 또 부딪힐 수 있어 다시 읽기를 최대 3번 한다.
- **왜 FAILED 를 되살리지(→ APPROVED) 않고 취소하나.** FAILED 는 이미 밖으로 나간 결론이다. 고객은 결제 실패를 보고 다시 결제했을 수 있다. 되살리면 그 결제와 합쳐 이중 결제가 된다. 취소는 최악이라도 "고객이 한 번 더 결제한다"로 끝난다. 그래서 전이표에 `FAILED → CANCEL_REQUESTED` 하나만 추가했다 (늦은 승인 전용). `FAILED → APPROVED` 는 여전히 금지다. 국내 PG 연동의 **망취소**(net-cancel)와 같은 생각이다: 승인은 났는데 가맹점이 그 결과를 반영하지 못했으면(타임아웃, DB 오류) 가맹점이 그 승인을 바로 취소한다.

**4) 보상 (Saga 의 가장 작은 형태)**

`CheckoutOrchestrator.Checkout`: 결제 승인 → 재고 확보 실패 시 **먼저 `CANCEL_REQUESTED` 를 기록하고** PG 취소를 부른다.
취소 성공 → `CANCELED`. 취소 실패 → `CANCEL_REQUESTED` 로 남고(`attempt_count = 1`, `next_retry_at = 지금 + CancelRetryDelay`) `Janitor.RetryCancels` 가 백오프하며 재시도 (취소는 포기할 수 없다).
`CANCEL_REQUESTED` 를 기록할 때부터 `next_retry_at` 을 "지금 + 지연"으로 둔다: 이 요청이 취소를 부르는 동안 janitor 가 같은 건을 집어 경쟁하지 않게.
"기록 먼저, 외부 호출 나중": 취소 호출 중 프로세스가 죽어도 기록이 남아 있으므로 janitor 가 이어서 처리한다.

## 스케줄 실행 (Janitor.Run + cmd)

`Janitor.Run(ctx, interval)` 은 ctx 가 취소될 때까지 `SweepPending` → `ReconcileUnknown` → `RetryCancels` (모두 `now = time.Now()`) 를 반복한다. PENDING 을 먼저 본다. 판단 불가로 UNKNOWN 에 넘긴 건은 `next_retry_at`(백오프 뒤)이 되면 UNKNOWN 쪽이 다시 본다. `cmd/main.go` 가 이걸 프로세스로 띄운다 (Kotlin 은 `JanitorScheduler` 가 앱 안에서 `@Scheduled` 로 돈다).

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
# 로그: janitor run: pending{...} unknown{approved=0 failed=1 ...} cancel{...}
# 크래시로 남은 PENDING 한 건 (1시간 전 생성 → PendingAfter·grace 모두 지남)
psql postgres://lab:lab@localhost:15432/lab -c "SET search_path=go_07;
  INSERT INTO payments
  (id, order_id, amount, status, idempotency_key, created_at)
  VALUES (gen_random_uuid(), 'p-1', 10000, 'PENDING', 'idem-p-1',
          now() - interval '1 hour')"
# 로그: janitor run: pending{approved=0 failed=1 toUnknown=0} ...
# Ctrl+C → Run 이 반환하고 "janitor stopped"
```

| 환경 변수 | 기본값 | 의미 |
|---|---|---|
| `DATABASE_URL` | `postgres://lab:lab@localhost:15432/lab?sslmode=disable` | 뒤에 `&search_path=go_07` 을 붙인다 (02/03 cmd 와 같은 방식, Kotlin 테이블과 안 섞이게). 그래서 `?` 가 들어 있어야 한다 |
| `JANITOR_INTERVAL` | `10s` | 한 바퀴가 **끝난 뒤** 쉬는 시간 (`time.ParseDuration` 형식: `500ms`, `2s`, `1m`). 0 이하는 거부 |

- cmd 의 janitor 설정은 코드에 고정: `MaxAttempts 5`, `BaseBackoff 10s`, `NotFoundGrace 5m`, `PendingAfter 2m` (Kotlin `application.yml` 기본값과 같다).
- Fake PG 는 프로세스 메모리에만 있다. cmd 는 결제를 승인하지 않으므로 밖에서 넣은 UNKNOWN/PENDING 은 PG 조회 시 NOT_FOUND → grace 안이면 재예약(PENDING 은 UNKNOWN 으로), 지났으면 FAILED.
- 실행 요약: 무언가 처리한 바퀴만 `janitor run: ...` 한 줄 (표준 `log` 에 DEBUG 레벨이 없어서 빈 바퀴는 조용히 넘어간다).
- 에러가 나도 루프는 멈추지 않는다: 로그만 남기고 다음 주기에 다시 시도한다. 세 작업은 독립적이라 한쪽이 실패해도 나머지는 실행한다.
- 종료: `signal.NotifyContext` 로 SIGINT/SIGTERM 이 ctx 를 취소 → `select` 의 `<-ctx.Done()` 이 interval 을 기다리지 않고 바로 반환.

**fixed delay 를 고른 이유.** 03 `OutboxRelay.Run` 과 같이 `time.After(interval)` 을 "다 돈 뒤" 기다린다. killbill 은 `scheduleAtFixedRate`(시작 시각 기준)를 쓰지만, PG 가 느려 한 바퀴가 길어지면 fixed rate 는 밀린 바퀴를 쉬지 않고 이어 붙여 장애 중인 PG 를 더 두드린다. 처리 대상은 `next_retry_at`(지수 백오프)이 정하므로 주기가 정확할 필요는 없다. (Go 에서 fixed rate 는 `time.Ticker` 다. Ticker 는 밀린 tick 을 하나만 버퍼에 남기고 버린다.)

**killbill 대응**

| killbill (`payment/core/janitor`) | 이 패키지 |
|---|---|
| [`Janitor.start()` `:110`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/Janitor.java#L110) `janitorExecutor.scheduleAtFixedRate(incompletePaymentAttemptTask, period, period, unit)` | `janitor.go:388` `Run` — `for` + `select { <-ctx.Done() / <-time.After(interval) }` (`:399`). 첫 바퀴는 바로 실행 |
| [`PaymentExecutors` `:102`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/PaymentExecutors.java#L102) `newSingleThreadScheduledExecutor("PaymentJanitor")` | `Run` 을 부르는 goroutine 하나 (`cmd/main.go:70` 은 main goroutine 에서 블록) |
| `PaymentConfig` `org.killbill.payment.janitor.rate` (기본 `1h`) | `JANITOR_INTERVAL` (기본 `10s`) |
| [`IncompletePaymentAttemptTask.run()` `:165`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L165), 항목별 `catch (Exception) → log.warn` `:184` | `janitor.go:408` `runOnce`: 에러는 `log.Printf` 후 계속 |
| `Janitor.stop()` `:113`: `isStopped` 플래그 + `shutdown()` + `awaitTermination(5s)` | ctx 취소 (`janitor.go:391` 시작 전 확인, `select` 에서 즉시 반환) |
| [`IncompletePaymentTransactionTask` `:63`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63) `TRANSACTION_STATUSES_TO_CONSIDER = PENDING, UNKNOWN` | `SweepPending`(PENDING) + `ReconcileUnknown`(UNKNOWN) + `RetryCancels` |
| 재시도 시각: notification queue + [`getNextNotificationTime` `:402`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L402) (`unknown.retries` 기본 `5m,1h,1d,...`, `pending.retries` 기본 `1h,1d`) | `next_retry_at` 컬럼 + 지수 백오프. PENDING 은 첫 확인만 `PendingAfter`, 이후엔 UNKNOWN 일정 |

### PENDING 정리: killbill 과 비교

killbill 코드는 모두 커밋 [`52e2f55`](https://github.com/killbill/killbill/tree/52e2f55f103baf0703ddee71e10a7036595fc851) 기준이다.

| killbill | 이 패키지 |
|---|---|
| [`IncompletePaymentTransactionTask.java:63`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63) `TRANSACTION_STATUSES_TO_CONSIDER = List.of(PENDING, UNKNOWN)` | `SweepPending` 이 PENDING 을, `ReconcileUnknown` 이 UNKNOWN 을 본다 |
| [`IncompletePaymentAttemptTask.java:151`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L151) `processPaymentEvent`: PENDING/UNKNOWN 결제 이벤트가 나면 notification 을 attempt 0 으로 예약. 첫 확인은 `pending.retries[0]` = 1h 뒤 | 예약 없이 `created_at <= now - PendingAfter`(2분)로 고른다. 이벤트를 못 남기고 죽은 경우도 잡힌다 |
| [`IncompletePaymentAttemptTask.java:198`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L198) INIT attempt 조회에 `getCreatedDateBefore()` 를 넘긴다. [`:420-422`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L420-L422) `clock.getUTCNow().minusMillis(attempts.delay)`, [`PaymentConfig.java:107-110`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/util/src/main/java/org/killbill/billing/util/config/definition/PaymentConfig.java#L107-L110) `attempts.delay` 기본 `12h` | claim 쿼리의 `created_at <= $1` (`$1 = now.Add(-j.PendingAfter)`). 같은 생각이다: "진행 중일 수 있는 건 건드리지 않는다. 충분히 오래된 것만 고친다" |
| [`IncompletePaymentAttemptTask.java:402-417`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L402-L417) `getNextNotificationTime`: UNKNOWN 이면 `getUnknownTransactionsRetries`, PENDING 이면 `getPendingTransactionsRetries` ([`PaymentConfig.java:61-64`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/util/src/main/java/org/killbill/billing/util/config/definition/PaymentConfig.java#L61-L64) `5m,1h,1d,1d,1d,1d,1d`, [`:71-74`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/util/src/main/java/org/killbill/billing/util/config/definition/PaymentConfig.java#L71-L74) `1h, 1d`). `:413-414` 횟수를 다 쓰면 `null` → 예약 중단 | 일정은 하나(지수 백오프)다. PENDING 은 첫 확인에서 결론이 안 나면 UNKNOWN 으로 넘어가 UNKNOWN 일정을 따른다. 횟수를 다 쓰면 MANUAL_REVIEW(보류)로 **눈에 보이게** 남긴다 |
| [`IncompletePaymentTransactionTask.java:224-228`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L224-L228) 플러그인이 UNKNOWN 을 답하면 현재 상태를 유지. [`:182-187`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L182-L187) "Unable to repair" 후 반환, [`:190-194`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L190-L194) 상태가 그대로면 다음 notification 만 예약 → PENDING 은 PENDING 일정으로 재시도 | 판단 불가면 `PENDING → UNKNOWN` (`pendingToUnknown`) |
| [`IncompletePaymentTransactionTask.java:141-145`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L141-L145) 계정 락(`:124`)을 잡은 뒤 다시 읽어서, 그 사이 PENDING/UNKNOWN 이 아니게 됐으면 아무것도 안 함 | 행 잠금(`FOR UPDATE`) + 잠근 뒤 `status = 'PENDING'` 재검사. 요청 쪽은 CAS 0행(`ErrStaleStatus`) → `Pay` 가 다시 읽어 자기가 받은 PG 결과와 맞춘다 (UNKNOWN 이면 확정, FAILED 인데 승인이면 망취소) |

**왜 판단 불가 PENDING 을 UNKNOWN 으로 넘기나 (killbill 처럼 PENDING 일정으로 재시도하지 않고).**
1. 의미가 다르다. killbill 의 PENDING 은 정상 상태일 수 있다: 플러그인이 "처리 중"이라고 답한 비동기 결제(계좌이체, 3DS 등)는 몇 시간~며칠 PENDING 이다. 그래서 별도의 느린 일정(`1h, 1d`)이 맞다. 이 패키지의 PENDING 은 "PG 호출 중"뿐이고 Fake PG 는 처리 중 상태를 답하지 않는다. `PendingAfter` 가 지났는데 PG 가 결론을 안 주면 그건 "결과를 모름", 즉 UNKNOWN 의 정의 그대로다.
2. 재시도 정책이 하나로 모인다. 백오프, `MaxAttempts`, MANUAL_REVIEW, grace 규칙을 PENDING 용으로 따로 만들지 않는다. PENDING 용 전이도 추가하지 않는다 (`PENDING → MANUAL_REVIEW` 없음).
3. 운영에서 보인다. 운영 쿼리의 "오래된 UNKNOWN" 에 같이 잡힌다. PENDING 에 머무르면 "진행 중"과 "버려짐"을 구별할 수 없다.
4. Kotlin 과 같은 규칙이다 (`sweepPending` → `toUnknown`).

grace 안의 `NOT_FOUND` 도 같은 이유로 UNKNOWN 으로 넘긴다 (시도로 세지 않음). 그러면 "오래된 PENDING 은 결론이 나면 확정하고, 아니면 UNKNOWN" 이라는 규칙 하나가 된다.

주의: killbill 에서 fixed rate 로 도는 건 `IncompletePaymentAttemptTask`(INIT 상태로 남은 *attempt*)다. UNKNOWN/PENDING *transaction* 재확인은 `IncompletePaymentTransactionTask` 가 notification queue 예약 시각에 처리한다. 여기선 둘을 "주기 실행 + 행마다 `next_retry_at`" 하나로 합쳤다.

## 핵심 코드 위치

- `state.go:29` 허용 전이표 (`FAILED → CANCEL_REQUESTED` 는 늦은 승인 망취소 전용), `state.go:73` `TransitionTx` — `:81` compare-and-set UPDATE, 이력 insert
- `payment_service.go:25` `Pay` — `recordChargeResult`(`:56`), **타임아웃 → UNKNOWN**(`:70`, FAILED 가 아닌 이유), janitor 가 먼저 정했으면 맞추기(`reconcileAfterStale` `:106`), 늦은 승인 망취소(`netCancel` `:162`)
- `janitor.go:46` `SweepPending` — `:48` `PendingAfter <= 0` 거부, `:64` `created_at` cutoff + `FOR UPDATE SKIP LOCKED`, `:99` NOT_FOUND + grace(`:113` grace 안은 UNKNOWN·시도 안 씀), `:119` 판단 불가 → UNKNOWN, `:137` `pendingToUnknown`
- `janitor.go:176` `ReconcileUnknown` — `:190` `FOR UPDATE SKIP LOCKED` + attempt_count +1, `:220` NOT_FOUND + grace(`:237` grace 안은 시도 안 씀), `:253` MANUAL_REVIEW, `:268` 지수 백오프(`janitor.go:158`, 지수 상한 `:152`, Base 가 커도 int64 최댓값에서 멈춤)
- `janitor.go:294` `RetryCancels` — 취소 재시도
- `janitor.go:349` `getStatus`/`cancel` — PG 호출 1회에 `PgCallTimeout`(기본 5초, `:361`)
- `janitor.go:388` `Run` — 스케줄 루프 (fixed delay `:399`, 취소 확인 `:391`), `:408` `runOnce` — PENDING 먼저(`:409`), 에러 로그 후 계속, 요약 로그 `:428`
- `cmd/main.go` — `DATABASE_URL` + `search_path=go_07`(`:34`), `JANITOR_INTERVAL`(`:38`), SIGINT/SIGTERM → ctx 취소(`:29`), `PendingAfter`(`:67`), `j.Run`(`:70`)
- `orchestrator.go:67` `Checkout` — `:89` 보상: 기록 먼저, 외부 호출 나중, `:106` 취소 실패 시 지연 후 재시도 예약
- `gateway.go` Fake PG — `TimeoutBeforeCharge`, `TimeoutAfterCharge`, `FailCancel`, `FailStatusQuery`, 테스트 훅 `OnBeforeCharge`/`OnCharge`/`OnGetStatus`(`:39`). 조회·취소는 ctx 가 끝났으면 `ctx.Err()` 를 돌려준다 (janitor 의 `PgCallTimeout`)

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
| `Test_run__settles_stale_pending_on_schedule` | `Run(ctx, 50ms)`. 3분 전 생성된 PENDING + PG 승인 기록 → SweepPending 을 직접 부르지 않아도 APPROVED + `pg_tx_id` |
| `Test_stuck_pending__pg_approved__janitor_marks_approved` | 크래시로 남은 PENDING, PG 엔 승인 기록 → APPROVED + `pg_tx_id`, `next_retry_at` NULL, 재결제 없음 |
| `Test_stuck_pending__no_pg_record__failed_after_grace` | PG 기록 없음, 3분 뒤(grace 안) → UNKNOWN(attempt 0, `next_retry_at = created_at + 5분`) → grace 끝에 ReconcileUnknown 이 FAILED |
| `Test_stuck_pending__no_pg_record_past_grace__failed_directly` | grace 도 지났으면 `PENDING → FAILED` 바로 |
| `Test_pending_younger_than_threshold__not_touched` | `PendingAfter` 보다 1초 젊은 PENDING 은 PG 승인 기록이 있어도 PG 조회도 안 하고 attempt/`next_retry_at`/이력 그대로. 대조군(3분 전)만 집힘 |
| `Test_stuck_pending__pg_query_unavailable__handed_to_unknown_flow` | 조회 장애 → UNKNOWN(attempt 1, 백오프 1분) → 복구 후 ReconcileUnknown 이 APPROVED |
| `Test_pending_race__janitor_settles_first__request_gets_no_error` | `Pay` 가 PG 응답을 받기 직전(`OnCharge` 훅) janitor 가 APPROVED 로 확정 → `Pay` 는 에러 없이 APPROVED, 이력은 REQUESTED·PENDING·APPROVED 3건뿐 (요청은 추가 전이 없음) |
| `Test_pending_race__request_waits_for_janitor_lock__then_adopts_settled` | janitor 가 PG 에 묻는 사이(`OnGetStatus` 훅) 요청 goroutine 이 PG 승인을 기록하려 함. 훅은 `pg_stat_activity` 에 `wait_event_type = 'Lock'` 인 backend 가 보일 때까지 기다린다 (= 요청이 정말 janitor 의 행 잠금을 기다림). janitor 커밋 후 요청은 0행 → 다시 읽어 같은 결론 → 에러 없이 APPROVED, 이력 1건 |
| `Test_sweep_pending__zero_threshold__rejected` | `PendingAfter = 0` 이면 에러, 아무것도 안 건드림 |
| `Test_late_pg_response__janitor_moved_to_unknown__request_approved__approved` | 요청이 PG 앞에서 멈춘 사이(`OnBeforeCharge` 훅) janitor 가 NOT_FOUND(grace 안) → UNKNOWN. 요청이 승인을 받으면 `UNKNOWN → APPROVED` + `pg_tx_id`, `next_retry_at` NULL |
| `Test_late_pg_response__janitor_moved_to_unknown__request_declined__failed` | 같은 상황에서 PG 거절 → `UNKNOWN → FAILED` |
| `Test_late_pg_response__janitor_failed__request_approved__net_canceled` | janitor 가 grace 후 NOT_FOUND → FAILED. 그 뒤 요청이 승인을 받으면 `FAILED → CANCEL_REQUESTED → CANCELED`, PG 에서 실제 취소 (예전엔 FAILED 그대로 + 돈 빠짐) |
| `Test_late_pg_response__janitor_failed__net_cancel_fails__janitor_retries` | 망취소마저 실패 → `CANCEL_REQUESTED`(attempt 1, `next_retry_at` 있음) → 복구 후 `RetryCancels` 가 CANCELED |
| `Test_late_pg_response__janitor_failed__request_timed_out__keeps_stored` | 요청도 타임아웃(결과 모름) → 저장된 FAILED 그대로, 추가 전이 없음 |
| `Test_sweep_pending__pg_status_query_hangs__timeout_is_inconclusive` | `PgCallTimeout = 50ms`, 조회가 500ms 멈춤 → 판단 불가 → UNKNOWN, `last_error` 에 `context deadline exceeded` |

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

-- PendingAfter 보다 훨씬 오래된 PENDING (janitor 프로세스가 안 돌고 있음)
SELECT id, order_id, amount, attempt_count, now() - created_at AS age
  FROM payments
 WHERE status = 'PENDING' AND created_at < now() - interval '10 minutes'
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

- ~~`REQUESTED → PENDING` 후 PG 응답 기록 전에 프로세스가 죽으면 PENDING 에 남던 문제~~ → 고침: `SweepPending` 이 `PendingAfter` 보다 오래된 PENDING 을 PG 조회로 정리한다 (위 "PENDING 정리: killbill 과 비교").
- `PendingAfter` 는 PG 호출 타임아웃보다 길어야 한다. 짧으면 janitor 가 진행 중인 요청과 자주 경쟁한다. 그때마다 요청이 janitor 의 행 잠금을 기다리고, 결과를 맞추는 추가 전이(UNKNOWN → APPROVED)나 망취소(FAILED → CANCEL_REQUESTED)가 생겨 고객에게는 결제가 취소로 끝난다. 길면 크래시한 결제가 그만큼 늦게 정리된다. 같은 이유로 `NotFoundGrace` 도 요청 하나가 살아 있을 수 있는 최대 시간(PG 호출 타임아웃 포함)보다 길어야 한다.
- janitor 가 `NOT_FOUND` 로 FAILED 처리한 뒤 PG 가 늦게 승인하는 경우, 요청이 아직 살아 있으면 망취소로 정리된다 (위 표). 요청도 이미 죽었거나(PG 승인 직후 프로세스 다운) PG 내부 지연으로 늦게 승인되면 아무도 그 승인을 모른다 → FAILED + 돈 빠짐이 남는다. 이건 09 대사(MISSING_IN_INTERNAL)가 잡는다.
- `Pay` 의 `Charge` 호출 자체에는 아직 제한 시간이 없다 (Fake PG 는 ctx 를 보지 않음). 실제 PG 클라이언트에선 `PendingAfter` 보다 짧은 타임아웃을 걸어야 위 경쟁이 드물어진다.
- 기준이 `created_at`(REQUESTED 생성 시각)이다. REQUESTED → PENDING 이 곧바로 이어지므로 PENDING 진입 시각과 거의 같다. 둘 사이에 긴 단계가 생기면 PENDING 진입 시각 컬럼이 필요하다.
- PENDING 이 정상적으로 오래 머무는 결제(비동기 계좌이체 등)가 생기면 "판단 불가 → UNKNOWN" 규칙이 맞지 않는다. 그때는 killbill 처럼 PG 가 "처리 중"이라고 답하면 PENDING 을 유지하고 별도 일정으로 재시도해야 한다.
- janitor 는 PG 조회를 트랜잭션(행 잠금) 안에서 한다. 조회는 상태를 바꾸지 않아서 괜찮지만, PG 가 느리면 잠금을 오래 쥔다. 대량이면 "집기(claim) → 커밋 → 조회 → 확정" 으로 나눈다 (Kotlin 버전이 이 방식).
- 같은 이유로, janitor 가 처리 도중 죽으면 트랜잭션이 롤백되어 집을 때 올린 `attempt_count` 도 사라진다. 처리할 때마다 죽는 행은 MANUAL_REVIEW 로 가지 않고 계속 재시도된다 (PENDING 도 같다). Kotlin 은 claim 을 먼저 커밋하므로 이 경우도 시도로 세어 `maxAttempts` 초과 시 보류한다 (PENDING 은 UNKNOWN 으로 넘긴 뒤 보류).
- UNKNOWN 상태에서는 다음 단계(재고)로 가지 않는다. janitor 가 APPROVED 로 확정한 뒤 이어서 처리하는 후속 트리거가 필요하다 (outbox 이벤트, 03).
- MANUAL_REVIEW 이후의 전이(사람이 승인/실패 확정)는 운영 도구의 몫으로 남겼다.
- 대사(09)는 이 모든 로직이 놓친 것을 마지막으로 잡는 안전망이다.
