# 07. 결제 상태 머신 · 보류 · 보상 · 재처리

## 문제

- 결제 상태를 아무 코드에서나 `UPDATE status = ...` 하면, "승인된 결제가 실패로 바뀌는" 식의 불가능한 전이가 생긴다.
- PG 타임아웃을 **실패로 단정**하면, 실제로는 승인됐는데 주문은 실패 처리된다 → 고객 돈만 빠지고, 재결제하면 이중 결제.
- 결제는 승인됐는데 다음 단계(재고 확보)가 실패하면? PG 승인은 DB 롤백으로 되돌릴 수 없다.
- 확정이 안 되는 결제를 무한히 재시도할 수도, 그냥 버릴 수도 없다.
- `PENDING`을 커밋한 뒤 PG 응답을 기록하기 전에 서버가 죽으면? 결제가 `PENDING`에 영원히 남는다.

## 해결

```mermaid
stateDiagram-v2
    [*] --> REQUESTED
    REQUESTED --> PENDING: PG 승인 요청
    PENDING --> APPROVED: 승인 응답 / janitor - 오래된 PENDING, PG 조회 승인
    PENDING --> FAILED: 거절 응답 / janitor - 오래된 PENDING, PG 조회 실패·유예 후 기록 없음
    PENDING --> UNKNOWN: 타임아웃/네트워크 오류 / janitor - 오래된 PENDING, 유예 안 NOT_FOUND·조회 불가
    UNKNOWN --> APPROVED: janitor - PG 조회 승인
    UNKNOWN --> FAILED: janitor - PG 조회 실패 / 유예 후에도 기록 없음
    UNKNOWN --> MANUAL_REVIEW: maxAttempts 초과 (보류)
    APPROVED --> CANCEL_REQUESTED: 후속 단계 실패 → 보상
    CANCEL_REQUESTED --> CANCELED: PG 취소 성공 (실패 시 janitor 재시도)
    FAILED --> CANCEL_REQUESTED: janitor가 FAILED로 확정한 뒤 PG 승인이 도착 → 망취소
    FAILED --> [*]
    CANCELED --> [*]
    MANUAL_REVIEW --> [*]: 사람이 확인
```

1. **허용 전이표**를 코드에 명시 (`PaymentStatus.ALLOWED`). 표에 없는 전이는 `IllegalTransitionException`.
2. **compare-and-set 전이**: `UPDATE ... SET status = ? WHERE id = ? AND status = ?` + history insert를 한 트랜잭션에서. 0행이면 다른 처리가 먼저 바꿨다는 뜻 → 덮어쓰지 않고 실패.
3. **UNKNOWN**: 타임아웃은 "모름". janitor가 PG 조회 API로 확정한다.
   - `NOT_FOUND`라도 바로 FAILED로 보지 않는다 — 요청이 아직 PG에서 처리 중일 수 있다. `created_at + not-found-grace`가 지나야 FAILED.
   - 유예 시간 안의 `NOT_FOUND`는 **시도 횟수를 쓰지 않고** 유예가 끝나는 시각(`created_at + not-found-grace`)으로 바로 재예약한다. 시도로 세면 백오프 합(10+20+40+80s = 150s)이 유예(5m)보다 짧아서, 돈이 안 빠진 결제가 유예가 끝나기도 전에 MANUAL_REVIEW로 넘어간다.
   - 결론을 못 내면(PG 조회 장애 등) 지수 백오프(10s → 20s → 40s → 80s, 지수 상한 20)로 다시 확인. `max-attempts`회째에도 결론이 없으면 **MANUAL_REVIEW**(보류) — 사람이 PG 어드민이나 정산 파일(09)로 확인.
   - janitor는 `FOR UPDATE SKIP LOCKED`로 행을 고르고 `next_retry_at`을 lease만큼 미루면서 `attempt_count`를 1 올린 뒤 커밋(claim) → PG 조회 동안 잠금을 쥐지 않으면서도 다른 janitor와 겹치지 않는다.
   - claim에서 시도를 세므로 처리 도중 죽어도 시도로 남는다. `max-attempts`보다 많이 집힌 행(= 매번 처리하다 죽음)은 PG를 부르지 않고 MANUAL_REVIEW로 보낸다.
4. **오래된 PENDING 정리** (`sweepPending`): 서버가 PG 호출 도중 죽은 결제.
   - `created_at <= now - pending-after`(기본 2m)인 PENDING만 집는다. 그보다 젊은 PENDING은 요청 스레드가 아직 PG 응답을 기다리는 중일 수 있다. killbill이 `createdDateBefore` cutoff로 "충분히 오래된 것"만 보는 것과 같다 (아래 비교).
   - claim은 UNKNOWN과 같은 방식이다(SKIP LOCKED + lease + `attempt_count` +1). PENDING엔 `next_retry_at`이 없어서 `created_at`으로 고르고, 집으면 lease 시각이 `next_retry_at`에 들어간다.
   - PG 조회 결과가 `APPROVED`면 APPROVED(+`pg_tx_id`), `FAILED`면 FAILED.
   - `NOT_FOUND`면 UNKNOWN과 **같은 grace 규칙**을 쓴다. `created_at + not-found-grace`가 지났으면 FAILED(요청이 PG에 도달하기 전에 죽음, 돈 안 빠짐). 아직이면 UNKNOWN으로 넘기고 `next_retry_at = grace 끝`, 시도로 세지 않는다. 기본값(2m < 5m)에선 PG 미도달 결제가 2m에 UNKNOWN, 5m에 FAILED가 된다.
   - 판단 불가(PG 조회 오류)면 **UNKNOWN으로 넘긴다**(`next_retry_at = now + 백오프`, 1회로 셈). 이후 재시도와 MANUAL_REVIEW는 기존 UNKNOWN 흐름 하나가 맡는다. 왜 PENDING에 별도 재시도 일정을 두지 않는지는 아래 비교 참고.
   - 집힐 때마다 처리 도중 죽는 행(`attempt_count > max-attempts`)은 PG를 부르지 않고 UNKNOWN으로 넘긴다 → UNKNOWN 쪽 crash-loop 규칙이 MANUAL_REVIEW로 보낸다.
   - 요청 스레드와의 경쟁. 요청이 먼저 확정하면 janitor의 CAS가 0행 → 건너뛰고 배치를 계속한다. 문제는 반대 순서다. PG 응답이 `pending-after`보다 늦게 오면 janitor가 먼저 정한다. 그런데 janitor가 본 PG 기록은 **그 시각의** 것이다. 요청이 PG 앞에서 멈춰 있었다면 janitor는 `NOT_FOUND`를 보고 UNKNOWN(유예 안)이나 FAILED(유예 후)로 정한다. 그 뒤 요청이 깨어나 PG 승인을 받으면 **결론이 다르다.** 예전 코드는 이때 요청의 `PENDING → APPROVED` 0행(`StaleStateException`)을 삼키고 FAILED를 돌려줬다. 돈은 빠졌는데 고객은 "실패"를 보고 재결제 → 이중 결제.
   - 그래서 `PaymentService`는 Stale이면 **다시 읽고, 요청이 PG에서 받은 결과와 맞춘다** (`reconcileAfterStale`):

     | 요청이 아는 것 | 저장된 상태 | 처리 |
     |---|---|---|
     | 승인(`pgTxId`) | APPROVED | 그대로 (같은 결론, 추가 전이 없음) |
     | 승인(`pgTxId`) | UNKNOWN | `UNKNOWN → APPROVED` + `pg_tx_id` |
     | 승인(`pgTxId`) | FAILED | **망취소**: `FAILED → CANCEL_REQUESTED` 커밋 → PG 취소 → `CANCELED`. 취소 실패면 `CANCEL_REQUESTED`로 남아 janitor가 재시도. ERROR 로그 |
     | 거절 | UNKNOWN | `UNKNOWN → FAILED` |
     | 거절 | FAILED | 그대로 |
     | 모름(타임아웃) | 무엇이든 | 그대로 (janitor가 더 많이 안다) |
     | 그 외 (MANUAL_REVIEW 등) | | 그대로 + WARN 로그 (사람 몫) |

     맞추는 전이도 janitor(`reconcileUnknown`)와 또 부딪힐 수 있어 다시 읽기를 최대 3번 한다.
   - **왜 FAILED를 되살리지(→ APPROVED) 않고 취소하나.** FAILED는 이미 밖으로 나간 결론이다. 고객은 결제 실패를 보고 다른 수단으로 다시 결제했을 수 있다. 되살리면 그 결제와 합쳐 이중 결제가 된다. 취소는 최악이라도 "고객이 한 번 더 결제한다"로 끝난다. 그래서 전이표에 `FAILED → CANCEL_REQUESTED` 하나만 추가했다(늦은 승인 전용). `FAILED → APPROVED`는 여전히 금지다. 국내 PG 연동의 **망취소**(net-cancel)와 같은 생각이다: 승인은 났는데 가맹점이 그 결과를 반영하지 못했으면(타임아웃, DB 오류) 가맹점이 그 승인을 바로 취소한다.
5. **보상(Saga)**: 재고 확보 실패 → 먼저 `CANCEL_REQUESTED`를 커밋해 "취소해야 함"을 기록 → PG 취소 → `CANCELED`. 취소가 실패하면 `CANCEL_REQUESTED`로 남고 janitor가 재시도한다. 보상이 유실되지 않는다. 재시도 중 다른 경로가 먼저 확정한 건(compare-and-set 0행)은 건너뛰고 배치의 나머지를 계속 처리한다.

## 스케줄 실행 (JanitorScheduler)

앱을 띄우면 `JanitorScheduler`가 `sweepPending(now)` → `reconcileUnknown(now)` → `retryCancelRequested(now)`를 주기 실행한다. PENDING을 먼저 본다. 판단 불가로 UNKNOWN에 넘긴 건은 `next_retry_at`(백오프 뒤)이 되면 UNKNOWN 쪽이 다시 본다.

```bash
docker compose -f ../infra/docker-compose.yml up -d postgres
cd kotlin
./gradlew :07-payment-state-machine:bootRun --args='--lab.janitor.interval=2s'
# 다른 터미널: 재확인 시각이 지난 UNKNOWN 한 건 (1시간 전 생성 → 유예 지남)
# 이 단계의 테이블은 스키마 k07에 있다 (application.yml 의 spring.datasource.hikari.schema) → search_path 먼저
psql postgres://lab:lab@localhost:15432/lab -c "SET search_path=k07; INSERT INTO payments
  (id, order_id, amount, status, idempotency_key, next_retry_at, created_at)
  VALUES (gen_random_uuid(), 'o-1', 10000, 'UNKNOWN', 'idem-1',
          now() - interval '1 minute', now() - interval '1 hour')"
# 로그: JanitorScheduler : janitor run: pending=0 unknown=1 cancelRequested=0
# → o-1 은 FAILED (Fake PG 는 앱 메모리에만 있어서 밖에서 넣은 건 NOT_FOUND)
# 크래시로 남은 PENDING 한 건 (1시간 전 생성 → pending-after·유예 모두 지남)
psql postgres://lab:lab@localhost:15432/lab -c "SET search_path=k07; INSERT INTO payments
  (id, order_id, amount, status, idempotency_key, created_at)
  VALUES (gen_random_uuid(), 'p-1', 10000, 'PENDING', 'idem-p-1',
          now() - interval '1 hour')"
# 로그: janitor run: pending=1 unknown=0 cancelRequested=0 → p-1 은 FAILED
```

| 설정 | 기본값 | 의미 |
|---|---|---|
| `lab.janitor.enabled` | `true` | `false`면 `JanitorScheduler` 빈도 `@EnableScheduling`도 생기지 않는다 (`@ConditionalOnProperty`) |
| `lab.janitor.interval` | `10s` | 한 바퀴가 **끝난 뒤** 다음 바퀴까지 쉬는 시간 (`fixedDelay`). 첫 실행도 기동 후 이만큼 뒤 |
| `lab.janitor.pending-after` | `2m` | 이보다 오래된(`created_at` 기준) PENDING만 PG에 조회한다. PG 호출 타임아웃보다 넉넉히 길어야 한다. 0 이하면 기동 실패(`require`) |

- 테스트는 끈다: `PaymentStateMachineTest`는 `now`를 직접 넘겨 janitor를 부른다. 스케줄러가 끼어들면 결정적이지 않다.
- 실행 요약 로그: 집어 간 건이 있으면 INFO `janitor run: pending=P unknown=N cancelRequested=M`. 할 일 없는 바퀴는 DEBUG.
- 한쪽 작업이 예외를 던져도 다른 쪽은 실행하고, 다음 주기도 그대로 돈다 (`Exception`만 잡는다. `Error`는 잡지 않는다).
- 인스턴스가 여러 대여도 `claimDue`/`claimStalePending`(SKIP LOCKED + lease) 덕분에 같은 행을 겹쳐 처리하지 않는다.

**fixedDelay vs fixedRate.** killbill은 `scheduleAtFixedRate`를 쓴다 (이전 실행의 *시작* 기준). 여기선 `fixedDelay`(이전 실행의 *끝* 기준)를 골랐다.
1. 한 바퀴의 시간은 PG 조회 속도에 달렸다. PG가 느려지면 fixedRate는 밀린 실행을 쉬지 않고 이어 붙여, 장애 중인 PG를 더 두드린다.
2. 무엇을 처리할지는 `next_retry_at`(지수 백오프)이 정한다. 주기는 "얼마나 자주 확인하나"일 뿐이라 정확할 필요가 없다.
3. Go 버전(`Janitor.Run`, `time.After`)과 같은 의미가 된다.

어느 쪽이든 Spring 기본 스케줄러(스레드 1개)는 같은 메서드를 겹쳐 실행하지 않는다. killbill도 단일 스레드 executor다.

**killbill 대응**

| killbill (`payment/core/janitor`) | 이 모듈 |
|---|---|
| [`Janitor.start()` `:110`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/Janitor.java#L110) `janitorExecutor.scheduleAtFixedRate(incompletePaymentAttemptTask, period, period, unit)` | `JanitorScheduler.kt:49` `@Scheduled(initialDelayString, fixedDelayString)`. 첫 실행도 한 주기 뒤 (killbill의 `initialDelay = period`와 같음) |
| [`PaymentExecutors` `:102`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/PaymentExecutors.java#L102) `newSingleThreadScheduledExecutor("PaymentJanitor")` | Spring 기본 `TaskScheduler` (`spring.task.scheduling.pool.size` 기본 1, 스레드 `scheduling-1`) |
| `PaymentConfig` `org.killbill.payment.janitor.rate` (기본 `1h`) | `lab.janitor.interval` (기본 `10s`) |
| [`IncompletePaymentAttemptTask.run()` `:165`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L165), 항목별 `catch (Exception) → log.warn("Exception during Janitor loop")` `:184` | `JanitorScheduler.kt:53` `run()`, 작업별 `catch (Exception) → log.warn` `:78` |
| `Janitor.stop()` `:113`: `isStopped` + `shutdown()` + `awaitTermination(5s)` | 스프링 컨텍스트 종료 시 스케줄러 종료 (SIGTERM) |
| [`IncompletePaymentTransactionTask` `:63`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63) `TRANSACTION_STATUSES_TO_CONSIDER = PENDING, UNKNOWN` | `sweepPending`(PENDING) + `reconcileUnknown`(UNKNOWN) + `retryCancelRequested` |
| 재시도 시각: notification queue + [`getNextNotificationTime` `:402`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L402) (`unknown.retries` 기본 `5m,1h,1d,...`, `pending.retries` 기본 `1h,1d`) | `next_retry_at` 컬럼 + 지수 백오프 (`base-backoff` 10s × 2^n). PENDING은 첫 확인만 `pending-after`, 이후엔 UNKNOWN 일정 |

### PENDING 정리: killbill과 비교

killbill 코드는 모두 커밋 [`52e2f55`](https://github.com/killbill/killbill/tree/52e2f55f103baf0703ddee71e10a7036595fc851) 기준이다.

| killbill | 이 모듈 |
|---|---|
| [`IncompletePaymentTransactionTask.java:63`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63) `TRANSACTION_STATUSES_TO_CONSIDER = List.of(PENDING, UNKNOWN)`. 둘 다 janitor 대상 | `sweepPending`이 PENDING을, `reconcileUnknown`이 UNKNOWN을 본다 |
| [`IncompletePaymentAttemptTask.java:151`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L151) `processPaymentEvent`: PENDING/UNKNOWN 결제 이벤트가 나면 notification을 attempt 0으로 예약. 첫 확인 시각은 `pending.retries[0]` = 1h 뒤 | 별도 예약 없이 `created_at <= now - pending-after`(2m)로 고른다. 이벤트를 못 남기고 죽은 경우도 잡힌다 |
| [`IncompletePaymentAttemptTask.java:198`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L198) INIT attempt 조회에 `getCreatedDateBefore()`를 넘긴다. [`:420-422`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L420-L422) `clock.getUTCNow().minusMillis(attempts.delay)`, [`PaymentConfig.java:107-110`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/util/src/main/java/org/killbill/billing/util/config/definition/PaymentConfig.java#L107-L110) `attempts.delay` 기본 `12h` | `claimStalePending`의 `created_at <= ?` (`now - pending-after`). 같은 생각이다: "진행 중일 수 있는 건 건드리지 않는다. 충분히 오래된 것만 고친다" |
| [`IncompletePaymentAttemptTask.java:402-417`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L402-L417) `getNextNotificationTime`: UNKNOWN이면 `getUnknownTransactionsRetries`, PENDING이면 `getPendingTransactionsRetries` ([`PaymentConfig.java:61-64`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/util/src/main/java/org/killbill/billing/util/config/definition/PaymentConfig.java#L61-L64) `5m,1h,1d,1d,1d,1d,1d`, [`:71-74`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/util/src/main/java/org/killbill/billing/util/config/definition/PaymentConfig.java#L71-L74) `1h, 1d`). `:413-414` 횟수를 다 쓰면 `null` → 예약 중단 | 일정은 하나(지수 백오프)다. PENDING은 첫 확인에서 결론이 안 나면 UNKNOWN으로 넘어가 UNKNOWN 일정을 따른다. 횟수를 다 쓰면 MANUAL_REVIEW(보류)로 **눈에 보이게** 남긴다 |
| [`IncompletePaymentTransactionTask.java:224-228`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L224-L228) 플러그인이 UNKNOWN을 답하면 현재 상태를 유지. [`:182-187`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L182-L187) "Unable to repair" 후 그대로 반환, [`:190-194`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L190-L194) 상태가 그대로면 다음 notification만 예약 → PENDING은 PENDING 일정으로 재시도 | 판단 불가면 `PENDING → UNKNOWN` (`Janitor.toUnknown`) |
| [`IncompletePaymentTransactionTask.java:141-145`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L141-L145) 계정 락(`:124`)을 잡은 뒤 다시 읽어서, 그 사이 PENDING/UNKNOWN이 아니게 됐으면 아무것도 안 함 | CAS `WHERE status = 'PENDING'` 0행 → `StaleStateException` → 건너뜀. 요청 스레드 쪽도 같은 CAS로 janitor 결과를 덮어쓰지 않는다. 대신 다시 읽어 자기가 받은 PG 결과와 맞춘다 (UNKNOWN이면 확정, FAILED인데 승인이면 망취소) |

**왜 판단 불가 PENDING을 UNKNOWN으로 넘기나 (killbill처럼 PENDING 일정으로 재시도하지 않고).**
1. 의미가 다르다. killbill의 PENDING은 정상 상태일 수 있다: 플러그인이 "처리 중"이라고 답한 비동기 결제(계좌이체, 3DS 등)는 몇 시간~며칠 PENDING이다. 그래서 별도의 느린 일정(`1h, 1d`)이 맞다. 이 모듈의 PENDING은 "PG 호출 중"뿐이다. Fake PG는 처리 중 상태를 답하지 않는다. `pending-after`가 지났는데 PG가 결론을 안 주면 그건 "결과를 모름", 즉 UNKNOWN의 정의 그대로다.
2. 재시도 정책이 하나로 모인다. 백오프, `max-attempts`, MANUAL_REVIEW, crash-loop 보류, grace 규칙을 PENDING용으로 따로 만들지 않는다. PENDING용 전이도 추가하지 않는다 (`PENDING → MANUAL_REVIEW` 없음).
3. 운영에서 보인다. 대시보드/운영 쿼리의 "오래된 UNKNOWN"에 같이 잡힌다. PENDING에 머무르면 "진행 중"과 "버려짐"을 구별할 수 없다.
4. Go와 같은 규칙이다 (`SweepPending` → `pendingToUnknown`).

grace 안의 `NOT_FOUND`도 같은 이유로 UNKNOWN으로 넘긴다(시도로 세지 않음). 그러면 "오래된 PENDING은 결론이 나면 확정하고, 아니면 UNKNOWN"이라는 규칙 하나가 된다.

주의: killbill에서 fixedRate로 도는 건 `IncompletePaymentAttemptTask`(INIT 상태로 남은 *attempt*, `attempts.delay` 기본 12h 지난 것)다. UNKNOWN/PENDING *transaction*의 재확인은 `IncompletePaymentTransactionTask`가 notification queue의 예약 시각에 맞춰 처리한다. 이 모듈은 둘을 합쳐 "주기 실행 + 행마다 `next_retry_at`"으로 단순화했다.

## 핵심 코드 위치

- `src/main/kotlin/lab/statemachine/PaymentStatus.kt:20` — 허용 전이표
- `src/main/kotlin/lab/statemachine/PaymentStateMachine.kt:20` — 전이표 검사(`:30`) → CAS(`:33`) → history(`:38`)
- `src/main/kotlin/lab/statemachine/PaymentRepository.kt:41` — CAS UPDATE (`:50` `WHERE id = ? AND status = ?`)
- `src/main/kotlin/lab/statemachine/PaymentRepository.kt:72` — janitor claim (`FOR UPDATE SKIP LOCKED` + lease + attempt_count +1 `:75`)
- `src/main/kotlin/lab/statemachine/PaymentRepository.kt:98` — 오래된 PENDING claim (`created_at <= ?` `:106`)
- `src/main/kotlin/lab/statemachine/PaymentService.kt:22` — PENDING 선커밋(`:31`), PG 호출(`:34`), 타임아웃 → UNKNOWN(`:56`), janitor가 먼저 정했으면 맞추기(`reconcileAfterStale` `:84`), 늦은 승인 망취소(`netCancel` `:144`)
- `src/main/kotlin/lab/statemachine/Janitor.kt:54` — `sweepPending`: 충돌 건 건너뛰기(`:61`), crash loop → UNKNOWN(`:70`), 판단 불가 → UNKNOWN(`:79`), NOT_FOUND 유예(`:98`, 유예 안은 UNKNOWN·시도 안 씀 `:109`), `toUnknown`(`:117`)
- `src/main/kotlin/lab/statemachine/Janitor.kt:126` — `reconcileUnknown`: crash loop 보류(`:130`), NOT_FOUND 유예(`:147`, 유예 안은 시도 안 씀 `:153`), MANUAL_REVIEW(`:187`), 백오프(`:197`)
- `src/main/kotlin/lab/statemachine/Janitor.kt:167` — `retryCancelRequested` (충돌 건 건너뛰기 `:173`)
- `src/main/kotlin/lab/statemachine/JanitorScheduler.kt:34` — 스케줄러: `@EnableScheduling` + `@ConditionalOnProperty`(`:36`), `@Scheduled` fixedDelay(`:49`), 한 바퀴 `run()`(`:53`, PENDING 먼저 `:55`), 예외 → 로그(`:78`), 요약 로그(`:70`)
- `src/main/kotlin/lab/statemachine/CheckoutOrchestrator.kt:42` — 보상: CANCEL_REQUESTED 선커밋(`:45`) → PG 취소(`:50`) → 실패 시 재시도 예약(`:55`)

## 테스트 실행

```bash
cd kotlin
./gradlew :07-payment-state-machine:test
```

janitor는 `now`를 인자로 받는다 → 테스트에서 시간을 "넘겨" 백오프/유예를 실제로 기다리지 않고 검증한다. 그래서 `PaymentStateMachineTest`는 스케줄러를 끈다(`lab.janitor.enabled=false`). 스케줄링 자체는 `JanitorSchedulerTest`가 따로 켜서 검증한다 (`@DirtiesContext`로 끝나면 스케줄러를 닫는다).

| 테스트 | 증명하는 것 |
|---|---|
| `invalid_transition__rejected` | APPROVED → FAILED, APPROVED → CANCELED(건너뛰기) 거부 |
| `every_transition_recorded_in_history` | null→REQUESTED→PENDING→APPROVED 가 모두 history에 |
| `concurrent_transition__only_one_wins` | 10 스레드가 UNKNOWN을 APPROVED/FAILED로 동시 확정 → 1개만 성공, history도 1건 |
| `pg_timeout_after_charge__unknown__janitor_confirms_approved` | 타임아웃 → UNKNOWN → janitor가 PG 조회로 APPROVED (재결제 없음) |
| `pg_timeout_before_charge__unknown__janitor_marks_failed` | 유예 시간 안엔 UNKNOWN 유지(attempt_count 0, 유예 끝 시각으로 재예약), 지난 뒤 FAILED |
| `not_found_within_grace__does_not_consume_attempts__ends_failed` | 기본 설정에서 `next_retry_at`을 그대로 따라가며 janitor 실행 → MANUAL_REVIEW가 아니라 FAILED, attempt_count = 1 |
| `unknown_exceeds_max_attempts__moved_to_manual_review` | PG 조회 장애 지속 → 백오프 10/20/40/80초 → 5회째 MANUAL_REVIEW |
| `downstream_failure__compensated_by_pg_cancel` | 재고 실패 → PG 취소 → CANCELED |
| `cancel_failure__left_cancel_requested__janitor_retries_cancel` | 취소 실패 → CANCEL_REQUESTED 유지 → PG 복구 후 janitor가 CANCELED |
| `cancel_retry__stale_row_skipped__rest_of_batch_processed` | 취소 재시도 중 한 건을 다른 경로가 먼저 확정 → 그 건은 건너뛰고 나머지 건도 CANCELED |
| `janitor_crash_loop__claims_counted__moved_to_manual_review` | claim만 하고 죽는 상황 5회 → 6번째 claim에서 MANUAL_REVIEW (attempt_count 6) |
| `PendingSweepTest.stuck_pending__pg_approved__janitor_marks_approved` | 크래시로 남은 PENDING, PG엔 승인 기록 → APPROVED + `pg_tx_id`, `next_retry_at` 비움, 재결제 없음 |
| `PendingSweepTest.stuck_pending__no_pg_record__failed_after_grace` | PG 기록 없음, 3분 뒤(grace 안) → UNKNOWN(attempt 0, `next_retry_at = created_at + 5m`) → grace 끝에 FAILED |
| `PendingSweepTest.stuck_pending__no_pg_record_past_grace__failed_directly` | grace도 지났으면 `PENDING → FAILED` 바로 |
| `PendingSweepTest.pending_younger_than_threshold__not_touched` | `pending-after`보다 1초 젊은 PENDING은 PG에 승인 기록이 있어도 PG 조회도 안 하고 attempt/`next_retry_at`/이력 그대로. 대조군(3분 전)만 집힘 |
| `PendingSweepTest.stuck_pending__pg_status_unavailable__handed_to_unknown_flow` | PG 조회 장애 → UNKNOWN(attempt 1, 백오프 10s) → 복구 후 UNKNOWN 흐름이 APPROVED |
| `PendingSweepTest.pending_race__janitor_settles_first__request_gets_no_error` | `pay()`가 PG 응답을 받기 직전(`onCharge` 훅) janitor가 APPROVED로 확정 → `pay()`는 예외 없이 APPROVED 반환, 이력은 REQUESTED·PENDING·APPROVED 3건뿐(요청은 추가 전이 없음) |
| `PendingSweepTest.pending_race__request_settles_first__janitor_skips_rest_continue` | janitor가 PG에 묻는 사이(`onGetStatus` 훅) 요청이 먼저 확정 → janitor CAS 0행, 덮어쓰지 않음. 같은 배치의 다른 건은 FAILED로 계속 |
| `PendingSweepTest.late_pg_response__janitor_moved_to_unknown__request_approved__approved` | 요청이 PG 앞에서 멈춘 사이(`beforeCharge` 훅) janitor가 NOT_FOUND(유예 안) → UNKNOWN. 요청이 승인을 받으면 `UNKNOWN → APPROVED` + `pg_tx_id`, `next_retry_at` 비움 |
| `PendingSweepTest.late_pg_response__janitor_moved_to_unknown__request_declined__failed` | 같은 상황에서 PG 거절 → `UNKNOWN → FAILED` |
| `PendingSweepTest.late_pg_response__janitor_failed__request_approved__net_canceled` | janitor가 유예 후 NOT_FOUND → FAILED. 그 뒤 요청이 승인을 받으면 `FAILED → CANCEL_REQUESTED → CANCELED`, PG에서 실제 취소 (예전엔 FAILED 그대로 + 돈 빠짐) |
| `PendingSweepTest.late_pg_response__janitor_failed__net_cancel_fails__janitor_retries` | 망취소마저 실패 → `CANCEL_REQUESTED`(attempt 1, `next_retry_at` 있음)로 남고 `retryCancelRequested`가 CANCELED로 |
| `PendingSweepTest.late_pg_response__janitor_failed__request_timed_out__keeps_stored` | 요청도 타임아웃(결과 모름) → 저장된 FAILED 그대로, 추가 전이 없음 |
| `PendingSweepTest.pending_after_zero_or_negative__rejected_at_construction` | `pending-after` 0·음수면 `Janitor` 생성 시 `IllegalArgumentException` (진행 중인 요청까지 집는 설정 실수 방지) |
| `PendingSweepTest.pending_crash_loop__claims_counted__ends_manual_review` | PENDING claim만 하고 죽기 5회 → 6번째에 PG 없이 UNKNOWN → UNKNOWN crash-loop 규칙으로 MANUAL_REVIEW |
| `JanitorSchedulerTest.scheduler_reconciles_due_unknown_without_direct_call` | `lab.janitor.enabled=true`, interval 200ms. PG엔 승인 기록, DB엔 기한 지난 UNKNOWN → janitor를 직접 부르지 않아도 APPROVED (폴링, 상한 10초) |
| `JanitorSchedulerTest.scheduler_settles_stale_pending_without_direct_call` | 같은 설정. 3분 전 생성된 PENDING + PG 승인 기록 → 스케줄러가 APPROVED + `pg_tx_id` |
| `JanitorSchedulerRunTest.exception_in_one_task__does_not_escape_or_skip_other` | `reconcileUnknown`이 예외를 던져도 `run()`은 예외를 밖으로 던지지 않고 `sweepPending`·`retryCancelRequested`도 실행. 순서는 PENDING → UNKNOWN → 취소, 두 바퀴 연속 |
| `SchemaIsolationTest.app_datasource__uses_k07_schema` | `@ServiceConnection`이 URL을 컨테이너로 바꿔도 `hikari.schema`가 남아 테스트의 커넥션도 `current_schema() = k07` |
| `SchemaIsolationTest.boot_03_then_07__same_db__07_schema_applies_and_07_insert_works` | 한 DB에 03 → 07 순서로 각 모듈의 설정(application.yml의 `hikari.schema`)과 schema.sql을 적용해도 07의 schema.sql이 깨지지 않고 `attempt_count`가 있는 insert가 된다 (전엔 public에 03의 payments가 남아 `column "next_retry_at" does not exist`) |
| `SchemaIsolationTest.boot_all_stages_in_order__same_db__every_table_lives_in_its_stage_schema` | 01→10 전부 한 DB에 차례로 적용 → 각 단계의 테이블이 자기 스키마(`kNN`, 04는 `public`)에 있고 스키마가 단계마다 다르다 |

## 운영 쿼리

로컬 compose DB에서는 `psql postgres://lab:lab@localhost:15432/lab` 접속 후 `SET search_path TO k07;` 를 먼저 실행한다 (Kotlin 07의 테이블은 스키마 `k07`에 있다).

```sql
-- 상태별 건수
SELECT status, count(*) FROM payments GROUP BY status ORDER BY status;

-- 오래된 UNKNOWN (janitor가 멈췄거나 PG 조회가 계속 실패 중)
SELECT id, order_id, amount, attempt_count, next_retry_at, last_error, now() - created_at AS age
FROM payments
WHERE status = 'UNKNOWN' AND created_at < now() - interval '10 minutes'
ORDER BY created_at;

-- pending-after보다 훨씬 오래된 PENDING (janitor가 멈췄거나 스케줄러가 꺼져 있음)
SELECT id, order_id, amount, attempt_count, next_retry_at, now() - created_at AS age
FROM payments
WHERE status = 'PENDING' AND created_at < now() - interval '10 minutes'
ORDER BY created_at;

-- 사람이 확인해야 할 결제 (MANUAL_REVIEW) + 마지막 전이 사유
SELECT p.id, p.order_id, p.amount, p.attempt_count, p.last_error, h.reason, h.created_at
FROM payments p
JOIN LATERAL (SELECT reason, created_at FROM payment_status_history
              WHERE payment_id = p.id ORDER BY id DESC LIMIT 1) h ON true
WHERE p.status = 'MANUAL_REVIEW'
ORDER BY h.created_at;

-- 취소가 계속 실패 중인 결제 (보상 미완료 → 고객 돈이 묶여 있음, 알림 대상)
SELECT id, order_id, attempt_count, next_retry_at, last_error
FROM payments WHERE status = 'CANCEL_REQUESTED' AND attempt_count >= 3;

-- 한 결제의 이력
SELECT from_status, to_status, reason, created_at
FROM payment_status_history WHERE payment_id = :id ORDER BY id;
```

## 한계 / 다음 단계

- ~~서버가 PG 호출 도중 죽으면 결제가 `PENDING`에 남던 문제~~ → 고침: `sweepPending`이 `pending-after`보다 오래된 PENDING을 PG 조회로 정리한다 (위 "PENDING 정리: killbill과 비교").
- `pending-after`는 PG 호출 타임아웃보다 길어야 한다. 짧으면 janitor가 진행 중인 요청과 자주 경쟁한다. 그때마다 요청이 결과를 맞추는 추가 전이(UNKNOWN → APPROVED)나 망취소(FAILED → CANCEL_REQUESTED)가 생기고, 고객에게는 결제가 취소로 끝난다. 반대로 길면 크래시한 결제가 그만큼 늦게 정리된다. 같은 이유로 `not-found-grace`도 요청 하나가 살아 있을 수 있는 최대 시간(PG 호출 타임아웃 포함)보다 길어야 한다. 그래야 "아직 진행 중인 요청"을 FAILED로 확정하는 일이 드물다.
- 기준이 `created_at`(REQUESTED 생성 시각)이다. 이 모듈은 REQUESTED → PENDING이 곧바로 이어지므로 PENDING 진입 시각과 거의 같다. 둘 사이에 긴 단계가 생기면 PENDING 진입 시각 컬럼이 필요하다.
- PENDING이 정상적으로 오래 머무는 결제(비동기 계좌이체 등)가 생기면 "판단 불가 → UNKNOWN" 규칙이 맞지 않는다. 그때는 killbill처럼 PG가 "처리 중"이라고 답하면 PENDING을 유지하고 별도 일정으로 재시도해야 한다.
- `NOT_FOUND`로 FAILED 처리한 뒤 PG가 늦게 승인하는 경우, 요청 스레드가 아직 살아 있으면 망취소로 정리된다(위 표). 요청 스레드도 이미 죽었거나(PG 승인 직후 서버 다운) PG 내부 지연으로 늦게 승인되면 아무도 그 승인을 모른다 → FAILED + 돈 빠짐이 남는다. 이건 **09 대사**(MISSING_IN_INTERNAL)가 잡아낸다.
- 취소 재시도는 포기하지 않고 백오프로 계속한다. 실패가 길어지면 알림(attempt_count 기준)을 걸어 사람이 개입한다.
- 스케줄러는 인스턴스마다 돈다. claim 덕분에 겹쳐 처리하진 않지만, 대수가 늘면 빈 바퀴 조회도 늘어난다. 한 대만 돌리려면 ShedLock 같은 분산 락이 필요하다 (killbill은 `GlobalLocker`로 계정 단위 락).
- ~~Fake PG가 모르는 `pg_tx_id`를 취소하면 `NoSuchElementException`이 나서 배치가 멈추던 문제~~ → 고침: `PgException`으로 응답해 백오프로 재예약된다 (Go와 동일). 테스트 `cancel_retry__pg_unknown_tx__rescheduled_rest_of_batch_processed`.
- 상태가 맞아도 "돈이 어디서 어디로 갔는지"는 별도 기록이 필요하다 → **08 원장**.
