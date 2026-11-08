# 07. 결제 상태 머신 · 보류 · 보상 · 재처리

## 문제

- 결제 상태를 아무 코드에서나 `UPDATE status = ...` 하면, "승인된 결제가 실패로 바뀌는" 식의 불가능한 전이가 생긴다.
- PG 타임아웃을 **실패로 단정**하면, 실제로는 승인됐는데 주문은 실패 처리된다 → 고객 돈만 빠지고, 재결제하면 이중 결제.
- 결제는 승인됐는데 다음 단계(재고 확보)가 실패하면? PG 승인은 DB 롤백으로 되돌릴 수 없다.
- 확정이 안 되는 결제를 무한히 재시도할 수도, 그냥 버릴 수도 없다.

## 해결

```mermaid
stateDiagram-v2
    [*] --> REQUESTED
    REQUESTED --> PENDING: PG 승인 요청
    PENDING --> APPROVED: 승인 응답
    PENDING --> FAILED: 거절 응답
    PENDING --> UNKNOWN: 타임아웃/네트워크 오류
    UNKNOWN --> APPROVED: janitor - PG 조회 승인
    UNKNOWN --> FAILED: janitor - PG 조회 실패 / 유예 후에도 기록 없음
    UNKNOWN --> MANUAL_REVIEW: maxAttempts 초과 (보류)
    APPROVED --> CANCEL_REQUESTED: 후속 단계 실패 → 보상
    CANCEL_REQUESTED --> CANCELED: PG 취소 성공 (실패 시 janitor 재시도)
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
4. **보상(Saga)**: 재고 확보 실패 → 먼저 `CANCEL_REQUESTED`를 커밋해 "취소해야 함"을 기록 → PG 취소 → `CANCELED`. 취소가 실패하면 `CANCEL_REQUESTED`로 남고 janitor가 재시도한다. 보상이 유실되지 않는다. 재시도 중 다른 경로가 먼저 확정한 건(compare-and-set 0행)은 건너뛰고 배치의 나머지를 계속 처리한다.

## 스케줄 실행 (JanitorScheduler)

앱을 띄우면 `JanitorScheduler`가 `reconcileUnknown(now)` → `retryCancelRequested(now)`를 주기 실행한다.

```bash
docker compose -f ../infra/docker-compose.yml up -d postgres
cd kotlin
./gradlew :07-payment-state-machine:bootRun --args='--lab.janitor.interval=2s'
# 다른 터미널: 재확인 시각이 지난 UNKNOWN 한 건 (1시간 전 생성 → 유예 지남)
psql postgres://lab:lab@localhost:15432/lab -c "INSERT INTO payments
  (id, order_id, amount, status, idempotency_key, next_retry_at, created_at)
  VALUES (gen_random_uuid(), 'o-1', 10000, 'UNKNOWN', 'idem-1',
          now() - interval '1 minute', now() - interval '1 hour')"
# 로그: JanitorScheduler : janitor run: unknown=1 cancelRequested=0
# → o-1 은 FAILED (Fake PG 는 앱 메모리에만 있어서 밖에서 넣은 건 NOT_FOUND)
```

| 설정 | 기본값 | 의미 |
|---|---|---|
| `lab.janitor.enabled` | `true` | `false`면 `JanitorScheduler` 빈도 `@EnableScheduling`도 생기지 않는다 (`@ConditionalOnProperty`) |
| `lab.janitor.interval` | `10s` | 한 바퀴가 **끝난 뒤** 다음 바퀴까지 쉬는 시간 (`fixedDelay`). 첫 실행도 기동 후 이만큼 뒤 |

- 테스트는 끈다: `PaymentStateMachineTest`는 `now`를 직접 넘겨 janitor를 부른다. 스케줄러가 끼어들면 결정적이지 않다.
- 실행 요약 로그: 집어 간 건이 있으면 INFO `janitor run: unknown=N cancelRequested=M`. 할 일 없는 바퀴는 DEBUG.
- 한쪽 작업이 예외를 던져도 다른 쪽은 실행하고, 다음 주기도 그대로 돈다 (`Exception`만 잡는다. `Error`는 잡지 않는다).
- 인스턴스가 여러 대여도 `claimDue`(SKIP LOCKED + lease) 덕분에 같은 행을 겹쳐 처리하지 않는다.

**fixedDelay vs fixedRate.** killbill은 `scheduleAtFixedRate`를 쓴다 (이전 실행의 *시작* 기준). 여기선 `fixedDelay`(이전 실행의 *끝* 기준)를 골랐다.
1. 한 바퀴의 시간은 PG 조회 속도에 달렸다. PG가 느려지면 fixedRate는 밀린 실행을 쉬지 않고 이어 붙여, 장애 중인 PG를 더 두드린다.
2. 무엇을 처리할지는 `next_retry_at`(지수 백오프)이 정한다. 주기는 "얼마나 자주 확인하나"일 뿐이라 정확할 필요가 없다.
3. Go 버전(`Janitor.Run`, `time.After`)과 같은 의미가 된다.

어느 쪽이든 Spring 기본 스케줄러(스레드 1개)는 같은 메서드를 겹쳐 실행하지 않는다. killbill도 단일 스레드 executor다.

**killbill 대응**

| killbill (`payment/core/janitor`) | 이 모듈 |
|---|---|
| [`Janitor.start()` `:110`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/Janitor.java#L110) `janitorExecutor.scheduleAtFixedRate(incompletePaymentAttemptTask, period, period, unit)` | `JanitorScheduler.kt:47` `@Scheduled(initialDelayString, fixedDelayString)`. 첫 실행도 한 주기 뒤 (killbill의 `initialDelay = period`와 같음) |
| [`PaymentExecutors` `:102`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/PaymentExecutors.java#L102) `newSingleThreadScheduledExecutor("PaymentJanitor")` | Spring 기본 `TaskScheduler` (`spring.task.scheduling.pool.size` 기본 1, 스레드 `scheduling-1`) |
| `PaymentConfig` `org.killbill.payment.janitor.rate` (기본 `1h`) | `lab.janitor.interval` (기본 `10s`) |
| [`IncompletePaymentAttemptTask.run()` `:165`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L165), 항목별 `catch (Exception) → log.warn("Exception during Janitor loop")` `:184` | `JanitorScheduler.kt:51` `run()`, 작업별 `catch (Exception) → log.warn` `:71` |
| `Janitor.stop()` `:113`: `isStopped` + `shutdown()` + `awaitTermination(5s)` | 스프링 컨텍스트 종료 시 스케줄러 종료 (SIGTERM) |
| [`IncompletePaymentTransactionTask` `:63`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63) `TRANSACTION_STATUSES_TO_CONSIDER = PENDING, UNKNOWN` | `UNKNOWN`과 `CANCEL_REQUESTED`만. **PENDING은 아직 안 본다** (한계 참고) |
| 재시도 시각: notification queue + [`getNextNotificationTime` `:402`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentAttemptTask.java#L402) (`unknown.retries` 기본 `5m,1h,1d,...`, `pending.retries` 기본 `1h,1d`) | `next_retry_at` 컬럼 + 지수 백오프 (`base-backoff` 10s × 2^n) |

주의: killbill에서 fixedRate로 도는 건 `IncompletePaymentAttemptTask`(INIT 상태로 남은 *attempt*, `attempts.delay` 기본 12h 지난 것)다. UNKNOWN/PENDING *transaction*의 재확인은 `IncompletePaymentTransactionTask`가 notification queue의 예약 시각에 맞춰 처리한다. 이 모듈은 둘을 합쳐 "주기 실행 + 행마다 `next_retry_at`"으로 단순화했다.

## 핵심 코드 위치

- `src/main/kotlin/lab/statemachine/PaymentStatus.kt:19` — 허용 전이표
- `src/main/kotlin/lab/statemachine/PaymentStateMachine.kt:20` — 전이표 검사(`:29`) → CAS(`:32`) → history(`:35`)
- `src/main/kotlin/lab/statemachine/PaymentRepository.kt:41` — CAS UPDATE (`:49` `WHERE id = ? AND status = ?`)
- `src/main/kotlin/lab/statemachine/PaymentRepository.kt:70` — janitor claim (`FOR UPDATE SKIP LOCKED` + lease + attempt_count +1 `:73`)
- `src/main/kotlin/lab/statemachine/PaymentService.kt:15` — PENDING 선커밋(`:25`), PG 호출(`:29`), 타임아웃 → UNKNOWN(`:34`)
- `src/main/kotlin/lab/statemachine/Janitor.kt:31` — `reconcileUnknown`: crash loop 보류(`:35`), NOT_FOUND 유예(`:52`, 유예 안은 시도 안 씀 `:58`), MANUAL_REVIEW(`:92`), 백오프(`:102`)
- `src/main/kotlin/lab/statemachine/Janitor.kt:72` — `retryCancelRequested` (충돌 건 건너뛰기 `:78`)
- `src/main/kotlin/lab/statemachine/JanitorScheduler.kt:34` — 스케줄러: `@EnableScheduling` + `@ConditionalOnProperty`(`:36`), `@Scheduled` fixedDelay(`:47`), 한 바퀴 `run()`(`:51`), 예외 → 로그(`:71`), 요약 로그(`:64`)
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
| `JanitorSchedulerTest.scheduler_reconciles_due_unknown_without_direct_call` | `lab.janitor.enabled=true`, interval 200ms. PG엔 승인 기록, DB엔 기한 지난 UNKNOWN → janitor를 직접 부르지 않아도 APPROVED (폴링, 상한 10초) |
| `JanitorSchedulerRunTest.exception_in_one_task__does_not_escape_or_skip_other` | `reconcileUnknown`이 예외를 던져도 `run()`은 예외를 밖으로 던지지 않고 `retryCancelRequested`도 실행. 두 바퀴 연속 |

## 운영 쿼리

```sql
-- 상태별 건수
SELECT status, count(*) FROM payments GROUP BY status ORDER BY status;

-- 오래된 UNKNOWN (janitor가 멈췄거나 PG 조회가 계속 실패 중)
SELECT id, order_id, amount, attempt_count, next_retry_at, last_error, now() - created_at AS age
FROM payments
WHERE status = 'UNKNOWN' AND created_at < now() - interval '10 minutes'
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

- 서버가 PG 호출 도중 죽으면 결제가 `PENDING`에 남는다. **다음 단계**: 스케줄러가 "일정 시간 이상 PENDING"도 UNKNOWN처럼 PG 조회로 정리한다. killbill은 [`TRANSACTION_STATUSES_TO_CONSIDER = PENDING, UNKNOWN`](https://github.com/killbill/killbill/blob/52e2f55f103baf0703ddee71e10a7036595fc851/payment/src/main/java/org/killbill/billing/payment/core/janitor/IncompletePaymentTransactionTask.java#L63)으로 둘 다 보고, PENDING은 별도 재시도 간격(`org.killbill.payment.janitor.pending.retries`, 기본 `1h, 1d`)을 쓴다. 여기선 `PENDING`엔 `next_retry_at`이 없으므로 `updated_at < now - pending-grace`로 집는 조건이 필요하다.
- `NOT_FOUND`로 FAILED 처리한 뒤 PG가 늦게 승인할 수도 있다(PG 내부 지연). 결국 **09 대사**(MISSING_IN_INTERNAL)가 잡아낸다.
- 취소 재시도는 포기하지 않고 백오프로 계속한다. 실패가 길어지면 알림(attempt_count 기준)을 걸어 사람이 개입한다.
- 스케줄러는 인스턴스마다 돈다. claim 덕분에 겹쳐 처리하진 않지만, 대수가 늘면 빈 바퀴 조회도 늘어난다. 한 대만 돌리려면 ShedLock 같은 분산 락이 필요하다 (killbill은 `GlobalLocker`로 계정 단위 락).
- ~~Fake PG가 모르는 `pg_tx_id`를 취소하면 `NoSuchElementException`이 나서 배치가 멈추던 문제~~ → 고침: `PgException`으로 응답해 백오프로 재예약된다 (Go와 동일). 테스트 `cancel_retry__pg_unknown_tx__rescheduled_rest_of_batch_processed`.
- 상태가 맞아도 "돈이 어디서 어디로 갔는지"는 별도 기록이 필요하다 → **08 원장**.
