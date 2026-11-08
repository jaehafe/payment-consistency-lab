# 02. 멱등키 (Idempotency Key)

## 문제

클라이언트가 `POST /payments`를 보냈는데 응답 직전에 네트워크가 끊겼다. 클라이언트는 결제가 됐는지 모른다.
- 재시도 안 하면 → 결제가 안 됐을 수도 있다
- 재시도 하면 → **두 번 결제**될 수도 있다

서버 쪽에도 같은 문제가 있다. 우리 서버가 PG에 승인을 요청했는데 **승인은 됐는데 응답이 유실**(타임아웃)되면, 우리는 성공인지 실패인지 모른다.
그리고 PG 승인은 외부 세계의 상태 변경이라 **DB 트랜잭션으로 롤백할 수 없다**.

## 해결

[Brandur의 rocket-rides-atomic](https://brandur.org/idempotency-keys) 방식:

1. 클라이언트가 요청마다 고유한 `Idempotency-Key` 헤더를 보내고, 재시도할 땐 **같은 key**를 쓴다.
2. 서버는 요청을 짧은 트랜잭션 여러 개(atomic phase)로 나누고, 어디까지 했는지 `recovery_point`에 기록한다.

```
Phase 1 (tx)   key insert/검증/잠금 (locked_at=now)     recovery_point = started
외부 호출       PG charge(idempotencyKey, ...)  ← 트랜잭션 밖
Phase 2 (tx)   payments insert + pg_tx_id 기록           recovery_point = pg_charged
Phase 3 (tx)   응답 저장 + locked_at=NULL                 recovery_point = finished
```

- 같은 key + `finished` → 저장된 응답을 그대로 돌려준다 (PG 호출 없음)
- 같은 key + 다른 바디 → **422** (클라이언트 버그. 절대 이전 응답을 돌려주면 안 됨)
- 같은 key + 다른 요청이 처리 중(잠금 유효) → **409**
- PG 에러/타임아웃 → 잠금을 풀고 **503**. 재시도하면 `started`부터 이어서 → PG를 다시 부르지만 **같은 key**라 PG가 기존 승인을 돌려준다 (PG 멱등성)
- 헤더 없음 → **400**

**왜 PG 호출을 트랜잭션 밖에서 하나?** 안에 넣어도 롤백으로 승인을 취소할 수 없으니 원자성은 생기지 않고, 느린 외부 호출 동안 DB 커넥션과 행 잠금만 붙잡게 된다. 대신 PG 쪽 멱등성에 기댄다.

## 핵심 코드 위치

- `src/main/kotlin/lab/idempotency/IdempotentPaymentService.kt:32` — `pay`: 전체 흐름
  - `:36` Phase 1 트랜잭션, `:77` `phase1LockKey` (422 `:84`, 409 `:89`, 잠금 이어받기 `:92`)
  - `:39` finished면 저장된 응답 재생
  - `:49` PG 호출 (트랜잭션 밖)
  - `:53` Phase 2, `:60` Phase 3
  - `:65` PG 실패 시 잠금 해제 → 503
- `src/main/kotlin/lab/idempotency/IdempotencyKeyRepository.kt:32` — `INSERT ... ON CONFLICT DO NOTHING` (동시 insert 직렬화)
- `src/main/kotlin/lab/idempotency/IdempotencyKeyRepository.kt:43` — `SELECT ... FOR UPDATE` + 잠금 만료 판정
- `src/main/kotlin/lab/idempotency/PaymentGateway.kt:36` — Fake PG: 자체 멱등성, `timeoutAfterCharge`, `chargeCount()`/`callCount()`
- `src/main/kotlin/lab/idempotency/PaymentController.kt:19` — 헤더 검증(400), 예외 → 상태코드

## 테스트 실행

```bash
cd kotlin
./gradlew :02-idempotency-key:test
```

실제 HTTP 서버(랜덤 포트) + Postgres 컨테이너로 테스트한다.

| 테스트 | 증명하는 것 |
|---|---|
| `same_key_twice__charged_once__same_response` | 두 번째 요청은 PG를 호출조차 안 하고 같은 응답 |
| `same_key_different_body__422` | key 재사용 + 다른 금액 → 422 |
| `concurrent_same_key__only_one_charge` | 스레드 10개가 동시에 같은 key → PG **호출** 1번(우리 Phase 1 잠금이 막음) · 승인 1번, payments 1건. 응답은 201(≥1, 모두 같은 바디) 또는 409(≥1)뿐, 5xx 없음 |
| `pg_timeout_after_charge__retry_resumes__no_double_charge` | 승인 후 응답 유실 → 503, key는 `started` + 잠금 해제 → 재시도 시 PG 호출은 2번이지만 실제 승인은 1번(PG 멱등성이 막음) |
| `missing_header__400` | 헤더 없으면 400, PG 호출 0번 |

201/409의 **개수**는 보장 대상이 아니다. 처리 중에 온 요청은 409, 처리가 끝난 뒤 도착한 요청은 저장된 201을 재생받으므로 타이밍에 따라 201이 여러 개일 수 있다 (보통은 201×1, 409×9). 보장되는 것은 **PG 승인 1회**다.
`callCount()`와 `chargeCount()`를 같이 보면 "누가 막았나"가 구분된다: call=1·charge=1은 **우리 서버**가, call=2·charge=1은 **PG 멱등성**이 막은 것.

## 한계 / 다음 단계

- 이 방식은 **우리 DB 하나 + 멱등한 외부 API** 조합일 때 성립한다. PG가 멱등키를 지원하지 않으면, 타임아웃 시 PG에 "이 주문 승인됐나?" 조회 API로 확인하는 절차(reconciliation)가 필요하다.
- 잠금 만료(`lock-timeout`)보다 오래 걸리는 처리는 두 요청이 동시에 진행될 수 있다. 타임아웃은 최대 처리 시간보다 넉넉히.
- 실서비스에선 key를 사용자별로 범위를 나누고(user_id + key), 오래된 key를 주기적으로 지운다(보통 24시간).
- 결제는 저장됐지만 **이벤트 발행**은 여전히 01의 이중 쓰기 문제를 갖는다 → **03 Transactional Outbox**.
