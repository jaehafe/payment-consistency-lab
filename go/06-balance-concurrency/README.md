# 06. 잔액 동시성 제어

## 문제

잔액 1,000원 계좌에 1,000원 인출 요청 10건이 **동시에** 들어온다. 정답은 "1건 성공, 9건 잔액 부족, 잔액 0".

가장 자연스러운 코드는 이렇다:

```
balance = SELECT balance            -- 1,000
if balance < amount → 잔액 부족
UPDATE SET balance = balance(1,000) - amount
```

10개 요청이 모두 "1,000" 을 읽은 뒤 각자 "0" 으로 덮어쓰면 → **10건 모두 성공, 10,000원 인출, 잔액 0**. (이중 지불 / lost update)
READ COMMITTED 의 UPDATE 는 행을 잠그긴 하지만, 앞 트랜잭션 커밋을 기다렸다가 "내가 계산한 값"으로 다시 덮어쓸 뿐이다.

## 해결: 5가지 전략

| 전략 | 방법 | 정합성 | 처리량 | 재시도 | 언제 쓰나 |
|---|---|---|---|---|---|
| Naive | 읽고 → 앱에서 검사 → 덮어쓰기 | ❌ 이중 지불 | 높음 | - | 쓰지 않는다 (문제 재현용) |
| Pessimistic | `SELECT ... FOR UPDATE` | ✅ | 같은 행은 직렬 (대기) | 불필요 | 충돌이 잦고, 읽은 값으로 복잡한 검사를 해야 할 때 |
| Optimistic | `UPDATE ... WHERE version = ?`, 0행이면 재시도 | ✅ | 충돌 적을 때 높음, 많으면 재시도 폭주 | **필요** | 충돌이 드물고 잠금을 오래 쥐기 싫을 때 (JPA `@Version`) |
| Serializable | 격리 수준 `SERIALIZABLE`, `40001` 이면 재시도 | ✅ | 충돌 시 롤백 비용 | **필요** | 여러 행/테이블에 걸친 복잡한 불변식을 DB 에 맡기고 싶을 때 |
| Conditional UPDATE | `UPDATE SET balance = balance - ? WHERE balance >= ?` 한 문장 | ✅ | 가장 높음 (잠금은 한 문장 동안만) | 불필요 | **잔액 차감의 기본 답**. 검사가 SQL 한 줄로 표현될 때 |

이체(`transfer`)는 두 행을 잠가야 한다. A→B 는 A 먼저, B→A 는 B 먼저 잠그면 서로를 기다리는 **데드락**이 생긴다.
→ 방향과 무관하게 **항상 id 오름차순**으로 잠그면 순환 대기가 생길 수 없다.

## 핵심 코드 위치

- `withdraw.go:66` `NaiveWithdraw` — `:81` 읽기-쓰기 사이 지연 주입 (레이스를 결정적으로 재현)
- `withdraw.go:106` `PessimisticWithdraw` — `:116` `FOR UPDATE`, `:128` 절대값 UPDATE (`:124` Kotlin 과의 차이)
- `withdraw.go:169` `OptimisticWithdraw` — `:204` `WHERE version = $3`, 0행이면 재시도. `:144` `attemptsOrDefault` (`MaxAttempts` = 첫 시도 포함 총 시도 횟수)
- `withdraw.go:232` `SerializableWithdraw` — `:248` `pgx.Serializable`, `:238` SQLSTATE `40001` 이면 재시도
- `withdraw.go:285` `ConditionalUpdateWithdraw` — `:296` 조건부 UPDATE 한 문장
- `withdraw.go:41` `SQLState` — `errors.As` 로 `*pgconn.PgError` 꺼내기
- `transfer.go:27` `Transfer` — `:32` id 정렬 후 잠금 / `transfer.go:38` `TransferUnordered` — 데드락 재현용

## 테스트 실행

```bash
cd go
go test ./06-balance-concurrency/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_naive__concurrent_withdrawals__double_spending` | 10건 모두 성공, 인출 합계 10,000원, 잔액 0 → 돈의 보존 법칙 붕괴 |
| `Test_pessimistic__concurrent_withdrawals__exactly_one_succeeds` | 1건 성공 / 9건 부족 / 잔액 0 |
| `Test_optimistic__concurrent_withdrawals__exactly_one_succeeds_with_retries` | 위와 같고, 충돌한 9건이 1번씩 재시도 (재시도 정확히 9회) |
| `Test_serializable__concurrent_withdrawals__exactly_one_succeeds_with_retries` | 위와 같고, 9건이 `40001` 후 1번씩 재시도 (정확히 9회) |
| `Test_conditional_update__concurrent_withdrawals__exactly_one_succeeds` | 1건 성공, 재시도 없음 |
| `Test_transfer__opposite_directions_concurrently__no_deadlock` | A→B 10건 + B→A 10건 동시, 에러 0, 총액 보존 |
| `Test_transfer__unordered_locking__deadlock_detected` | 정렬 안 한 잠금 → Postgres 가 정확히 1건을 `40P01` 로 죽임, 나머지는 성공, 총액 보존 |

**결정적(non-flaky) 재현 방법**: 모든 goroutine 을 채널 `close` 로 동시에 출발시키고(start barrier),
읽기와 쓰기 사이에 200ms 지연을 주입한다. 그러면 10개가 모두 읽은 뒤에야 첫 쓰기가 일어난다 — 그래서 위 숫자(10건, 9회)는 테스트가 `==` 로 단언한다.
데드락 테스트는 첫 잠금과 두 번째 잠금 사이에 300ms 틈을 줘서 두 트랜잭션이 반드시 첫 행을 하나씩 쥐게 만든다.
테스트 풀은 `MaxConns = 40` (goroutine 수보다 커넥션이 적으면 풀에서 줄을 서느라 "동시"가 아니게 된다).

### Kotlin 구현과의 차이

- **재시도 한도**: `MaxAttempts` 는 첫 시도를 **포함한** 총 시도 횟수다 (5 = 첫 시도 + 재시도 최대 4). Kotlin `maxAttempts` 와 같은 의미. 0(zero value)이면 기본값 20.
- **Pessimistic 의 UPDATE**: 여기는 나이브처럼 `balance = $2`(읽은 값 - amount, 절대값), Kotlin 은 `balance = balance - ?`(상대값). `FOR UPDATE` 가 있으면 둘 다 안전하다 — 여기서 절대값을 쓴 건 "안전하게 만드는 건 `FOR UPDATE` 하나뿐"임을 보이려고. `FOR UPDATE` 를 지워 보면: Go 는 10건 성공·잔액 **0**(나이브와 똑같은 lost update), Kotlin 은 10건 성공·잔액 **-9,000**(초과 인출이지만 잔액 = 1,000 - 출금 합계는 성립).

## 한계 / 다음 단계

- 낙관적/직렬화 전략은 재시도 한도가 필요하다. 핫스팟 계좌(이벤트 상품 재고 등)에서는 재시도 폭주로 오히려 느려진다.
- `SERIALIZABLE` 은 `40001` 이 UPDATE 가 아니라 `COMMIT` 에서 날 수도 있다. 재시도 루프는 트랜잭션 **전체**를 감싸야 한다.
- 실무에서는 Conditional UPDATE + `CHECK (balance >= 0)` 제약을 함께 둔다 (이 스키마는 나이브 구현을 보이려고 일부러 뺐다).
- Postgres 의 데드락 감지는 `deadlock_timeout`(기본 1초) 후에 일어난다. 그동안 두 트랜잭션은 멈춰 있다. 감지에 기대지 말고 순서로 예방한다.
- 여러 서버/DB 에 걸친 잔액은 이 방법으로 안 된다 → 원장(08) + 보상(07).
