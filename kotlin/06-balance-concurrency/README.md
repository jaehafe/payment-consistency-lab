# 06. 잔액 동시성 제어

## 문제

잔액 1,000원 계좌에 1,000원 출금 요청 10건이 **동시에** 온다. 가장 자연스러워 보이는 코드:

```kotlin
tx {
  val balance = SELECT balance            // 10건 모두 1,000을 읽는다
  if (balance < amount) return INSUFFICIENT // 10건 모두 통과
  UPDATE accounts SET balance = balance(읽은 값) - amount   // 10건 모두 0을 쓴다
}
```

트랜잭션으로 감쌌는데도 **10건이 전부 성공**한다 (실제 테스트 결과: `성공 10 건, 잔액 0, 출금 합계 10000`).
READ COMMITTED의 SELECT는 잠금을 걸지 않으므로 "읽기 → 검사 → 쓰기" 사이에 다른 트랜잭션이 끼어든다. 이중 지불, lost update.

## 해결: 5가지 전략

| 전략 | 방법 | 정합성 | 처리량 (같은 계좌 경합 시) | 재시도 필요 | 언제 쓰나 |
|---|---|---|---|---|---|
| Naive | SELECT → 앱 검사 → 절대값 UPDATE | **깨짐** | - | - | 쓰지 않는다 |
| Pessimistic | `SELECT ... FOR UPDATE` | 보장 | 낮음 (줄 서기) | 없음 | 경합이 잦은 핫 계좌, 검사 로직이 복잡해 SQL 한 문장으로 안 될 때 |
| Optimistic | `UPDATE ... WHERE version = ?` + 재시도 | 보장 | 경합 적으면 높음 / 많으면 재시도 폭풍 | **필요** | 충돌이 드문 데이터(설정, 프로필), 사용자 편집 충돌 감지 |
| Serializable | `SERIALIZABLE` + 40001 재시도 | 보장 | 경합 시 abort 많음 | **필요** | 여러 행/테이블에 걸친 복잡한 불변식을 DB에 맡기고 싶을 때 |
| Conditional Update | `UPDATE ... SET balance = balance - ? WHERE balance >= ?` | 보장 | 높음 (잠금 구간 최소) | 없음 | **실무 기본값.** 조건이 한 행 안에서 표현될 때 |

이체(`transfer`)는 두 행을 잠가야 한다. A→B와 B→A가 동시에 오면 서로 상대가 잡은 행을 기다리는 **데드락**이 생긴다.
→ 이체 방향과 상관없이 **항상 id 순서로** 잠근다. 모두 같은 순서로 잠그면 순환 대기가 생길 수 없다.

## 핵심 코드 위치

- `src/main/kotlin/lab/balance/NaiveWithdraw.kt:23` — 읽기(`:24`) → 주입 지연(`:27`) → 절대값 쓰기(`:30`)
- `src/main/kotlin/lab/balance/PessimisticWithdraw.kt:22` — `FOR UPDATE`, `:28` 상대값 UPDATE (Go와의 차이)
- `src/main/kotlin/lab/balance/OptimisticWithdraw.kt:42` — version CAS, `:45` 0행이면 재시도, `:22` `maxAttempts`(첫 시도 포함 총 시도 횟수)
- `src/main/kotlin/lab/balance/SerializableWithdraw.kt:24` — 격리 수준, `:47` 40001만 재시도
- `src/main/kotlin/lab/balance/ConditionalUpdateWithdraw.kt:24` — 조건부 UPDATE 한 문장
- `src/main/kotlin/lab/balance/TransferService.kt:27` — id 정렬 잠금 / `:35` 정렬 안 한 버전(데드락)
- `src/test/kotlin/lab/balance/Concurrency.kt` — 시작 래치로 스레드를 한꺼번에 출발시키는 헬퍼

## 테스트 실행

```bash
cd kotlin
./gradlew :06-balance-concurrency:test
```

레이스를 **결정적으로** 재현하기 위해: 시작 래치로 10개 스레드를 동시에 출발시키고, 전략에 "읽기와 쓰기 사이 지연"(`readWriteDelayMs`)을 주입한다.
지연 동안 나머지 9건이 모두 읽기를 마치므로 매번 같은 결과가 나온다 — 그래서 아래 숫자(10건, 9회)는 테스트가 `==`로 단언한다. 커넥션 풀은 30개(`application.yml`) — 풀이 작으면 요청이 커넥션 대기로 직렬화돼 레이스가 가려진다.

| 테스트 | 증명하는 것 |
|---|---|
| `naive__concurrent_withdrawals__double_spending` | 10건 모두 성공, 출금 합계 10,000, 잔액 0 ≠ 1,000 - 10,000 |
| `pessimistic__concurrent_withdrawals__exactly_one_succeeds` | 지연을 넣어도 1건만 성공 |
| `optimistic__concurrent_withdrawals__exactly_one_succeeds_with_retries` | 1건 성공, 버전 충돌 재시도 정확히 9회 (9건이 1번씩) |
| `serializable__concurrent_withdrawals__exactly_one_succeeds_with_retries` | **나이브와 같은 코드**인데 격리 수준만으로 1건 성공, 40001 재시도 정확히 9회 |
| `conditional_update__concurrent_withdrawals__exactly_one_succeeds` | 지연 주입 지점 자체가 없다 (틈이 없음) |
| `transfer__opposite_directions_concurrently__no_deadlock` | A→B 10건 + B→A 10건 동시, 잠금 사이 지연이 있어도 데드락 없음, 총액 보존 |
| `transfer__unordered_locking__deadlock_detected` | 정렬 안 하면 Postgres가 1초(deadlock_timeout) 뒤 한쪽을 **SQLSTATE 40P01**로 죽임 |

### Go 구현과의 차이

- **재시도 한도**: `maxAttempts`는 첫 시도를 **포함한** 총 시도 횟수다 (20 = 첫 시도 + 재시도 최대 19). Go의 `MaxAttempts`도 같은 의미.
- **Pessimistic의 UPDATE**: 여기는 `balance = balance - ?`(상대값), Go는 나이브처럼 `balance = <읽은 값 - amount>`(절대값). `FOR UPDATE`가 있으면 둘 다 안전하다. `FOR UPDATE`를 지워 보면 차이가 보인다: Kotlin은 10건 성공·잔액 **-9,000**(초과 인출, 하지만 잔액 = 1,000 - 출금 합계는 성립), Go는 10건 성공·잔액 **0**(lost update, 합계도 깨짐).

## 한계 / 다음 단계

- `CHECK (balance >= 0)`은 나이브 구현의 초과 인출을 보이려고 일부러 뺐다. 실서비스에선 **반드시 넣는다** — 코드 버그가 있어도 DB가 마지막에 막아 준다.
- 핫 계좌(모든 요청이 같은 행)는 어떤 전략이든 한 행에 직렬화된다. 처리량이 문제면 잔액을 여러 행으로 쪼개거나(sub-account), 원장(08)에 append만 하고 잔액은 비동기 집계하는 구조를 고려한다.
- 데드락은 정렬로 "예방"하는 게 원칙이지만, 완전히 없앨 수 없는 경우도 있으니 40P01도 재시도 대상에 넣어 두는 게 안전하다.
- 잔액만 바꾸면 "왜 이 잔액인가"를 설명할 수 없다 → **08 복식부기 원장**.
