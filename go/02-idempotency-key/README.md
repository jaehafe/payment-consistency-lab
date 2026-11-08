# 02. 멱등키 (Idempotency Key)

## 문제

클라이언트가 `POST /payments` 를 보냈는데 응답 전에 네트워크가 끊겼다. 결제가 됐는지 모른다.
- 재시도하면 → **이중 결제** 위험
- 재시도 안 하면 → 결제 누락 위험
- 사용자가 결제 버튼을 연타하면 → 같은 요청이 **동시에** 여러 개 들어온다

서버 쪽에서도 같은 문제가 있다. 서버 → PG 호출이 타임아웃 나면, PG 에서 승인이 됐는지 서버도 모른다.

## 해결: Brandur 의 rocket-rides-atomic 방식

클라이언트가 요청마다 고유한 `Idempotency-Key` 헤더를 보낸다. 서버는 key 별로 처리 상태를 DB 에 기록한다.
처리를 **원자적 단계(atomic phase)** 로 나누고, 어디까지 끝났는지 `recovery_point` 에 남긴다.

```
Phase 1 (tx)  key upsert + 잠금(locked_at)         recovery_point = started
   │            ├ 같은 key 다른 바디 → 422
   │            ├ finished → 저장된 응답 그대로 반환 (PG 호출 없음)
   │            └ 잠금 유효 → 409
외부 호출      PG.charge(idempotencyKey)  ← 트랜잭션 밖!
Phase 2 (tx)  payments insert + pg_tx_id 저장       recovery_point = pg_charged
Phase 3 (tx)  응답 저장 + 잠금 해제                  recovery_point = finished
```

- **왜 PG 호출이 트랜잭션 밖인가?** PG 에서 일어난 승인은 DB 롤백으로 되돌릴 수 없다. tx 안에 넣어도 원자성이 생기지 않고, 느린 외부 호출 동안 DB 커넥션·행 잠금만 오래 쥐게 된다. 대신 **PG 자체의 멱등성**(같은 key 면 새로 승인하지 않음)에 기댄다.
- **실패하면?** 잠금을 풀고(`locked_at = NULL`) 503 을 준다. 클라이언트가 같은 key 로 재시도하면 `recovery_point` 부터 이어서 실행한다.
- **동시 요청은?** Phase 1 의 `INSERT ... ON CONFLICT DO NOTHING` + `SELECT ... FOR UPDATE` 로 같은 key 요청들이 한 줄로 서고, 먼저 잠근 하나만 진행하고 나머지는 409.
- **프로세스가 죽어 잠금이 남으면?** `locked_at` 이 `LockTimeout`(기본 30초)보다 오래되면 다음 요청이 잠금을 가져간다.

## 핵심 코드 위치

- `service.go:70` `Pay` — 전체 흐름. `:88-90` 실패 시 잠금 해제
- `service.go:130` `lockKey` (Phase 1) — `:143` `ON CONFLICT DO NOTHING`, `:156` `FOR UPDATE`, `:164` 422, `:167` 409
- `service.go:97` `resume` — `recovery_point` 부터 이어서 실행. `:103` **PG 호출이 트랜잭션 밖인 이유**
- `service.go:184` `savePayment` (Phase 2) — payments insert 와 `recovery_point` 전진을 한 tx 로
- `service.go:205` `finish` (Phase 3) — `:230` `RETURNING response_body` 로 재시도 응답과 바이트 단위까지 동일하게
- `gateway.go:40` Fake PG — `:56` PG 쪽 멱등성, `:65` `TimeoutAfterCharge`(승인됐는데 응답 유실)
- `handler.go:13` 라우팅 (`"POST /payments"`, Go 1.22+ 패턴), `:44~` 에러 → 상태 코드 매핑

## 테스트 실행

```bash
cd go
go test ./02-idempotency-key/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_same_key_twice__charged_once__same_response` | 같은 key 2번 → 둘 다 201, 바디 동일, PG 승인 1번 |
| `Test_same_key_different_body__422` | 같은 key 로 금액만 바꿔 보내면 422 |
| `Test_concurrent_same_key__only_one_charge` | goroutine 10개가 **동시에** 같은 key 로 요청 → PG 승인 1번, 나머지 409 |
| `Test_pg_timeout_after_charge__retry_resumes__no_double_charge` | PG 승인 후 응답 유실 → 503 → 재시도 시 PG 멱등성으로 새 승인 없이 201 |
| `Test_missing_header__400` | 헤더 없으면 400, PG 호출 없음 |

동시성 테스트는 Fake PG 에 300ms 지연을 주고, 채널을 `close` 해서 10개 goroutine 을 동시에 출발시킨다.

### 직접 띄워보기

```bash
docker compose -f ../infra/docker-compose.yml up -d postgres
cd go && go run ./02-idempotency-key/cmd     # :8081 (PORT 로 변경 가능), 스키마 go_02 사용
curl -i -X POST localhost:8081/payments -H 'Idempotency-Key: k1' -H 'Content-Type: application/json' \
  -d '{"orderId":"o-1","amount":10000}'
# 같은 명령을 다시 → 같은 응답. amount 만 바꾸면 → 422
```

## 한계 / 다음 단계

- 이 단계는 **"요청 → PG → DB"** 의 중복만 막는다. 결제 후 **이벤트 발행**은 여전히 dual write 문제가 있다 → [03-outbox-polling](../03-outbox-polling).
- `request_hash` 는 바디 원문의 SHA-256 이다. 같은 JSON 이라도 공백/키 순서가 다르면 다른 요청으로 본다 (Stripe 도 비슷하게 엄격).
- 같은 `orderId` 를 **다른 key** 로 보내면 `payments.order_id UNIQUE` 위반으로 500 이 된다. 실제로는 주문 단위 중복 검사를 별도로 둔다.
- `idempotency_keys` 는 계속 쌓이므로 보관 기간(예: 24시간) 후 지우는 배치가 필요하다.
- 진짜 PG 가 멱등키를 지원하지 않는다면? `pg_charged` 직전 상태를 "조회 API 로 확인"하는 복구 로직이 필요하다.
