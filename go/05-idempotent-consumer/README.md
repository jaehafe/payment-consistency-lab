# 05. 멱등 컨슈머 (Idempotent Consumer)

## 문제

03/04 의 outbox 는 **at-least-once** 다. 같은 이벤트가 두 번 올 수 있다. 컨슈머 쪽에서도:
- DB 처리 후 offset 커밋 전에 죽으면 → 재시작 후 같은 메시지를 다시 받는다.
- 반대로 offset 을 먼저 커밋하고 DB 처리 중 죽으면 → 메시지가 **유실**된다 (at-most-once). 결제 시스템에서는 더 나쁘다.

`orders.status = 'PAID'` 같은 덮어쓰기는 여러 번 해도 결과가 같지만(자연적 멱등),
**포인트 적립(`point_ledger` INSERT)** 처럼 할 때마다 쌓이는 부작용은 중복 처리되면 그대로 두 배가 된다.

## 해결

1. **offset 은 DB 커밋 "후에" 커밋한다** → 유실 대신 중복을 택한다 (at-least-once).
2. 중복은 **`processed_messages` 테이블로 걸러낸다**: 비즈니스 처리와 **같은 트랜잭션**에서
   `INSERT INTO processed_messages (consumer_group, message_id) ... ON CONFLICT DO NOTHING` →
   영향 행이 0 이면 이미 처리한 메시지이므로 건너뛴다.

```
IdempotentConsumer.Handle   tx { INSERT processed_messages ON CONFLICT DO NOTHING
                                 └ 0행 → skip
                                 UPDATE orders; INSERT point_ledger }
ConsumerLoop.PollOnce       poll → Handle(각 레코드) → (DB 커밋 끝난 뒤) CommitRecords
```

결과적으로 "전달은 at-least-once, 효과는 exactly-once" 가 된다.

## 핵심 코드 위치

- `consumer.go:29` `NaiveConsumer.Handle` — 받을 때마다 처리 (중복 시 포인트 2배)
- `consumer.go:58` `IdempotentConsumer.Handle` — `:79` `ON CONFLICT DO NOTHING`, `:83` `RowsAffected() == 0` 이면 skip
- `consumer.go:101` `applyPayment` — 자연적으로 멱등한 처리(orders) vs 아닌 처리(point_ledger)
- `consumer.go:132` `ConsumerLoop.PollOnce` — `:156` **DB 커밋 → offset 커밋 순서**, `:163` `CommitRecords`
- `infra_test.go` `newGroupClient` — `kgo.DisableAutoCommit()` (자동 커밋을 끄고 직접 커밋)

## 테스트 실행

```bash
cd go
go test ./05-idempotent-consumer/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_naive_consumer__duplicate_message__points_doubled` | 같은 `message_id` 메시지 2건 → 100점이어야 할 포인트가 200점 |
| `Test_idempotent_consumer__duplicate_message__processed_once` | 같은 메시지 2건 → 1건 처리 + 1건 스킵, 포인트 100점 |
| `Test_idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped` | 컨슈머 A 가 DB 커밋 후 offset 커밋 전에 종료 → 같은 그룹의 B 가 같은 메시지를 **실제로 다시 받고** 스킵, 이후 offset=1 커밋 |

중복 메시지는 03 에서 본 "릴레이가 발행 후 마킹 전에 죽음" 상황처럼 같은 `message_id` 로 두 번 produce 해서 만든다.

## 한계 / 다음 단계

- `processed_messages` 는 계속 쌓인다. 브로커 보관 기간(retention)보다 오래된 행은 지워도 된다.
- 이 방식은 **처리 결과가 같은 DB 에 있을 때**만 같은 트랜잭션으로 묶을 수 있다. 처리 중 외부 API 를 호출한다면
  그 API 에도 멱등키(02 단계)를 넘겨야 한다.
- `message_id` 는 프로듀서가 **재발행해도 같은 값**이어야 한다 (outbox.id 가 그 역할). 재발행 때마다 새 UUID 를 만들면 소용없다.
- Kafka 트랜잭션(EOS)은 "Kafka → Kafka" 처리에서만 exactly-once 를 준다. 외부 DB 에 쓰는 컨슈머는 결국 이 패턴이 필요하다.
