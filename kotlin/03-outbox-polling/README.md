# 03. Transactional Outbox + Polling Relay

## 문제

01에서 본 것처럼 "DB 커밋"과 "Kafka 발행"을 따로 하면 이벤트 유실이나 유령 이벤트가 생긴다.

## 해결

**Kafka에 직접 보내지 않는다.** 대신 "이 이벤트를 보내야 한다"는 사실을 같은 DB의 `outbox` 테이블에 **결제와 같은 트랜잭션으로** 쓴다.
그러면 둘 다 커밋되거나 둘 다 롤백된다 — Postgres 하나가 원자성을 보장하니까.

실제 발행은 별도의 **Polling Relay**가 한다:

```
tx {
  SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT n
  FOR UPDATE SKIP LOCKED          ← relay 여러 대가 서로 다른 행을 나눠 가짐
  → Kafka 동기 발행 (acks=all)     ← 브로커 ack를 받아야 다음으로
  → UPDATE outbox SET published_at = now()
} commit
```

- **브로커가 죽어 있어도** 결제 요청은 성공한다. 이벤트는 outbox에 남아 있다가 브로커가 살아나면 나간다 → 유실 없음.
- **FOR UPDATE SKIP LOCKED**: 앱 인스턴스가 여러 대라 relay도 여러 개 돌 때, 이미 다른 relay가 잠근 행은 기다리지 않고 건너뛴다. 중복 발행도, 대기도 없다.
- **at-least-once**: "발행"과 "published_at 커밋"은 여전히 두 시스템에 대한 쓰기다. 발행 후 커밋 전에 죽으면 다음 relay가 다시 보낸다 → **중복 가능**. 순서를 반대로(먼저 mark, 나중에 발행) 하면 유실 가능(at-most-once)이 된다. 결제 이벤트는 유실보다 중복이 낫다. 중복은 컨슈머가 `message_id`(= `outbox.id`)로 거른다 → **05**.

## 핵심 코드 위치

- `src/main/kotlin/lab/outbox/polling/PaymentService.kt:26` — 한 트랜잭션에서 payments insert + outbox insert(`:28`)
- `src/main/kotlin/lab/outbox/polling/OutboxRelay.kt:28` — `relayOnce`
  - `:30` 잠금 조회, `:35` 동기 발행, `:37` 크래시 주입 지점, `:40` published_at 기록
- `src/main/kotlin/lab/outbox/polling/OutboxRepository.kt:35` — `FOR UPDATE SKIP LOCKED` 쿼리와 그 이유
- `src/main/kotlin/lab/outbox/polling/OutboxPublisher.kt:33` — 동기 발행, `message_id`/`event_type` 헤더
- `src/main/kotlin/lab/outbox/polling/OutboxRelayScheduler.kt:22` — 주기 실행 (밀려 있으면 연속 실행)
- `src/main/resources/schema.sql` — 미발행 행만 담는 부분 인덱스

## 테스트 실행

```bash
cd kotlin
./gradlew :03-outbox-polling:test
```

테스트에서는 스케줄러를 끄고(`lab.outbox.relay.enabled=false`) `relayOnce()`를 직접 호출한다.

| 테스트 | 증명하는 것 |
|---|---|
| `pay__writes_payment_and_outbox_atomically` | 결제 + outbox 행이 함께 생기고, pay()는 Kafka를 건드리지 않음 |
| `pay__db_failure__no_payment_no_outbox` | 롤백되면 둘 다 없음 (01의 유령 이벤트 불가능) |
| `broker_down__payment_saved__event_delivered_after_recovery` | 브로커 장애 중엔 outbox에 남고, 복구 후 정확히 1건 전달 (01의 유실 해결) |
| `two_relays_concurrently__each_event_published_once` | relay 2개가 동시에 40건을 나눠 발행, 중복 0 (SKIP LOCKED) |
| `crash_after_publish_before_mark__event_published_twice` | 발행 후 커밋 전 크래시 → 같은 message_id로 2번 발행 (at-least-once) |

> 직접 확인: `OutboxRepository.kt`에서 `FOR UPDATE SKIP LOCKED` 줄을 지우고 `two_relays...` 테스트를 돌려 보면 `expected: <40> but was: <80>`으로 실패한다.

## 한계 / 다음 단계

- **지연**: 폴링 주기만큼 이벤트가 늦게 나간다. 주기를 줄이면 DB 부하가 는다.
- **DB 부하**: 이벤트가 없어도 계속 쿼리한다. 발행 완료된 행은 주기적으로 지워야 한다(테이블 비대화).
- **트랜잭션을 연 채 발행**: relay 트랜잭션이 Kafka 응답을 기다리는 동안 행 잠금을 쥐고 있다. 배치를 작게.
- **순서**: relay가 여러 대면 같은 주문의 이벤트가 서로 다른 relay에 나뉘어 순서가 뒤바뀔 수 있다. 엄격한 순서가 필요하면 aggregate_id별 파티셔닝이나 단일 relay가 필요하다.
- → **04**: 폴링 대신 WAL을 읽는 CDC(Debezium). → **05**: at-least-once 중복을 컨슈머에서 제거.
