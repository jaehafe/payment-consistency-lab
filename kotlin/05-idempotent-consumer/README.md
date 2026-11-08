# 05. 멱등 컨슈머 (Idempotent Consumer)

## 문제

03/04의 outbox는 **at-least-once**다. 같은 이벤트가 두 번 올 수 있다. 컨슈머 쪽에서도:
- DB 처리 후 offset 커밋 전에 죽으면 → 재시작 후 같은 메시지를 다시 받는다.
- 리밸런스 중에 다른 컨슈머가 같은 파티션을 이어받으면 → 커밋 안 된 메시지를 다시 받는다.

즉 **중복 수신은 버그가 아니라 정상 동작**이다. 그런데 "포인트 1% 적립"처럼 실행할 때마다 쌓이는 부작용은 두 번 실행하면 두 번 쌓인다.

## 해결

메시지마다 고유한 `message_id`(헤더, = `outbox.id`)를 `processed_messages`에 기록하고, **부작용과 같은 DB 트랜잭션**에서 처리한다.

```
tx {
  INSERT INTO processed_messages (consumer_group, message_id) ... ON CONFLICT DO NOTHING
  → 0행이면: 이미 처리한 메시지 → skip
  → 1행이면: orders 갱신 + point_ledger insert
} commit
→ 그 다음에 Kafka offset commit
```

- **같은 트랜잭션**: 부작용이 실패하면 표시도 롤백 → 재전송 시 다시 처리(유실 없음). 표시가 커밋됐으면 부작용도 커밋됨(중복 없음).
- **DB 커밋 후 offset 커밋**: 반대로 하면 DB 처리 중 죽었을 때 메시지가 영영 다시 안 온다(유실). 이 순서면 최악이 "재전송"이고, 재전송은 위의 표시로 걸러진다.
- **enable.auto.commit=false**: 자동 커밋은 처리 완료와 무관하게 주기적으로 offset을 커밋한다.

결과적으로 **at-least-once 전달 + 멱등 처리 = effectively-once 처리**.

## 핵심 코드 위치

- `src/main/kotlin/lab/consumer/IdempotentConsumer.kt:14` — 트랜잭션 안에서 표시(`:19`) → 중복이면 skip(`:22`) → 부작용(`:28`)
- `src/main/kotlin/lab/consumer/NaiveConsumer.kt:15` — 비교용: 무조건 처리
- `src/main/kotlin/lab/consumer/MessageHandler.kt:11` — 부작용: 주문 갱신(자연적 멱등) vs 포인트 적립(멱등 아님)
- `src/main/kotlin/lab/consumer/ConsumerLoop.kt:46` — poll 루프: `message_id` 헤더 확인(`:53`) → 처리(`:56`, DB 커밋까지) → 크래시 주입(`:60`) → offset 커밋(`:64`)
- `src/main/kotlin/lab/consumer/ConsumerRunner.kt` — 앱 실행 시 백그라운드 루프. 실패 시 컨슈머를 재생성하는 이유
- `src/main/resources/application.yml` — `enable-auto-commit: false`

## 테스트 실행

```bash
cd kotlin
./gradlew :05-idempotent-consumer:test
```

테스트는 같은 `message_id`로 메시지를 직접 두 번 발행해 03의 at-least-once 중복을 흉내낸다.

| 테스트 | 증명하는 것 |
|---|---|
| `naive_consumer__duplicate_message__points_doubled` | 중복 메시지 → 포인트 2번 적립 (200점) |
| `idempotent_consumer__duplicate_message__processed_once` | 두 번 받았지만 처리 [true, false], 포인트 1번 |
| `idempotent_consumer__crash_before_offset_commit__redelivery_is_skipped` | DB 커밋 후 offset 커밋 전 크래시 → 재시작한 컨슈머가 **실제로 재전송받고** 건너뜀, 그 뒤 offset = 그 메시지 offset + 1 로 커밋 |
| `consumer_loop__missing_message_id_header__fails_with_clear_error` | `message_id` 헤더 없는 메시지 → NPE가 아니라 무엇이 빠졌는지(토픽-파티션@offset) 알려주는 `IllegalStateException`, 처리 안 함 |

## 한계 / 다음 단계

- `processed_messages`는 계속 쌓이므로 보관 기간을 정해 지워야 한다. 단, 브로커 재전송 가능 기간(retention, outbox 재발행 가능 기간)보다 길게 보관해야 한다.
- 부작용이 **우리 DB 밖**(이메일 발송, 외부 API 호출)이면 같은 트랜잭션으로 묶을 수 없다. 그 외부 API 자체에 멱등키(02)를 넘기거나, 그 작업을 또 outbox(03)로 보낸다.
- 처리 실패가 계속되는 메시지(poison message)는 재시도 횟수를 제한하고 DLQ(dead letter topic)로 보내야 한다.
  지금 `ConsumerRunner`는 실패하면 컨슈머를 새로 만들어 마지막 커밋 지점부터 다시 받으므로, poison 메시지가 있으면 그 파티션이 거기서 멈춰 무한 재시도한다.
- `message_id` 헤더가 빠진 메시지도 처리할 수 없다(중복 제거 키가 없음). 원인을 알 수 있게 명확한 예외를 던지지만, 동작은 위의 poison 메시지와 같다.
- 순서: 같은 주문의 이벤트는 key=orderId라 같은 파티션으로 오지만, 재전송이 섞이면 "오래된 이벤트가 나중에" 올 수 있다. 상태 전이(PENDING→PAID)를 검사하거나 버전 번호를 쓰는 방법이 있다.
