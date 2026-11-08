# 01. 이중 쓰기(Dual Write) 문제 재현

## 문제

결제가 끝나면 두 가지를 해야 한다.

1. DB(`payments`)에 결제를 저장
2. Kafka(`payment.events`)에 `PaymentCompleted` 이벤트 발행 → 배송/포인트/알림 서비스가 받아서 처리

DB와 Kafka는 **서로 다른 시스템**이고, 둘을 하나로 묶는 트랜잭션이 없다.
그래서 어떤 순서로 하든 "한쪽만 성공"하는 틈이 생긴다.

| 구현 | 실패 지점 | 결과 |
|---|---|---|
| `commitThenPublish` | DB 커밋 후 발행 실패 (브로커 장애, 프로세스 사망) | 결제는 있는데 **이벤트 유실** → 배송이 안 나감 |
| `publishThenCommit` | 발행 후 DB 커밋 실패 | 이벤트는 나갔는데 결제가 없음 → **유령 이벤트** (없는 결제로 배송) |
| `publishInsideTx` | 트랜잭션 안에서 발행 후 롤백 | Kafka는 DB 트랜잭션에 참여하지 않으므로 **이벤트는 이미 나감** |

세 번째가 특히 함정이다. `@Transactional` 메서드 안에서 `kafkaTemplate.send()`를 부르면 원자적일 것 같지만, 실제로는 `publishThenCommit`과 똑같다.

## 해결

이 단계는 **문제 재현**만 한다. 해결은 다음 단계에서:
- 이벤트 유실/유령 이벤트 → **03 Transactional Outbox** (이벤트를 DB 트랜잭션 안에 같이 쓴다)
- 그 결과 생기는 중복 → **05 멱등 컨슈머**

## 핵심 코드 위치

- `src/main/kotlin/lab/dualwrite/DualWritePaymentService.kt:23` — `commitThenPublish`: `:29`에서 커밋된 뒤 발행
- `src/main/kotlin/lab/dualwrite/DualWritePaymentService.kt:38` — `publishThenCommit`: 발행 후 `:46`에서 DB 실패 → 롤백
- `src/main/kotlin/lab/dualwrite/DualWritePaymentService.kt:56` — `publishInsideTx`: 트랜잭션 블록 안에서 발행 후 `:62`에서 롤백
- `src/main/kotlin/lab/dualwrite/PaymentEventPublisher.kt:32` — 동기 발행(`send().get()`): 리턴하면 이미 브로커에 기록됨
- `src/main/kotlin/lab/dualwrite/DbFailureInjector.kt:16` / `PaymentEventPublisher.kt` `brokerDown` — 장애 주입 토글
- `src/test/kotlin/lab/dualwrite/DualWriteProblemTest.kt` — 세 실패 모드를 실제 Postgres + Redpanda로 증명
- `src/test/kotlin/lab/dualwrite/EventProbe.kt` — 테스트에서 토픽을 끝까지 읽어 "이벤트가 실제로 있는지/없는지" 확인

## 테스트 실행

```bash
cd kotlin
./gradlew :01-dual-write-problem:test
```

Docker만 떠 있으면 된다 (Testcontainers가 Postgres 16, Redpanda를 자동으로 띄움).

| 테스트 | 증명하는 것 |
|---|---|
| `commit_then_publish__broker_failure__payment_saved_but_event_lost` | DB에 결제 1건, 브로커에 이벤트 0건 |
| `publish_then_commit__db_failure__ghost_event_published` | DB에 결제 0건, 브로커에 이벤트 1건 |
| `publish_inside_tx__rollback__event_already_sent` | 롤백됐는데도 브로커에 이벤트 1건 |

## 한계 / 다음 단계

- "발행 재시도"를 붙여도 해결되지 않는다. 재시도 루프 도중 프로세스가 죽으면 똑같이 유실된다.
- Kafka 트랜잭션(`transactional.id`)도 답이 아니다. Kafka 트랜잭션은 Kafka 안의 쓰기끼리만 묶어 주고, Postgres 커밋과는 묶이지 않는다.
- XA/2PC(분산 트랜잭션)는 이론상 가능하지만 Kafka가 지원하지 않고, 지원하더라도 느리고 운영이 어렵다.
- → **02**에서 먼저 "클라이언트 재시도로 인한 이중 결제"를 막고, **03**에서 이중 쓰기 자체를 없앤다.
