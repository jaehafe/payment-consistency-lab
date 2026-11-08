# 01. Dual Write 문제 재현

## 문제

결제를 DB에 저장하고, `PaymentCompleted` 이벤트를 Kafka 로 보내야 한다.
가장 쉬운 방법은 "둘 다 하면 된다" 이지만, **DB 와 Kafka 는 하나의 트랜잭션으로 묶이지 않는다.**
어떤 순서로 하든 둘 중 하나만 성공하는 순간이 존재한다.

| 구현 | 실패 지점 | 결과 |
|---|---|---|
| `CommitThenPublish` | DB 커밋 후 발행 실패 | 결제는 있는데 이벤트 없음 (**이벤트 유실**) — 배송/포인트가 영영 안 움직임 |
| `PublishThenCommit` | 발행 후 DB 커밋 실패 | 이벤트는 나갔는데 결제 없음 (**유령 이벤트**) — 결제 안 된 주문이 배송됨 |
| `PublishInsideTx` | tx 안에서 발행 후 롤백 | 롤백돼도 메시지는 이미 브로커에 있음 — "트랜잭션 안에서 보내면 안전하다"는 착각 |

세 번째는 Spring 에서 `@Transactional` 메서드 안에서 `kafkaTemplate.send()` 를 호출하는 흔한 패턴과 같은 구조다.
Kafka 는 DB 트랜잭션에 참여하지 않는다 (Kafka 트랜잭션은 Kafka 내부끼리의 원자성일 뿐, Postgres 와는 무관).

## 해결 (이 단계에서는 해결하지 않는다)

이 단계는 문제를 **눈으로 확인**하는 것이 목적이다. 해결책은:
- [03-outbox-polling](../03-outbox-polling): 이벤트를 같은 DB 의 outbox 테이블에 같은 트랜잭션으로 적고, 별도 릴레이가 발행
- [04-outbox-cdc](../04-outbox-cdc): outbox 를 Debezium 이 WAL 로 읽어서 발행

## 핵심 코드 위치

- `dual_write.go:32` `CommitThenPublish` — `:40` autocommit INSERT, `:45` 발행 (문제 지점)
- `dual_write.go:55` `PublishThenCommit` — `:61` 먼저 발행, `:66` 이후 DB 저장 실패 (문제 지점)
- `dual_write.go:76` `PublishInsideTx` — `:89` `defer tx.Rollback`, `:101` 발행 이후 실패 → 롤백
- `publisher.go:51` `ToggleablePublisher` — 실제 Redpanda 로 보내되, 스위치로 "브로커 장애"를 흉내 냄
- `dual_write_test.go` — 세 가지 실패를 실제 Postgres + Redpanda 로 증명

## 테스트 실행

Docker 가 떠 있어야 한다 (Testcontainers 가 Postgres, Redpanda 를 자동으로 띄운다).

```bash
cd go
go test ./01-dual-write-problem/ -v
# 하나만
go test ./01-dual-write-problem/ -v -run Test_publish_then_commit__db_failure__ghost_event_published
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_commit_then_publish__broker_failure__payment_saved_but_event_lost` | DB 에 결제 1건, 토픽에 메시지 0건 |
| `Test_publish_then_commit__db_failure__ghost_event_published` | DB 에 결제 0건, 토픽에 메시지 1건 (유령) |
| `Test_publish_inside_tx__rollback__event_already_sent` | 롤백됐는데 토픽에 메시지 1건 |

토픽 메시지 수는 high watermark(끝 offset)를 조회해서 정확히 센다 (`infra_test.go` 의 `consumeAll`). "0건"도 기다리지 않고 확정한다.

## 한계 / 다음 단계

- "재시도하면 되지 않나?" → 발행 재시도는 프로세스가 죽으면 같이 사라진다. 재시도 대상 자체를 **DB에 영속화**해야 한다 → outbox.
- 장애 주입은 스위치(`SetDown`, `FailBeforeCommit`)로 한다. 실제 운영에서는 네트워크 단절, 브로커 리더 선출, 커넥션 끊김, 배포 중 SIGKILL 등으로 일어난다.
- PG(외부 결제) 호출의 중복 문제는 [02-idempotency-key](../02-idempotency-key) 에서 다룬다.
