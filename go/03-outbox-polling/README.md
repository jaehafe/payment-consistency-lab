# 03. Transactional Outbox + Polling Relay

## 문제

[01](../01-dual-write-problem) 에서 봤듯이 DB 저장과 Kafka 발행은 원자적으로 묶을 수 없다.

## 해결

Kafka 로 바로 보내지 말고, **"보낼 이벤트"를 같은 DB 의 `outbox` 테이블에 같은 트랜잭션으로 적는다.**
payments 와 outbox 는 같은 Postgres 이므로 로컬 트랜잭션 하나로 원자성이 보장된다.
그 다음 별도의 **릴레이**가 outbox 를 주기적으로 읽어서 Kafka 로 보낸다.

```
PaymentService.Pay          tx { INSERT payments; INSERT outbox }  ← 둘 다 되거나 둘 다 안 되거나
OutboxRelay.RelayOnce       tx { SELECT ... FOR UPDATE SKIP LOCKED
                                 → Kafka 동기 발행 (acks=all)
                                 → UPDATE published_at = now() }
```

- 브로커가 죽어 있어도 결제는 성공한다. 이벤트는 outbox 에 안전하게 남아 있다가 복구 후 나간다.
- `FOR UPDATE SKIP LOCKED`: 릴레이를 여러 개 띄워도 서로 다른 행을 집는다 (기다리지도, 중복으로 집지도 않음).
- **at-least-once**: "발행 성공 → published_at 갱신 커밋" 사이에서 죽으면 재시작 후 다시 보낸다. **중복은 피할 수 없다.**
  그래서 outbox.id 를 Kafka 헤더 `message_id` 로 실어 보내고, 컨슈머가 이걸로 중복을 걸러낸다 → [05](../05-idempotent-consumer).

## 핵심 코드 위치

- `payment_service.go:26` `Pay` — `:41~68` **트랜잭션 경계**, `:62` 실패 주입(롤백)
- `outbox_relay.go:32` `RelayOnce`
  - `:39-49` `FOR UPDATE SKIP LOCKED` 와 그 이유
  - `:74` 동기 발행. 실패 시 return → tx 롤백 → 다음 주기 재시도
  - `:80-82` **at-least-once 의 근원** (발행 후 마킹 전 크래시 지점)
  - `:86` `published_at` 갱신
- `outbox_relay.go:96` `Run` — 주기 실행 루프 (`time.Ticker`)
- `schema.sql` — `published_at IS NULL` 부분 인덱스

## 테스트 실행

```bash
cd go
go test ./03-outbox-polling/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_pay__writes_payment_and_outbox_atomically` | 결제 1건 → payments 1행 + outbox 1행(미발행) |
| `Test_pay__db_failure__no_payment_no_outbox` | 커밋 실패 → 둘 다 0행 (01 과 달리 불일치 없음) |
| `Test_broker_down__payment_saved__event_delivered_after_recovery` | 브로커 장애 중 결제 성공, 릴레이 실패, 복구 후 1건 전달 |
| `Test_two_relays_concurrently__each_event_published_once` | goroutine 2개로 릴레이 동시 실행 → 30건이 정확히 한 번씩, 두 릴레이가 나눠서 처리 |
| `Test_crash_after_publish_before_mark__event_published_twice` | 발행 후 마킹 전 크래시 → 같은 `message_id` 메시지가 토픽에 2건 |

### 직접 띄워보기

```bash
docker compose -f ../infra/docker-compose.yml up -d postgres redpanda
cd go && go run ./03-outbox-polling/cmd   # 스키마 go_03, 토픽 payment.events.go03 사용 (04 의 Debezium 토픽과 분리)
```

## 한계 / 다음 단계

- **지연**: 폴링 주기만큼 늦게 나간다. 주기를 줄이면 DB 부하가 는다.
- **DB 부하**: 이벤트가 없어도 계속 SELECT 한다. 발행된 행이 쌓이면 테이블/인덱스가 커지므로 정리 배치가 필요하다.
- **순서**: 릴레이 여러 개 + SKIP LOCKED 는 서로 다른 행을 병렬로 보내므로 같은 주문의 이벤트 순서가 뒤바뀔 수 있다. 순서가 중요하면 릴레이를 하나로 두거나 aggregate_id 로 파티셔닝한다.
- **배치 내 일부 실패**: 배치 중간에 발행이 실패하면 앞에서 이미 보낸 것도 롤백되어 다시 나간다 (역시 at-least-once).
- 폴링 대신 WAL 을 읽는 방식 → [04-outbox-cdc](../04-outbox-cdc). 중복 처리 → [05-idempotent-consumer](../05-idempotent-consumer).
