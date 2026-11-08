# 04. Outbox + CDC (Debezium Outbox Event Router)

## 문제

[03](../03-outbox-polling) 의 폴링 릴레이는 동작하지만, 주기만큼 지연되고 이벤트가 없어도 DB 를 계속 조회하며,
릴레이 코드(잠금, 재시도, 마킹)를 직접 운영해야 한다.

## 해결

애플리케이션은 **outbox INSERT 까지만** 한다. 릴레이가 없다.
Postgres 의 WAL(변경 로그)을 Debezium(Kafka Connect)이 logical replication 으로 읽어서,
outbox INSERT 를 감지하면 Kafka 로 보낸다. 테이블 컬럼은 Debezium Outbox Event Router 기본 이름을 따른다.

| outbox 컬럼 | Kafka 로 가는 곳 |
|---|---|
| `aggregatetype = 'payment'` | 토픽 `payment.events` (`route.topic.replacement = ${routedByValue}.events`) |
| `aggregateid` | 메시지 key (orderId → 주문 단위 순서 보장) |
| `id` | 헤더 `message_id` |
| `type` | 헤더 `event_type` |
| `payload` | 메시지 value (JSON 그대로, `expand.json.payload`) |

> 실제 메시지에는 헤더 `id` 도 하나 더 붙는다 (Event Router 가 기본으로 넣는 이벤트 id, `message_id` 와 같은 값). 컨슈머는 `message_id` 를 쓰면 된다.

WAL 에는 **커밋된** 변경만 나타나므로 롤백된 결제의 이벤트는 절대 나가지 않는다.

## 핵심 코드 위치

- `payment_service.go:20` `Pay` — 03 과 같은 한 트랜잭션 (payments + outbox). `:44` 컬럼 → Kafka 매핑 설명
- `schema.sql` — Debezium 기본 컬럼명, `published_at` 없음 (발행 위치는 Debezium 이 WAL offset 으로 관리)
- `cmd/main.go` — E2E 데모: 결제 저장 → Debezium 이 보낸 메시지를 `payment.events` 에서 읽어 출력
- `../../infra/debezium/outbox-connector.json` — 커넥터 설정 (Kotlin/Go 공통)

## 테스트 실행

자동 테스트는 "outbox 행이 Debezium 이 기대하는 형태로 들어가는가" 까지만 본다.

```bash
cd go
go test ./04-outbox-cdc/ -v     # Test_pay__writes_outbox_row
```

### E2E (수동)

```bash
# 1) Postgres(wal_level=logical) + Redpanda + Kafka Connect(Debezium)
docker compose -f infra/docker-compose.yml --profile cdc up -d     # 저장소 루트에서

# 2) Connect 가 뜰 때까지 잠시 기다린 뒤 커넥터 등록
curl -s localhost:8083/connectors          # [] 가 나오면 준비 완료
./infra/debezium/register.sh

# 3) 결제 1건 저장 → Debezium 이 보낸 메시지 출력
cd go && go run ./04-outbox-cdc/cmd
#   결제 저장 (+outbox): paymentId=... orderId=order-... — 애플리케이션은 Kafka 를 호출하지 않았다
#   Debezium 이 보낸 메시지: key=order-... headers=event_type=PaymentCompleted message_id=... value={...}

# 커넥터 상태 확인
curl -s localhost:8083/connectors/payment-outbox-connector/status
```

`cmd/main.go` 는 `public` 스키마를 쓴다 (커넥터의 `table.include.list = public.outbox`).

## Polling vs CDC 트레이드오프

| | Polling (03) | CDC (04) |
|---|---|---|
| 지연 | 폴링 주기만큼 (수백 ms ~ 수 초) | WAL 을 스트리밍하므로 매우 짧음 |
| DB 부하 | 주기적 SELECT/UPDATE, 발행 마킹 UPDATE | WAL 읽기만 (쿼리 부하 거의 없음) |
| 운영 복잡도 | 앱 코드만 있으면 됨 | Kafka Connect 클러스터, 커넥터, replication slot 관리 |
| 장애 시 위험 | 릴레이가 멈추면 outbox 가 쌓임 | 커넥터가 멈추면 **replication slot 때문에 WAL 이 디스크에 계속 쌓임** (디스크 풀 위험) |
| 중복 | at-least-once | 역시 at-least-once (커넥터 재시작 시 마지막 offset 이후 재전송) → 컨슈머 멱등성 필요 |
| outbox 정리 | `published_at` 기준 배치 삭제 | INSERT 직후 같은 tx 에서 DELETE 해도 됨 (WAL 에 INSERT 가 남으므로). 또는 주기적 삭제 |
| 스키마 변경 | 자유 | 커넥터 설정과 컬럼명 계약을 맞춰야 함 |

## 한계 / 다음 단계

- CDC 도 중복 전달이 가능하다 → [05-idempotent-consumer](../05-idempotent-consumer) 에서 `message_id` 로 걸러낸다.
- E2E 를 Testcontainers 로 자동화할 수도 있지만 (Postgres + Redpanda + Debezium Connect 컨테이너 3개, 같은 네트워크),
  기동이 느리고 불안정할 수 있어 이 저장소에서는 수동 절차로 남겨두었다.
