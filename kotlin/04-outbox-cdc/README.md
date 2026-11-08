# 04. Transactional Outbox + CDC (Debezium Outbox Event Router)

## 문제

03의 Polling Relay는 동작하지만:
- 폴링 주기만큼 **지연**이 생기고, 이벤트가 없어도 계속 **DB를 두드린다**.
- relay 코드(조회, 잠금, 발행, published_at 갱신, 재시도)를 **직접 작성·운영**해야 한다.

## 해결

애플리케이션은 **outbox insert까지만** 한다 (03과 같은 한 트랜잭션). 발행 코드는 전부 사라진다.

Postgres는 모든 커밋을 WAL(Write-Ahead Log)에 기록한다. `wal_level=logical`로 두면 외부에서 이 로그를 구독할 수 있고,
**Debezium**(Kafka Connect 커넥터)이 WAL을 tail 하다가 `outbox` insert를 발견하면 **Outbox Event Router**가 Kafka 메시지로 바꿔 보낸다.

| outbox 컬럼 | Kafka 메시지 | 커넥터 설정 (`infra/debezium/outbox-connector.json`) |
|---|---|---|
| `aggregatetype` = `payment` | 토픽 `payment.events` | `route.by.field`, `route.topic.replacement=${routedByValue}.events` |
| `aggregateid` = orderId | key | `table.field.event.key` |
| `id` | 헤더 `message_id` | `table.fields.additional.placement` |
| `type` | 헤더 `event_type` | `table.fields.additional.placement` |
| `payload` | value (JSON 그대로) | `table.expand.json.payload=true` |

> 실제 메시지에는 헤더 `id`도 하나 더 붙는다 (Event Router가 기본으로 넣는 이벤트 id, `message_id`와 같은 값). 컨슈머는 `message_id`를 쓰면 된다.

`published_at` 컬럼이 없다는 점에 주목. "어디까지 보냈는지"는 Debezium이 WAL 위치(LSN)를 Kafka에 offset으로 저장해서 추적한다.

## 핵심 코드 위치

- `src/main/kotlin/lab/outbox/cdc/PaymentService.kt:25` — 결제 + outbox를 한 트랜잭션에 (`:27` outbox insert)
- `src/main/kotlin/lab/outbox/cdc/OutboxRepository.kt:20` — insert만 존재 (03의 relay/조회/mark 코드가 없음)
- `src/main/resources/schema.sql` — Debezium 기본 컬럼명
- `build.gradle.kts` — Kafka 의존성 자체가 없다
- `../../infra/debezium/outbox-connector.json` — 커넥터 설정

## 테스트 실행

```bash
cd kotlin
./gradlew :04-outbox-cdc:test
```

| 테스트 | 증명하는 것 |
|---|---|
| `pay__writes_outbox_row` | 결제와 함께 Debezium 라우팅 규칙에 맞는 outbox 행이 생김 |

### E2E 수동 실습 (Debezium)

```bash
# 1) 인프라 기동 (Postgres wal_level=logical + Redpanda + Kafka Connect)
cd infra
docker compose --profile cdc up -d

# 2) 앱을 한 번 실행해 테이블 생성 + 결제 1건 (demo 프로필은 결제 하나 만들고 종료)
cd ../kotlin
./gradlew :04-outbox-cdc:bootRun --args='--spring.profiles.active=demo'

# 3) 커넥터 등록 (outbox 테이블이 먼저 있어야 한다)
curl -X POST -H 'Content-Type: application/json' \
  --data @../infra/debezium/outbox-connector.json http://localhost:8083/connectors
curl http://localhost:8083/connectors/payment-outbox-connector/status

# 4) 토픽 확인 — key, 헤더(message_id, event_type), payload가 보인다
docker compose -f ../infra/docker-compose.yml exec redpanda \
  rpk topic consume payment.events -n 1

# 5) 결제를 더 만들면 거의 즉시(폴링 주기 없이) 토픽에 나타난다
./gradlew :04-outbox-cdc:bootRun --args='--spring.profiles.active=demo'
```

3)은 `../infra/debezium/register.sh`로 대신할 수 있다 (jq 필요).
(커넥터는 등록 시점에 스냅샷을 찍으므로 등록 전에 만든 결제도 함께 발행된다)

> **이 단계만 `public` 스키마를 쓴다.** 다른 Kotlin 단계는 `k01`~`k10` 스키마에 테이블을 두지만(`spring.datasource.hikari.schema`),
> 커넥터의 `table.include.list`가 `public.outbox`이고 Go 04도 같은 테이블에 쓰므로 04는 `public`에 남겼다 (`application.yml` 주석).
> Kotlin 03은 `k03.outbox_polling`, Go 03은 `go_03.outbox`를 쓰므로 `public.outbox`는 이 단계(Kotlin·Go 04) 전용이다.
> 예전 버전의 Kotlin 03(테이블 이름이 `outbox`였음, public)을 이 DB에 돌린 적이 있다면 2) 전에 `DROP TABLE outbox;` 할 것 —
> 남아 있으면 `CREATE TABLE IF NOT EXISTS`가 그 테이블을 그대로 두어 insert가 `column "aggregatetype" does not exist`로 실패한다.

## Polling vs CDC 트레이드오프

| | Polling (03) | CDC (04) |
|---|---|---|
| 지연 | 폴링 주기 (수백 ms~초) | WAL 기반, 거의 실시간 |
| DB 부하 | 주기적 쿼리 + published_at UPDATE | WAL 읽기만 (쿼리 없음) |
| 운영 복잡도 | 앱 코드만 (인프라 추가 없음) | Kafka Connect 클러스터, 커넥터, replication slot 관리 |
| 장애 시 | relay가 재시도 | **replication slot이 WAL을 붙잡아 디스크가 찰 수 있음** (커넥터 장시간 중단 시 주의) |
| outbox 정리 | 발행 완료 행을 지우는 배치 필요 | insert 직후 같은 트랜잭션에서 delete 해도 됨 (WAL에는 insert가 남음) — 테이블이 커지지 않음 |
| 보장 | at-least-once | at-least-once (커넥터 재시작 시 마지막 offset 이후 재발행) |

## 한계 / 다음 단계

- CDC도 at-least-once다. 커넥터가 offset 커밋 전에 죽으면 재발행된다 → **05 멱등 컨슈머**는 여전히 필요하다.
- 스키마 변경(outbox 컬럼 변경) 시 커넥터 설정도 함께 바꿔야 한다.
- 이 저장소에는 Debezium까지 띄우는 자동 E2E 테스트는 포함하지 않았다 (이미지가 크고 기동이 느려 수동 절차로 대신).
