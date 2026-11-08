# payment-consistency-lab

결제 시스템의 **데이터 일관성 문제**를 직접 재현하고 해결하며 배우는 실습 레포.
같은 시나리오를 **Spring Boot + Kotlin** 과 **Go** 로 나란히 구현했다.

> 모든 단계의 핵심 주장은 **테스트가 증명**한다. Testcontainers로 실제 PostgreSQL + Kafka(Redpanda)를 띄워서
> "이벤트가 유실된다", "중복 결제가 안 된다", "메시지가 두 번 온다"를 눈으로 확인한다.

## 학습 로드맵

| 단계 | 주제 | 문제 | 해결 | Kotlin | Go |
|---|---|---|---|---|---|
| 01 | Dual Write 문제 | DB 저장과 메시지 발행을 따로 하면 한쪽만 성공한다 (이벤트 유실 / 유령 이벤트) | — (문제 재현) | [kotlin/01](kotlin/01-dual-write-problem) | [go/01](go/01-dual-write-problem) |
| 02 | 멱등키 (Idempotency Key) | 네트워크 재시도·따닥 클릭으로 이중 결제 | Stripe식 `Idempotency-Key` + recovery point + PG 측 멱등성 | [kotlin/02](kotlin/02-idempotency-key) | [go/02](go/02-idempotency-key) |
| 03 | Transactional Outbox (Polling) | 01의 dual write | 결제와 이벤트를 **한 트랜잭션**에 저장 → relay가 `FOR UPDATE SKIP LOCKED`로 발행 | [kotlin/03](kotlin/03-outbox-polling) | [go/03](go/03-outbox-polling) |
| 04 | Transactional Outbox (CDC) | polling의 지연·DB 부하 | Debezium Outbox Event Router가 WAL을 읽어 발행 | [kotlin/04](kotlin/04-outbox-cdc) | [go/04](go/04-outbox-cdc) |
| 05 | 멱등 컨슈머 | outbox는 at-least-once → 메시지 중복 수신 | `processed_messages` 를 같은 트랜잭션에 기록, DB 커밋 후 offset 커밋, 반복 실패는 `<topic>-dlt` 로 (DLT ack 후 offset 커밋) | [kotlin/05](kotlin/05-idempotent-consumer) | [go/05](go/05-idempotent-consumer) |
| 06 | 잔액 동시성 | 동시 인출 → lost update, 이중지불 / 이체 데드락 | 비관적·낙관적 락, `SERIALIZABLE`, 조건부 UPDATE 비교 / 잠금 순서 정렬 | [kotlin/06](kotlin/06-balance-concurrency) | [go/06](go/06-balance-concurrency) |
| 07 | 결제 상태 머신 | PG 타임아웃 = 성공인지 실패인지 모름, 후속 단계 실패 | `UNKNOWN` 상태 + janitor 재조회·백오프 → `MANUAL_REVIEW` 보류 / 오래된 `PENDING` 정리 / PG 취소 보상 | [kotlin/07](kotlin/07-payment-state-machine) | [go/07](go/07-payment-state-machine) |
| 08 | 원장·정산 | 입금·출금·수수료·환불 금액 추적, 정산 중복 | 복식부기 원장(차변=대변, 원장 멱등성), KST 거래일 정산, 시산표 | [kotlin/08](kotlin/08-ledger-settlement) | [go/08](go/08-ledger-settlement) |
| 09 | 대사 | PG 정산 파일 ↔ 내부 거래 불일치 | `FULL OUTER JOIN` / `HAVING count(*) > 1` 대사 SQL, 리포트 | [kotlin/09](kotlin/09-reconciliation) | [go/09](go/09-reconciliation) |
| 10 | 관측성 | 장애 시 원인 파악 불가 | request_id 구조화 로그, Prometheus 메트릭·알림, 시나리오별 런북 | [kotlin/10](kotlin/10-observability) | [go/10](go/10-observability) |

각 단계 README: **문제 → 해결 → 핵심 코드 위치 → 테스트 실행법 → 한계/다음 단계**.
두 구현이 지키는 공통 계약(스키마, 테스트 이름)은 [docs/SPEC.md](docs/SPEC.md).

### 테스트로 증명하는 것들

- **01** 커밋 후 발행 실패 → 결제 1건, 이벤트 0건 / 발행 후 커밋 실패 → 결제 0건, 유령 이벤트 1건 / 트랜잭션 안 발행 후 롤백 → 이벤트는 이미 나감
- **02** 같은 키로 10개 동시 요청 → PG 호출 1회·승인 1회 (응답은 보통 201 ×1, 409 ×9 — 처리 완료 후 도착한 요청은 저장된 201을 replay 받으므로 201 수는 타이밍 의존) / PG 승인 후 응답 유실 → 재시도해도 승인 1회
- **03** relay 2개 동시 실행 → 이벤트(Kotlin 40건, Go 30건)가 각각 정확히 1번 발행 (`FOR UPDATE SKIP LOCKED` 통째로 지우면 중복 발행으로 실패, `SKIP LOCKED`만 지우면 중복 없이 대기만 생김) / 발행 후 마킹 전 크래시 → 같은 `message_id` 2번 발행 (at-least-once)
- **05** 나이브 컨슈머는 중복 메시지에 포인트 2배 적립 / 멱등 컨슈머는 offset 커밋 전 크래시 후 재전달돼도 skip / 처리 실패한 메시지를 건너뛰고 offset을 커밋하지 않음 / 3번 실패한 메시지와 `message_id` 없는 메시지는 DLT 로 보내고 뒤 메시지는 계속 처리 / DLT 발행이 실패하면 offset 을 넘기지 않음
- **06** 잔액 1,000원 계좌에 1,000원 인출 10건 동시 → 나이브는 10건 모두 성공(이중지불), 나머지 전략은 정확히 1건 (낙관적·SERIALIZABLE은 재시도 정확히 9회) / 잠금 순서 미정렬 이체는 실제 데드락(`40P01`)
- **07** 10개 동시 확정 시도 중 1개만 승리 / UNKNOWN → janitor가 PG 조회로 확정, PG에 도달 못한 결제는 유예 후 FAILED, 판단 불가가 계속되면 MANUAL_REVIEW / 크래시로 남은 PENDING은 threshold(2m) 후 PG 조회로 확정(결론 없으면 UNKNOWN 흐름으로) / janitor는 앱 안에서 주기 실행 (killbill 방식) / 재고 실패 시 PG 취소 보상, 취소 실패도 재시도
- **08** 차변=대변 강제, 같은 거래 두 번 전기/기록 불가, 정산 중 늦게 커밋된 거래는 정산 표시 안 됨, 수수료 원 단위 반올림, UTC 15:30 거래는 KST 다음날 정산, 시산표 합계 0
- **09** 누락(양방향)·금액 차이·중복(양방향) 5종 탐지, 같은 파일 재import 무시(줄바꿈만 달라도), KST 자정 경계, 차액 합계 리포트
- **10** 한 요청의 모든 로그에 같은 request_id / PG 타임아웃 시 UNKNOWN 메트릭 증가 / 중복 요청 시 replay 메트릭 증가 / 같은 스레드의 다음 요청에 이전 요청 MDC가 새지 않음

## 실행

필요: Docker, (Kotlin) JDK 21, (Go) Go 1.22+

```bash
# Kotlin — Gradle wrapper 포함
cd kotlin && ./gradlew test
./gradlew :02-idempotency-key:test      # 단계별

# Go
cd go && go test ./...
go test ./02-idempotency-key/ -v        # 단계별
```

로컬 compose DB(`localhost:15432/lab`)는 모든 단계가 같이 쓰므로 **단계마다 Postgres 스키마를 나눈다** — Kotlin 단계 N은 `kNN`(`k01`~`k10`), Go는 `go_NN`, 04만 Kotlin·Go 모두 `public`(Debezium 커넥터가 `public.outbox`를 본다).
그래서 여러 단계를 차례로 `bootRun` 해도 이름이 같은 `payments`(01~04, 07, 10이 컬럼이 다 다르다)가 섞이지 않는다. psql로 볼 때는 `SET search_path TO k07;` 처럼 먼저 맞춘다. 규칙은 [docs/SPEC.md](docs/SPEC.md) "스키마 규칙".

04(Debezium CDC) 실습:
```bash
docker compose -f infra/docker-compose.yml --profile cdc up -d
./infra/debezium/register.sh
# 이후 각 04 README 절차대로 앱 실행 → payment.events 토픽 확인
```

10(관측성) 실습:
```bash
docker compose -f infra/docker-compose.yml --profile obs up -d   # Prometheus :9090, Grafana :3000 (admin/admin)
# Kotlin 앱 :8090, Go 앱 :8091 실행 후 각 10 README의 런북대로 장애 주입
```

## 레포 구조

```
docs/SPEC.md          두 구현의 공통 계약
infra/                docker-compose (Postgres wal_level=logical, Redpanda, Debezium Connect, Prometheus, Grafana), 커넥터·알림 설정
kotlin/               Gradle 멀티모듈, Spring Boot 3.5 + Kotlin 2.2, JdbcTemplate (ORM 없이 트랜잭션 경계 노출)
go/                   단일 Go 모듈, pgx/v5 + franz-go (Kotlin 개발자용 Go 문법 주석 포함)
```

## 참고한 오픈소스

| 레포 | 이 레포에서 대응하는 단계 |
|---|---|
| [brandur/rocket-rides-atomic](https://github.com/brandur/rocket-rides-atomic) | 02 — idempotency key, atomic phase, recovery point |
| [brandur/rocket-rides-unified](https://github.com/brandur/rocket-rides-unified) | 03 — transactionally-staged job drain |
| [aws-powertools/powertools-lambda-python](https://github.com/aws-powertools/powertools-lambda-python) | 02 — 프로덕션 수준 멱등성 유틸리티 |
| [eventuate-tram/eventuate-tram-core](https://github.com/eventuate-tram/eventuate-tram-core) · [examples](https://github.com/eventuate-tram/eventuate-tram-examples-customers-and-orders) | 03, 05 — outbox, idempotent consumer |
| [debezium/debezium-examples](https://github.com/debezium/debezium-examples) (`outbox`) | 04 — CDC outbox |
| [gruelbox/transaction-outbox](https://github.com/gruelbox/transaction-outbox) | 03 — 작고 읽기 쉬운 outbox 라이브러리 |
| [spring-projects/spring-modulith](https://github.com/spring-projects/spring-modulith) | 03 — Event Publication Registry |
| [killbill/killbill](https://github.com/killbill/killbill) | 07 — 결제 상태 머신, `payment/core/janitor` |
| [eventuate-tram/eventuate-tram-sagas](https://github.com/eventuate-tram/eventuate-tram-sagas) | 07 — 보상 트랜잭션(saga) |
| [blnkfinance/blnk](https://github.com/blnkfinance/blnk) | 06, 08, 09 — 원장, 잔액, 대사(`api/reconciliation_api.go`) |
| [formancehq/ledger](https://github.com/formancehq/ledger) | 08 — 프로그래머블 원장 |
| [tigerbeetle/tigerbeetle](https://github.com/tigerbeetle/tigerbeetle) | 06, 08 — 금융 거래 전용 DB (복식부기·잔액 제약을 DB 레벨로) |
| [juspay/hyperswitch](https://github.com/juspay/hyperswitch) | 실제 결제 스위치 전체 구조 |

개념 자료: [microservices.io — Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html), [Idempotent Consumer](https://microservices.io/patterns/communication-style/idempotent-consumer.html), Brandur — *Implementing Stripe-like Idempotency Keys in Postgres*, Chris Richardson — *Microservices Patterns* 3~4장.

## 알려진 한계

- 04 Debezium E2E는 자동 테스트 없이 README의 수동 절차만 제공 (아직 실제 Debezium으로 검증하지 않음)
- 07 오래된 PENDING 판단은 `created_at` 기준(PENDING 진입 시각 아님). Go janitor는 PG 호출을 트랜잭션 안에서 하므로 크래시하면 확인 횟수도 롤백됨 (각 README 한계 항목 참고)
- 05 DLT 실패 횟수는 메모리 (재시작 시 리셋), DLT 재처리 도구 없음
- 10 Prometheus/Grafana 알림이 실제로 뜨는지는 아직 직접 띄워서 검증하지 않음
- 08 PG 수수료 비용 계정 없이 단순화 (PG가 총액에서 환불만 차감해 입금한다고 가정)
- 각 단계는 독립 실행 가능하도록 일부 코드(Fake PG 등)를 의도적으로 중복
