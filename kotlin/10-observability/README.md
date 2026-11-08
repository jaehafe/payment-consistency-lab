# 10. 관측성 — 로그 · 메트릭 · 장애 시나리오

## 문제

01~09에서 정합성 장치를 다 넣어도, 운영 중에 "지금 무슨 일이 일어나고 있는가"를 모르면 소용없다.
- PG가 느려지거나 타임아웃이 늘면 UNKNOWN(07)이 쌓이고 고객은 결제 결과를 못 받는다.
- 클라이언트 재시도가 폭주하면 멱등키(02)가 막아 주긴 하지만, 원인(우리 응답 지연)은 그대로다.
- 고객 문의 "결제했는데 주문이 안 됐어요" — 수많은 로그 중 그 요청의 로그를 어떻게 찾나?

## 해결

07의 결제 흐름을 간소화해(PG 호출 + UNKNOWN + 멱등키 재요청) 관측성을 붙였다.

**구조화 로그 (JSON 한 줄)** — Spring Boot `logging.structured.format.console: logstash` + MDC.
`X-Request-Id`를 받아(없으면 생성) MDC에 넣으면 그 요청 중 찍히는 모든 로그에 자동으로 붙고, 응답 헤더로도 돌려준다.

```json
{"@timestamp":"2026-11-08T15:46:00.817+09:00","message":"결제 승인 pg_tx_id=pg_ad1d...","logger_name":"lab.observability.PaymentService",
 "level":"INFO","request_id":"req-ffbb...","idempotency_key":"aa26...","order_id":"order-83d1...","payment_id":"3e61..."}
```

**메트릭 (Prometheus, `:8090/actuator/prometheus`)** — 이름은 `infra/observability/alerts.yml`과 정확히 맞춰져 있다.

| 메트릭 | 종류 | 의미 |
|---|---|---|
| `payment_requests_total{result="approved\|failed\|unknown\|replayed\|conflict"}` | counter | 결과별 요청 수. 모든 라벨을 0으로 미리 등록(비율 계산이 "값 없음"이 되지 않게) |
| `pg_call_duration_seconds` (`_bucket/_count/_sum`, 라벨 `outcome=success\|declined\|timeout`) | histogram | PG 호출 시간 → p99 |
| `payment_unknown_current` | gauge | 지금 UNKNOWN 상태인 결제 수 (스크레이프 때마다 DB에서 셈) |
| `idempotency_replays_total` | counter | 같은 멱등키 재요청에 저장된 결과를 돌려준 횟수 |

## 핵심 코드 위치

- `src/main/kotlin/lab/observability/RequestIdFilter.kt:25` — request_id 결정 → MDC(`:28`) → 응답 헤더, 끝나면 MDC 정리(`:38`, 스레드 재사용 때문)
- `src/main/kotlin/lab/observability/PaymentService.kt:26` — MDC에 idempotency_key/order_id(`:27`), payment_id(`:41`) 추가, 결과별 메트릭
- `src/main/kotlin/lab/observability/PaymentService.kt:66` — 재요청(replayed) / 처리 중(conflict) 판정
- `src/main/kotlin/lab/observability/PaymentMetrics.kt:29` — counter 사전 등록, `:42` DB 기반 gauge, `:55` histogram(`publishPercentileHistogram` `:58`)
- `src/main/kotlin/lab/observability/PaymentGateway.kt:23` — Fake PG. 타임아웃은 `lab.pg.timeout-ms`(기본 5초)만큼 실제로 기다린 뒤 난다(`:46`) → 타임아웃 급증 때 p99도 오른다. 거절(`declineCharge`)도 지연이 타임아웃 이상이면 타임아웃으로 본다(`:36`)
- `src/main/kotlin/lab/observability/PaymentController.kt` — `POST /payments`, 실습용 장애 주입 `POST /admin/pg-faults`
- `src/main/resources/application.yml` — 포트 8090, JSON 로그, prometheus 엔드포인트 노출, `lab.pg.timeout-ms`

## 테스트 실행

```bash
cd kotlin
./gradlew :10-observability:test
```

| 테스트 | 증명하는 것 |
|---|---|
| `request_id_propagated_to_all_logs` | 콘솔 출력을 캡처해 JSON 파싱 → 필터·서비스의 모든 로그(수신/PG 요청/PG 응답/승인/요청 완료)가 같은 request_id, 응답 헤더에도 |
| `pg_timeout__unknown_counter_and_gauge_increase` | 타임아웃 → 202 + `result="unknown"` +1 + `payment_unknown_current` +1, `outcome="timeout"` PG 호출 시간이 타임아웃 설정(테스트 500ms) 이상 |
| `duplicate_requests__replay_metric_increments` | 같은 키 3번 → `idempotency_replays_total` +2, 승인은 1 |
| `pg_latency__histogram_records_duration` | PG 300ms 지연 → timer에 300ms+ 기록, 250ms 초과 버킷으로 들어감 |
| `metrics_endpoint_exposes_payment_metrics` | `/actuator/prometheus`에 네 메트릭과 다섯 result 라벨이 모두 노출 |
| `RequestIdFilterTest.mdc_cleared_between_requests_on_same_thread` | 필터를 직접 두 번 호출(같은 스레드 = 스레드 재사용): 두 번째 요청(400)의 MDC에 첫 요청의 payment_id/order_id/idempotency_key가 없다. `MDC.clear()`를 지우면 실패한다 (스프링/DB 없는 단위 테스트) |

> 스프링 부트 테스트는 기본적으로 메트릭 export를 끈다. 실제 Prometheus 레지스트리를 검증하려고 `@AutoConfigureObservability`를 붙였다.
> 테스트는 `lab.pg.timeout-ms=500`으로 PG 타임아웃을 줄여 돌린다 (지연 테스트의 300ms보다는 길어야 한다 — 지연이 타임아웃 이상이면 Fake PG가 타임아웃을 낸다).

## 로컬에서 띄워 보기

```bash
cd infra
docker compose --profile obs up -d postgres prometheus grafana   # Prometheus :9090, Grafana :3000 (admin/admin)
cd ../kotlin
./gradlew :10-observability:bootRun > /tmp/obs.log               # :8090, JSON 로그를 파일로 (Gradle 출력·배너 같은 비JSON 줄도 섞인다)

# 평소 트래픽 (초당 ~5건)
while true; do
  curl -s -o /dev/null -X POST localhost:8090/payments -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $(uuidgen)" -d "{\"orderId\":\"order-$RANDOM\",\"amount\":10000}"
  sleep 0.2
done
```

Prometheus `http://localhost:9090/alerts`에서 알림 상태, Graph 탭에서 아래 PromQL을 본다.

> `/tmp/obs.log`에는 Gradle 출력과 스프링 배너 같은 JSON이 아닌 줄도 있다. `jq`는 그런 줄에서 파싱 에러로 멈추므로 아래 명령은 모두 `grep '^{'`로 JSON 줄만 거른 뒤 `jq`에 넘긴다.

## 런북

### 시나리오 A — PG 지연 급증

재현: `curl -X POST localhost:8090/admin/pg-faults -H 'Content-Type: application/json' -d '{"latencyMs": 4000}'`

1. **알림**: `PgLatencyHigh` (PG 호출 p99 > 3s, 5분 지속). (지연을 `timeoutMs`(기본 5초) 이상으로 주면 Fake PG가 타임아웃을 내서 시나리오 B가 된다.) 곧이어 클라이언트가 타임아웃 후 재시도하면 `DuplicateRequestStorm`도 뜰 수 있다.
2. **메트릭/PromQL**:
   ```promql
   # p99 (outcome별로 쪼개 보면 "느린 성공"인지 "타임아웃"인지 구분됨)
   histogram_quantile(0.99, sum by (le, outcome) (rate(pg_call_duration_seconds_bucket[5m])))
   # 평균 응답 시간
   sum(rate(pg_call_duration_seconds_sum[5m])) / sum(rate(pg_call_duration_seconds_count[5m]))
   # 처리량이 같이 떨어졌는지 (서블릿 스레드가 PG 대기에 묶이면 처리량 감소)
   sum(rate(payment_requests_total[1m]))
   ```
3. **로그**: 느린 요청 하나를 골라 request_id로 전체 흐름 확인
   ```bash
   grep '^{' /tmp/obs.log | jq -c 'select(.message|startswith("PG 응답"))
          | select((.message|capture("duration_ms=(?<d>[0-9]+)").d|tonumber) > 3000)
          | {request_id, order_id, message}' | head
   grep '"request_id":"<위에서 찾은 id>"' /tmp/obs.log | grep '^{' | jq -c '{"@timestamp", message}'
   ```
4. **판단**: `outcome="success"`의 p99만 높고 실패율이 정상 → PG 쪽 지연 (PG 상태 페이지/담당자 확인). 우리 쪽 DB 쿼리 시간은 정상인지 `요청 완료 duration_ms`와 `PG 응답 duration_ms` 차이로 확인한다. 대응: PG 호출 타임아웃을 짧게 두고 UNKNOWN + janitor(07)로 넘기면 스레드 고갈을 막을 수 있다.

### 시나리오 B — PG 타임아웃 급증

재현: `curl -X POST localhost:8090/admin/pg-faults -H 'Content-Type: application/json' -d '{"timeoutAfterCharge": true}'`
(Fake PG는 승인한 뒤 응답을 안 주고, 호출 쪽은 `timeoutMs`(기본 5초, `"timeoutMs": 3500`처럼 바꿀 수 있음)만큼 기다리다 타임아웃을 본다. 복구: `{"timeoutAfterCharge": false}`)

1. **알림** (뜨는 순서): 먼저 `PaymentUnknownPiling` (`payment_unknown_current > 10`, `for: 2m`). 위의 트래픽 루프는 요청을 하나씩 보내므로 타임아웃 동안 5초에 1건으로 느려져 10건을 넘기까지 약 1분, 거기에 2분 → 약 3분 뒤. 그다음 약 5분 뒤 `PaymentFailureRateHigh` (failed+unknown 비율 > 5%, `for: 5m`)와 `PgLatencyHigh` (타임아웃 호출이 5초씩 걸려 p99 ≈ 5s > 3s, `for: 5m`)가 거의 함께 뜬다. 정확한 시각은 트래픽량과 Prometheus 규칙 평가 주기(`evaluation_interval`, 기본 1m — 각 단계가 최대 1분씩 늦어진다)에 따라 달라지지만 `for`가 짧은 Piling이 먼저다 (e2e 실측: Piling firing ≈ 4~5분, 나머지 둘 ≈ 7분). 타임아웃을 3초 이하로 줄이면 `PgLatencyHigh`는 안 뜬다.
2. **메트릭/PromQL**:
   ```promql
   payment_unknown_current                                            # 계속 오르기만 하면 janitor가 확정을 못 하는 중
   sum by (result) (rate(payment_requests_total[5m]))                 # unknown이 늘었는지, failed가 늘었는지
   sum(rate(pg_call_duration_seconds_count{outcome="timeout"}[5m]))
     / sum(rate(pg_call_duration_seconds_count[5m]))                  # PG 타임아웃 비율
   ```
3. **로그**:
   ```bash
   grep '^{' /tmp/obs.log | jq -c 'select(.message|startswith("PG 응답 불명")) | {request_id, payment_id, order_id, message}' | tail
   ```
   그리고 DB에서: `SELECT count(*), min(created_at) FROM payments WHERE status = 'UNKNOWN';`
4. **판단**: UNKNOWN은 "실패"가 아니다 — 대부분 실제로는 승인됐다 (07). **환불하거나 재결제를 유도하지 말 것.** PG 조회 API로 janitor가 확정하게 두고, PG 장애가 길어지면 MANUAL_REVIEW 목록과 다음 날 정산 파일 대사(09, `MISSING_IN_INTERNAL`)로 정리한다. `payment_unknown_current`가 PG 복구 후에도 줄지 않으면 janitor 정지를 의심한다.

### 시나리오 C — 중복 요청 폭주

재현: 같은 키로 반복 요청
```bash
KEY=$(uuidgen); end=$((SECONDS + 240)); while (( SECONDS < end )); do   # 4분 동안 계속
  curl -s -o /dev/null -X POST localhost:8090/payments -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $KEY" -d '{"orderId":"order-storm","amount":10000}'; sleep 0.05; done
```
> 횟수(예: 500번)로 끊으면 로컬에선 15초 안에 끝나 `rate[1m] > 5`가 2분을 못 버틴다 → pending만 되고 firing은 안 된다.
> 시간으로 돌려야 한다. Prometheus `evaluation_interval`이 기본 1m이라 실제로는 시작 후 약 3분 뒤 firing (e2e 실측 185초).

1. **알림**: `DuplicateRequestStorm` (`idempotency_replays_total` 초당 > 5, 2분 지속).
2. **메트릭/PromQL**:
   ```promql
   sum(rate(idempotency_replays_total[1m]))
   sum by (result) (rate(payment_requests_total{result=~"replayed|conflict"}[1m]))
   ```
   `conflict`(처리 중 409)가 많으면 클라이언트가 응답을 기다리지 않고 재시도하는 중 → 우리 응답이 느린지(시나리오 A) 먼저 확인.
3. **로그**: 어느 키/주문이 폭주 중인지
   ```bash
   grep '^{' /tmp/obs.log | jq -r 'select(.message|startswith("멱등 재요청")) | .idempotency_key' | sort | uniq -c | sort -rn | head
   ```
4. **판단**: 멱등키 덕분에 **이중 결제는 일어나지 않는다** (`result="approved"`는 늘지 않음). 원인은 클라이언트 재시도 정책(백오프 없음, 타임아웃 너무 짧음) 또는 우리 응답 지연. 특정 클라이언트/키 하나라면 해당 클라이언트 팀에 연락, 전체적이면 A를 먼저 해결.

## 한계 / 다음 단계

- `payment_unknown_current`는 스크레이프마다 DB `count(*)`를 친다. 부분 인덱스(`WHERE status='UNKNOWN'`)로 가볍게 했지만, 인스턴스가 많으면 같은 값을 여러 번 센다 → 대시보드에서 `max()`로 보거나 별도 exporter 하나로 뽑는다.
- 로그 수집(Loki/ELK)과 분산 트레이싱(OpenTelemetry, traceparent 전파)은 생략했다. request_id는 그 최소 버전이다.
- 멱등키 재요청 시 요청 바디 비교(422)는 02에서 다뤘으므로 여기선 생략.
- 실습용 `/admin/pg-faults`는 인증이 없다. 실서비스에 이런 엔드포인트를 두면 안 된다.
