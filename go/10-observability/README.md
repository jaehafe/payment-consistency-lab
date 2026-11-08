# 10. 관측성 — 로그 · 메트릭 · 장애 시나리오

## 문제

새벽 3시에 "결제가 이상하다"는 연락이 온다. 그때 필요한 건:
- **지금 무슨 일이 일어나는가** → 메트릭 (실패율, PG 지연, UNKNOWN 누적)
- **특정 요청이 어떻게 처리됐는가** → 그 요청의 모든 로그를 한 번에 (request_id)
- **무엇부터 볼지** → 런북

## 해결

07 의 결제 흐름을 간소화해(PG 호출 + UNKNOWN + 멱등키 replay) 관측성을 붙였다.

**구조화 로그 (`log/slog` JSON)** — 모든 줄에 `request_id`, `payment_id`, `order_id`, `idempotency_key` 가 자동으로 붙는다.

```json
{"time":"...","level":"INFO","msg":"pg call finished","pg_duration_ms":80,"status":"APPROVED","error":"","request_id":"demo-1","idempotency_key":"k1","order_id":"o-1","payment_id":"f3adda91-..."}
{"time":"...","level":"INFO","msg":"http request","method":"POST","path":"/payments","status":201,"duration_ms":86,"request_id":"demo-1","idempotency_key":"k1","order_id":"o-1","payment_id":"f3adda91-..."}
```

- 미들웨어가 `X-Request-Id` 헤더를 받거나(없으면 생성) 응답 헤더로 돌려주고, `context.Context` 에 로그 필드 묶음을 넣는다.
  헤더 값은 `^[A-Za-z0-9._-]{1,64}$` 일 때만 쓰고, 아니면 새로 만든다 (값이 그대로 로그 필드가 되므로 인젝션/거대 값 방지, Kotlin `RequestIdFilter` 와 같은 규칙).
- 서비스가 결제 행을 만들면 같은 묶음에 `payment_id` 를 추가 → 이후 모든 로그(미들웨어의 access log 포함)에 붙는다.
- Spring 의 MDC 와 같은 역할. Go 에는 스레드 로컬이 없어서 ctx 로 전달하고, `slog.Handler` 데코레이터가 ctx 에서 꺼내 붙인다.

**메트릭 (Prometheus, `/metrics`, 포트 8091)** — 이름은 `infra/observability/alerts.yml` 과 정확히 맞춘다.

| 메트릭 | 타입 | 의미 | 쓰는 알림 |
|---|---|---|---|
| `payment_requests_total{result="approved\|failed\|unknown\|replayed\|conflict"}` | counter | 요청 결과별 수 (모든 라벨 0 으로 미리 생성) | PaymentFailureRateHigh |
| `pg_call_duration_seconds` | histogram | PG 호출 시간 (버킷에 3초 경계 포함) | PgLatencyHigh |
| `payment_unknown_current` | gauge | 지금 UNKNOWN 인 결제 수 — 스크레이프마다 **DB 를 셈** (GaugeFunc) | PaymentUnknownPiling |
| `idempotency_replays_total` | counter | 멱등키 재요청 replay 수 | DuplicateRequestStorm |

전용 `prometheus.Registry` 를 쓴다 → 테스트마다 새 레지스트리라 값이 섞이지 않고, Go/프로세스 런타임 메트릭도 함께 노출.

## 핵심 코드 위치

- `logging.go:36` `AddLogAttrs` — `context.WithValue` 로 요청 단위 로그 필드 묶음, `logging.go:73` `contextHandler`(slog 데코레이터)
- `server.go:47` `RequestLogging` 미들웨어 — `:51` X-Request-Id 검증(`:41` 정규식), `:60` `r.WithContext(ctx)` 로 전파, access log
- `server.go:25` `Routes` — 미들웨어 없는 mux. `cmd/main.go` 가 여기에 `/admin/faults` 를 붙인 뒤 전체를 미들웨어로 한 번 감싼다 → 장애 주입 요청도 request_id 가 붙은 access log 로 남는다
- `metrics.go:44` `NewMetrics` — `:45` 전용 레지스트리, `:56` 버킷, `:67` DB 기반 GaugeFunc, `:84` 라벨 0 초기화
- `service.go:49` `pay` — `:67` PG 타임아웃, `:72` 히스토그램 기록, `:82` 타임아웃 → UNKNOWN, `service.go:102` replay/conflict
- `gateway.go:71` `FaultsHandler` — 실행 중 장애 주입 (`/admin/faults`)
- `cmd/main.go` — :8091 서버

## 테스트 실행

```bash
cd go
go test ./10-observability/ -v
```

| 테스트 | 증명하는 것 |
|---|---|
| `Test_request_id_propagated_to_all_logs` | 로그를 버퍼로 캡처 → 4줄 모두 같은 request_id + order_id + idempotency_key (+ payment_id). 헤더 없으면 생성된 id 가 응답 헤더와 모든 로그에 |
| `Test_invalid_request_id_header__replaced` | 따옴표가 든 값 / 65자 / 공백이 든 X-Request-Id → 버리고 새 id 생성, 그 값은 응답 헤더에도 로그에도 없음 |
| `Test_pg_timeout__unknown_counter_and_gauge_increase` | PG 무응답 3건 → 202, `result="unknown"` 3, 게이지 3. DB 에서 1건 확정하면 게이지 2 |
| `Test_duplicate_requests__replay_metric_increments` | 같은 key 3번 → replays 2, approved 1, PG 호출 1, 응답 동일 |
| `Test_pg_latency__histogram_records_duration` | PG 150ms → 샘플 1, 합 ≥ 0.15s, le=0.1 버킷 0 / le=0.25 버킷 1 |
| `Test_metrics_endpoint_exposes_payment_metrics` | `GET /metrics` 에 4개 메트릭 이름(알림 규칙이 쓰는 그대로)이 노출 |

## 직접 띄워보기

```bash
# 저장소 루트에서: postgres + prometheus(:9090) + grafana(:3000, admin/admin)
docker compose -f infra/docker-compose.yml --profile obs up -d
cd go && go run ./10-observability/cmd | tee /tmp/pay.log      # :8091, 스키마 go_10

# 평상시 트래픽 (다른 터미널)
for i in $(seq 1 1000); do
  curl -s -o /dev/null -X POST localhost:8091/payments -H "Idempotency-Key: k-$i" \
    -d "{\"orderId\":\"o-$i\",\"amount\":10000}"; sleep 0.2; done
```

Prometheus `http://localhost:9090/alerts` 에서 알림 상태, Grafana 에서 아래 PromQL 로 그래프를 본다.

---

## 런북

공통: 알림 → **메트릭으로 범위 파악** → **로그를 request_id 로 grep** → 원인 판단 → 조치.
로그 한 요청 전체 보기: `grep '"request_id":"<id>"' /tmp/pay.log | jq .`

### 시나리오 1 — PG 지연 급증

재현: `curl -X POST 'localhost:8091/admin/faults?latency=4s'` (복구: `latency=80ms`)

1. **알림**: `PgLatencyHigh` (p99 > 3s, 5분 지속). 곧이어 사용자 체감 지연.
2. **메트릭**:
   ```promql
   histogram_quantile(0.99, sum by (le) (rate(pg_call_duration_seconds_bucket[5m])))   # p99
   histogram_quantile(0.50, sum by (le) (rate(pg_call_duration_seconds_bucket[5m])))   # p50 도 올랐나? (전체 지연 vs 일부 지연)
   sum by (result) (rate(payment_requests_total[5m]))                                    # 아직 approved 인가, unknown 으로 넘어가기 시작했나
   go_goroutines                                                                          # 대기 중인 요청이 쌓이는가
   ```
3. **로그**: 느린 요청 찾기 → request_id 로 전체 흐름
   ```bash
   jq -c 'select(.msg=="pg call finished" and .pg_duration_ms > 3000) | {request_id, payment_id, pg_duration_ms}' /tmp/pay.log | head
   grep '"request_id":"<위에서 찾은 id>"' /tmp/pay.log | jq .
   ```
4. **판단**: `pg call finished` 의 `pg_duration_ms` 만 크고 나머지 구간(`http request.duration_ms - pg_duration_ms`)은 작다 → 우리 쪽이 아니라 **PG 지연**. PG 상태 페이지 확인 / PG 사 문의.
   p99 가 PG 타임아웃(여기선 5s)에 가까워지면 곧 시나리오 2 로 번진다.

### 시나리오 2 — PG 타임아웃 급증

재현: `curl -X POST 'localhost:8091/admin/faults?hang=true'` (복구: `hang=false`)

1. **알림** (뜨는 순서): 먼저 `PaymentUnknownPiling` (`payment_unknown_current > 10`, `for: 2m`). 위 트래픽 루프는 요청을 하나씩 보내므로 타임아웃(5s) 동안 5초에 1건으로 느려져 10건을 넘기까지 약 1분, 거기에 2분 → 약 3분 뒤. 그다음 약 5분 뒤 `PaymentFailureRateHigh` (failed+unknown 비율 > 5%, `for: 5m`)와 `PgLatencyHigh` (p99 ≈ 타임아웃 5s > 3s, `for: 5m`)가 거의 함께 뜬다. Prometheus 규칙 평가 주기(`evaluation_interval`)만큼 각 단계가 늦어질 수 있다. 이 레포는 공식 예시처럼 15s 로 두었다 (기본 1m — 1m 일 때 실측은 Piling firing ≈ 4~5분, 나머지 둘 ≈ 7분). 15s 에서 e2e 실측: Piling pending ≈ 66초 / firing ≈ 183초, PgLatencyHigh firing ≈ 322초, PaymentFailureRateHigh firing ≈ 348초.
2. **메트릭**:
   ```promql
   sum(rate(payment_requests_total{result="unknown"}[5m])) / sum(rate(payment_requests_total[5m]))   # UNKNOWN 비율
   payment_unknown_current                                                                            # 누적 (줄지 않으면 janitor 도 문제)
   histogram_quantile(0.99, sum by (le) (rate(pg_call_duration_seconds_bucket[5m])))                  # 타임아웃 값에 붙어 있음
   ```
3. **로그**:
   ```bash
   jq -c 'select(.level=="WARN" and .msg=="pg call finished") | {request_id, payment_id, error, pg_duration_ms}' /tmp/pay.log | head
   ```
   `"error":"context deadline exceeded"`, `pg_duration_ms` ≈ 타임아웃 값 → PG 무응답.
4. **판단/조치**:
   - UNKNOWN 은 **실패가 아니다**. 승인됐을 수 있으므로 사용자에게 재결제를 유도하지 않는다 (이중 결제).
   - 07 의 janitor 가 PG 조회 API 로 확정 중인지 확인 (`payment_unknown_current` 가 PG 복구 후 줄어드는가). 안 줄면 janitor 정지 또는 조회 API 도 장애.
   - 장기화되면 MANUAL_REVIEW 목록(07 README 운영 쿼리)과 다음 날 대사(09)의 `MISSING_IN_INTERNAL` 로 사후 확인.

### 시나리오 3 — 중복 요청 폭주

재현: 같은 key 로 반복 요청
```bash
end=$((SECONDS + 240)); while (( SECONDS < end )); do curl -s -o /dev/null -X POST localhost:8091/payments -H 'Idempotency-Key: storm-1' \
  -d '{"orderId":"o-storm","amount":10000}'; sleep 0.05; done      # 4분 동안 계속
```
> 횟수(예: 500번)로 끊으면 로컬에선 15초 안에 끝나 `rate[1m] > 5` 가 2분을 못 버틴다 → pending 만 되고 firing 은 안 된다.
> 시간으로 돌려야 한다. `evaluation_interval` 15s 에서 시작 후 약 2.5분 뒤 firing (e2e 실측: pending ≈ 32초, firing ≈ 156초. 기본값 1m 일 때는 ≈ 185초).

1. **알림**: `DuplicateRequestStorm` (`rate(idempotency_replays_total[1m]) > 5`, 2분).
2. **메트릭**:
   ```promql
   sum(rate(idempotency_replays_total[1m]))
   sum by (result) (rate(payment_requests_total{result=~"replayed|conflict"}[1m]))   # conflict 가 많으면 "처리 중 재시도" — 클라이언트 타임아웃이 서버 처리보다 짧다
   ```
3. **로그**: 어떤 key/주문이 폭주하는지
   ```bash
   jq -r 'select(.msg=="idempotent replay" or .msg=="duplicate request while in progress") | .idempotency_key' /tmp/pay.log | sort | uniq -c | sort -rn | head
   ```
4. **판단**: 소수의 key 에 몰림 → 특정 클라이언트의 **재시도 루프 버그**(백오프 없음). 여러 key 에 고르게 → 앞단(게이트웨이/LB) 타임아웃이 짧아 일괄 재시도.
   멱등키 덕분에 **이중 결제는 없다**(PG 호출 1회 — `pg_call_duration_seconds_count` 가 함께 늘지 않는 것으로 확인). 조치는 클라이언트 재시도 정책 수정, 필요시 key 단위 rate limit.

## 한계 / 다음 단계

- 트레이싱(OpenTelemetry)은 다루지 않았다. 서비스가 여러 개가 되면 request_id 대신 trace_id 로 서비스 간 흐름을 잇는다.
- 로그 수집(Loki/ELK)은 생략하고 파일 + `jq` 로 대신했다.
- `payment_unknown_current` 는 스크레이프마다 `count(*)` 를 한다. 부분 인덱스로 가볍게 했지만, 인스턴스가 많으면 한 곳(janitor)에서만 노출하는 게 낫다.
- 히스토그램 버킷은 알림 임계값(3s)에 맞춰 고정했다. 임계값을 바꾸면 버킷도 같이 바꿔야 한다.
