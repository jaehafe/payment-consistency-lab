#!/usr/bin/env bash
# 10-observability end to end: Kotlin (:8090) + Go (:8091) scraped by the compose Prometheus (profile `obs`).
#
#   e2e/10-observability.sh --quick   # ~1 min: scrape targets up, metrics in Prometheus, request_id in JSON logs, Grafana datasource health
#   e2e/10-observability.sh           # + runbook scenarios with real alerts (~15 min):
#        B) PG timeout storm (Kotlin timeoutAfterCharge / Go hang) with the README's sequential traffic loop
#           -> PaymentUnknownPiling, PgLatencyHigh, PaymentFailureRateHigh must reach pending then firing,
#              Piling first (README order). Timeline is printed.
#        C) duplicate-request storm: first the README's literal 500-request loop (records whether it is enough
#           for `for: 2m`), then a sustained same-key loop until DuplicateRequestStorm fires. No double charge.
#
# Needs ports 8090/8091 free (prometheus.yml scrapes host.docker.internal:8090 / :8091).
# Uses scratch DBs e2e_k10 / e2e_g10 (dropped and recreated). Never runs `compose down`.
# Helpers are inline on purpose (independent of e2e/lib.sh). Works with macOS bash 3.2.
set -euo pipefail

QUICK=0; [[ "${1:-}" == "--quick" ]] && QUICK=1
REPO="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$REPO/infra/docker-compose.yml"
LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pcl-e2e-10.XXXXXX")"
PROM=http://localhost:9090
GRAFANA=http://localhost:3000
KJOB=kotlin-10-observability; GJOB=go-10-observability
ALERT_TIMEOUT="${ALERT_TIMEOUT:-900}"   # seconds to wait for scenario B alerts

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
log() { printf '\033[36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { printf '\033[31mFATAL:\033[0m %s\n' "$*" >&2; exit 1; }
psql_q() { compose exec -T postgres psql -U lab -d "$1" -v ON_ERROR_STOP=1 -qtAX -c "$2"; }

PASS=0; FAIL=0; RESULTS=()
check() {
  local d="$1"; shift
  if "$@"; then PASS=$((PASS+1)); RESULTS+=("PASS  $d"); printf '  \033[32mPASS\033[0m %s\n' "$d" >&2
  else FAIL=$((FAIL+1)); RESULTS+=("FAIL  $d"); printf '  \033[31mFAIL\033[0m %s\n' "$d" >&2; fi
}
check_eq() { local d="$1" e="$2" a="$3"; if [[ "$e" == "$a" ]]; then check "$d (= $a)" true; else check "$d (expected '$e', got '$a')" false; fi; }

PIDS=""
cleanup() {
  local p
  for p in $PIDS; do kill "$p" 2>/dev/null || true; done
  sleep 1
  for p in $PIDS; do kill -9 "$p" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM
bg() { "$@" & PIDS="$PIDS $!"; }

prom_q() { curl -s --get "$PROM/api/v1/query" --data-urlencode "query=$1"; }
prom_val() { prom_q "$1" | jq -r '.data.result[0].value[1] // "none"'; }
metric() { # metric URL NAME_REGEX -> sum of matching sample values on the app's own endpoint
  curl -s "$1" | awk -v re="$2" '$0 !~ /^#/ && $1 ~ re {s+=$NF} END {printf "%d\n", s}'
}

# pay PORT KEY ORDER [REQUEST_ID] -> prints the HTTP status
pay() {
  local hdr=(); [[ -n "${4:-}" ]] && hdr=(-H "X-Request-Id: $4")
  curl -s -m 15 -o /dev/null -w '%{http_code}' -X POST "localhost:$1/payments" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $2" "${hdr[@]+"${hdr[@]}"}" -d "{\"orderId\":\"$3\",\"amount\":10000}"
}

# README traffic loop (sequential, sleep 0.2) until the stop file appears.
traffic() {
  local port="$1" stop="$2" i=0
  while [[ ! -f "$stop" ]]; do
    i=$((i+1)); pay "$port" "t-$port-$$-$i" "order-t-$port-$i" >/dev/null || true; sleep 0.2
  done
}

# ---------------------------------------------------------------------------------------------
log "1) infra: postgres + prometheus + grafana (profile obs)"
compose --profile obs up -d postgres prometheus grafana >/dev/null 2>&1 || die "compose up failed"
for i in $(seq 1 60); do curl -sf "$PROM/-/ready" >/dev/null 2>&1 && curl -sf "$GRAFANA/api/health" >/dev/null 2>&1 && break; sleep 1; done
for p in 8090 8091; do lsof -nP -iTCP:$p -sTCP:LISTEN >/dev/null 2>&1 && die "port $p is busy (prometheus.yml scrapes it; stop that process first)"; done
for d in e2e_k10 e2e_g10; do psql_q lab "DROP DATABASE IF EXISTS $d WITH (FORCE)" >/dev/null 2>&1; psql_q lab "CREATE DATABASE $d" >/dev/null; done

log "2) start apps (Kotlin :8090 db e2e_k10, Go :8091 db e2e_g10)"
JAR="$(ls "$REPO"/kotlin/10-observability/build/libs/*.jar 2>/dev/null | head -1 || true)"
if [[ -z "$JAR" ]]; then (cd "$REPO/kotlin" && ./gradlew -q :10-observability:bootJar) >&2; JAR="$(ls "$REPO"/kotlin/10-observability/build/libs/*.jar | head -1)"; fi
(cd "$REPO/go" && go build -o "$LOG_DIR/obs-go" ./10-observability/cmd)
KLOG="$LOG_DIR/kotlin.log"; GLOG="$LOG_DIR/go.log"
java -jar "$JAR" --spring.datasource.url=jdbc:postgresql://localhost:15432/e2e_k10 >"$KLOG" 2>&1 & PIDS="$PIDS $!"
DATABASE_URL='postgres://lab:lab@localhost:15432/e2e_g10?sslmode=disable' "$LOG_DIR/obs-go" >"$GLOG" 2>&1 & PIDS="$PIDS $!"
for i in $(seq 1 60); do
  curl -sf localhost:8090/actuator/prometheus >/dev/null 2>&1 && curl -sf localhost:8091/metrics >/dev/null 2>&1 && break; sleep 1
done
curl -sf localhost:8090/actuator/prometheus >/dev/null || { tail -20 "$KLOG"; die "kotlin app not up"; }
curl -sf localhost:8091/metrics >/dev/null || { tail -20 "$GLOG"; die "go app not up"; }

log "3) Prometheus scrapes both targets (via host.docker.internal)"
for i in $(seq 1 30); do
  up="$(curl -s "$PROM/api/v1/targets" | jq -r --arg k $KJOB --arg g $GJOB '[.data.activeTargets[]|select(.labels.job==$k or .labels.job==$g)|select(.health=="up")]|length')"
  [[ "$up" == 2 ]] && break; sleep 2
done
curl -s "$PROM/api/v1/targets" | jq -r '.data.activeTargets[]|"    \(.labels.job) \(.scrapeUrl) health=\(.health) \(.lastError)"' >&2
check_eq "both targets health=up in /api/v1/targets" 2 "$up"
check_eq "all 4 alert rules loaded and healthy" 4 "$(curl -s "$PROM/api/v1/rules" | jq '[.data.groups[].rules[]|select(.type=="alerting" and .health=="ok")]|length')"

log "4) metrics: send a few requests, then look for them in Prometheus"
for p in 8090 8091; do for i in 1 2 3; do pay $p "warm-$p-$i" "order-warm-$p-$i" >/dev/null; done; pay $p "warm-$p-1" "order-warm-$p-1" >/dev/null; done
sleep 12   # > 2 scrape intervals
for job in $KJOB $GJOB; do
  check_eq "$job: payment_requests_total has 5 result labels" 5 "$(prom_q "count(payment_requests_total{job=\"$job\"})" | jq -r '.data.result[0].value[1]')"
  q="sum(payment_requests_total{job=\"$job\",result=\"approved\"})"   # (bash 3.2 brace-expands {a,b} inside "$( )")
  check_eq "$job: approved total" 3 "$(prom_val "$q")"
  check_eq "$job: idempotency_replays_total" 1 "$(prom_val "sum(idempotency_replays_total{job=\"$job\"})")"
  check_eq "$job: payment_unknown_current" 0 "$(prom_val "sum(payment_unknown_current{job=\"$job\"})")"
  check_eq "$job: pg_call_duration_seconds_count" 3 "$(prom_val "sum(pg_call_duration_seconds_count{job=\"$job\"})")"
  check "$job: pg_call_duration_seconds_bucket present (le series > 5)" \
    test "$(prom_val "count(pg_call_duration_seconds_bucket{job=\"$job\"})")" -gt 5
done

log "5) request_id in JSON logs"
for p in 8090 8091; do
  [[ $p == 8090 ]] && f="$KLOG" || f="$GLOG"
  rid="e2e-rid-$p-$RANDOM"
  echo_hdr="$(curl -s -D - -o /dev/null -X POST localhost:$p/payments -H 'Content-Type: application/json' -H "X-Request-Id: $rid" \
    -H "Idempotency-Key: rid-$rid" -d "{\"orderId\":\"order-$rid\",\"amount\":10000}" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-request-id"{print $2}')"
  check_eq ":$p echoes X-Request-Id" "$rid" "$echo_hdr"
  sleep 0.5
  n="$(grep '^{' "$f" | jq -c --arg r "$rid" 'select(.request_id==$r)' | wc -l | tr -d ' ')"
  check ":$p: >=3 JSON log lines carry request_id=$rid (got $n)" test "$n" -ge 3
  check ":$p: those lines also carry order_id + idempotency_key + payment_id" \
    test "$(grep '^{' "$f" | jq -c --arg r "$rid" 'select(.request_id==$r and .order_id=="order-"+$r and .idempotency_key=="rid-"+$r and (.payment_id|type)=="string")' | wc -l | tr -d ' ')" -ge 2
  gen="$(curl -s -D - -o /dev/null -X POST localhost:$p/payments -H 'Content-Type: application/json' -H "Idempotency-Key: gen-$RANDOM$RANDOM" \
    -d '{"orderId":"order-gen","amount":10000}' | tr -d '\r' | awk -F': ' 'tolower($1)=="x-request-id"{print $2}')"
  sleep 0.5
  check ":$p: no header -> generated id '$gen' returned and present in logs" \
    test -n "$gen" -a "$(grep -c "\"request_id\":\"$gen\"" "$f")" -ge 1
done

log "6) Grafana datasource health"
uid="$(curl -s -u admin:admin "$GRAFANA/api/datasources" | jq -r '.[]|select(.type=="prometheus")|.uid' | head -1)"
check_eq "Grafana Prometheus datasource health" OK "$(curl -s -u admin:admin "$GRAFANA/api/datasources/uid/$uid/health" | jq -r .status)"

if [[ $QUICK == 1 ]]; then log "--quick: skipping alert scenarios"; else
# ---------------------------------------------------------------------------------------------
EVAL="$(curl -s "$PROM/api/v1/status/config" | jq -r .data.yaml | awk '/evaluation_interval/{print $2; exit}')"
log "7) scenario B (PG timeout). Prometheus evaluation_interval=$EVAL. Waiting up to ${ALERT_TIMEOUT}s."
STOP="$LOG_DIR/stop-traffic"; rm -f "$STOP"
bg traffic 8090 "$STOP"; bg traffic 8091 "$STOP"
log "baseline traffic 30s (and wait until no alert from a previous run is still active, max 8 min)"; sleep 30
# A previous full run's storm (~20 replays/s) stays in the alerts' rate[5m] window and dilutes the
# failure ratio, delaying PaymentFailureRateHigh by minutes. Wait until that has aged out too.
RQ='sum(rate(idempotency_replays_total{job=~"kotlin-10-observability|go-10-observability"}[5m])) or vector(0)'
for i in $(seq 1 96); do
  n="$(curl -s "$PROM/api/v1/alerts" | jq --arg k $KJOB --arg g $GJOB '[.data.alerts[]|select(.labels.job==$k or .labels.job==$g)]|length')"
  r="$(prom_val "$RQ")"
  [[ "$n" == 0 ]] && awk -v r="$r" 'BEGIN{exit !(r < 0.5)}' && break
  [[ $i == 1 ]] && log "  waiting for an earlier run to age out ($n alerts active, replay rate[5m]=$r/s)"; sleep 5
done
curl -s -X POST localhost:8090/admin/pg-faults -H 'Content-Type: application/json' -d '{"timeoutAfterCharge": true}' >/dev/null
curl -s -X POST 'localhost:8091/admin/faults?hang=true' >/dev/null
T0=$(date +%s); log "faults injected at $(date +%H:%M:%S) (T0)"
EV="$LOG_DIR/alert-events.txt"; : >"$EV"
# poll_alerts T0 [ALERTNAME] -> appends "<alert>|<job>|<state>|<sec since T0>|<activeAt>" the first time each (alert,job,state) is seen
poll_alerts() {
  local now; now=$(date +%s)
  curl -s "$PROM/api/v1/alerts" | jq -r --arg only "${2:-}" '.data.alerts[]|select($only=="" or .labels.alertname==$only)|"\(.labels.alertname)|\(.labels.job)|\(.state)|\(.activeAt)"' |
  while IFS='|' read -r a j s at; do
    grep -q "^$a|$j|$s|" "$EV" || { echo "$a|$j|$s|$((now-$1))|$at" >>"$EV"; log "  alert $a{job=$j} -> $s at T0+$((now-$1))s"; }
  done
}
fired() { grep -q "^$1|$2|firing|" "$EV"; }
B_ALERTS="PaymentUnknownPiling PgLatencyHigh PaymentFailureRateHigh"
while :; do
  poll_alerts $T0
  done_all=1; for a in $B_ALERTS; do for j in $KJOB $GJOB; do fired $a $j || done_all=0; done; done
  [[ $done_all == 1 ]] && break
  (( $(date +%s) - T0 > ALERT_TIMEOUT )) && { log "timeout waiting for scenario B alerts"; break; }
  sleep 5
done
touch "$STOP"
curl -s -X POST localhost:8090/admin/pg-faults -H 'Content-Type: application/json' -d '{"timeoutAfterCharge": false}' >/dev/null
curl -s -X POST 'localhost:8091/admin/faults?hang=false' >/dev/null
sleep 13   # in-flight requests finish their 5s timeout as UNKNOWN, then >1 scrape interval
for j in $KJOB $GJOB; do
  check_eq "$j: payment_unknown_current (Prometheus) == DB count(UNKNOWN)" \
    "$( [[ $j == $KJOB ]] && psql_q e2e_k10 "SELECT count(*) FROM k10.payments WHERE status='UNKNOWN'" || psql_q e2e_g10 "SELECT count(*) FROM go_10.payments WHERE status='UNKNOWN'")" \
    "$(prom_val "sum(payment_unknown_current{job=\"$j\"})")"
done
t_of() { awk -F'|' -v a="$1" -v j="$2" -v s="$3" '$1==a && $2==j && $3==s {print $4}' "$EV"; }
for j in $KJOB $GJOB; do
  for a in $B_ALERTS; do
    check "$j: $a pending (T0+$(t_of $a $j pending)s) then firing (T0+$(t_of $a $j firing)s)" \
      test -n "$(t_of $a $j pending)" -a -n "$(t_of $a $j firing)"
  done
  pf="$(t_of PaymentUnknownPiling $j firing)"; lf="$(t_of PgLatencyHigh $j firing)"; ff="$(t_of PaymentFailureRateHigh $j firing)"
  check "$j: README order - PaymentUnknownPiling fires before PgLatencyHigh and PaymentFailureRateHigh" \
    test -n "$pf" -a -n "$lf" -a -n "$ff" -a "${pf:-0}" -lt "${lf:-0}" -a "${pf:-0}" -lt "${ff:-0}"
done
check "DuplicateRequestStorm did NOT fire during scenario B (sequential loop does not retry)" bash -c "! grep -q '^DuplicateRequestStorm|.*|firing|' '$EV'"

# ---------------------------------------------------------------------------------------------
log "8) scenario C (duplicate storm), both apps in parallel. Waiting for scenario B rates to settle is not needed (different alert)."
for p in 8090 8091; do pay $p "storm-$p" "order-storm-$p" >/dev/null; done   # the one real charge
sleep 6
ka0="$(metric localhost:8090/actuator/prometheus '^payment_requests_total.*result="approved"')"; kc0="$(metric localhost:8090/actuator/prometheus '^pg_call_duration_seconds_count')"
ga0="$(metric localhost:8091/metrics '^payment_requests_total.*result="approved"')";              gc0="$(metric localhost:8091/metrics '^pg_call_duration_seconds_count')"
: >"$EV"; T0=$(date +%s)
readme_storm() { local i; for i in $(seq 1 500); do pay "$1" "storm-$1" "order-storm-$1" >/dev/null; done; }
log "8a) README literal loop: 500 sequential requests per app"
readme_storm 8090 & p1=$!; readme_storm 8091 & p2=$!; wait $p1 $p2
DUR=$(( $(date +%s) - T0 )); log "500 requests took ${DUR}s (alert needs rate>5/s sustained for 2m)"
end=$(( $(date +%s) + 150 )); while (( $(date +%s) < end )); do poll_alerts $T0 DuplicateRequestStorm; sleep 5; done
for j in $KJOB $GJOB; do
  log "README storm, $j: pending=T0+$(t_of DuplicateRequestStorm $j pending)s firing=T0+$(t_of DuplicateRequestStorm $j firing)s"
  RESULTS+=("INFO  README 500-loop ($DUR s) $j DuplicateRequestStorm: pending='$(t_of DuplicateRequestStorm $j pending)' firing='$(t_of DuplicateRequestStorm $j firing)'")
done
log "8b) sustained storm (same key, until DuplicateRequestStorm fires for both jobs, max 6 min)"
: >"$EV"; T0=$(date +%s); rm -f "$STOP"
storm_loop() { while [[ ! -f "$STOP" ]]; do pay "$1" "storm-$1" "order-storm-$1" >/dev/null; sleep 0.05; done; }
bg storm_loop 8090; bg storm_loop 8091
while :; do
  poll_alerts $T0 DuplicateRequestStorm
  fired DuplicateRequestStorm $KJOB && fired DuplicateRequestStorm $GJOB && break
  (( $(date +%s) - T0 > 360 )) && break
  sleep 5
done
touch "$STOP"; sleep 1
for j in $KJOB $GJOB; do
  check "$j: DuplicateRequestStorm pending (T0+$(t_of DuplicateRequestStorm $j pending)s) then firing (T0+$(t_of DuplicateRequestStorm $j firing)s)" \
    test -n "$(t_of DuplicateRequestStorm $j pending)" -a -n "$(t_of DuplicateRequestStorm $j firing)"
done
check_eq "kotlin: no double charge during storm (approved delta)" 0 $(( $(metric localhost:8090/actuator/prometheus '^payment_requests_total.*result="approved"') - ka0 ))
check_eq "kotlin: no extra PG call during storm" 0 $(( $(metric localhost:8090/actuator/prometheus '^pg_call_duration_seconds_count') - kc0 ))
check_eq "go: no double charge during storm (approved delta)" 0 $(( $(metric localhost:8091/metrics '^payment_requests_total.*result="approved"') - ga0 ))
check_eq "go: no extra PG call during storm" 0 $(( $(metric localhost:8091/metrics '^pg_call_duration_seconds_count') - gc0 ))
check_eq "kotlin: one payments row for the storm order" 1 "$(psql_q e2e_k10 "SELECT count(*) FROM k10.payments WHERE order_id='order-storm-8090'")"
check_eq "go: one payments row for the storm order" 1 "$(psql_q e2e_g10 "SELECT count(*) FROM go_10.payments WHERE order_id='order-storm-8091'")"
MISSED="$(curl -s "$PROM/metrics" | awk '/^prometheus_rule_group_iterations_missed_total/{print $NF}')"
RESULTS+=("INFO  Prometheus missed rule-group iterations so far: ${MISSED:-?} (non-zero shifts the timeline by 1 eval interval)")
fi

echo
echo "==== 10-observability$([[ $QUICK == 1 ]] && echo ' --quick'): $PASS passed, $FAIL failed   (logs: $LOG_DIR)"
for r in "${RESULTS[@]}"; do echo "  $r"; done
if [[ $FAIL -gt 0 ]]; then echo "RESULT: FAIL"; exit 1; fi
echo "RESULT: PASS"
