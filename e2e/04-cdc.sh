#!/usr/bin/env bash
# 04-outbox-cdc end to end, Kotlin + Go, against the shared compose stack (profile `cdc`).
#
#   e2e/04-cdc.sh
#
# What it proves:
#   1. Kotlin app (demo profile) writes payments + outbox in one tx; Debezium's Outbox Event Router
#      publishes it to `payment.events` with key=orderId, headers event_type/message_id(=outbox.id),
#      value = the payload JSON itself (expanded, no Debezium envelope).
#   2. Go cmd does the same and itself reads the message back from the topic.
#   3. Connect down -> payment committed -> nothing on the topic, slot inactive and WAL lag grows;
#      Connect back up -> the event is delivered late from the replication slot, exactly once.
#
# Uses DB `lab` / `public.outbox` (the connector config is fixed to it). Never runs `down`;
# it only `stop`s/`start`s the `connect` service, and the trap restarts it if the script dies mid-test.
# Helpers are inline on purpose (independent of e2e/lib.sh).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$REPO/infra/docker-compose.yml"
TOPIC=payment.events
CONNECTOR=payment-outbox-connector
LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pcl-e2e-04.XXXXXX")"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
log() { printf '\033[36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { printf '\033[31mFATAL:\033[0m %s\n' "$*" >&2; exit 1; }
psql_q() { compose exec -T postgres psql -U lab -d lab -v ON_ERROR_STOP=1 -qtAX -c "$1"; }
rpk() { compose exec -T redpanda rpk "$@"; }

PASS=0; FAIL=0; RESULTS=()
check() { # check "desc" cmd...
  local d="$1"; shift
  if "$@"; then PASS=$((PASS+1)); RESULTS+=("PASS  $d"); printf '  \033[32mPASS\033[0m %s\n' "$d" >&2
  else FAIL=$((FAIL+1)); RESULTS+=("FAIL  $d"); printf '  \033[31mFAIL\033[0m %s\n' "$d" >&2; fi
}
check_eq() { local d="$1" e="$2" a="$3"; if [[ "$e" == "$a" ]]; then check "$d (= $a)" true; else check "$d (expected '$e', got '$a')" false; fi; }

CONNECT_STOPPED=0
cleanup() {
  if [[ $CONNECT_STOPPED == 1 ]]; then log "trap: restarting connect"; compose start connect >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT INT TERM

# records_for_key KEY -> every record of the topic with that key, as JSON lines (rpk default format)
records_for_key() { rpk topic consume "$TOPIC" -o :end 2>/dev/null | jq -c --arg k "$1" 'select(.key==$k)'; }
topic_count() { rpk topic describe "$TOPIC" -p 2>/dev/null | awk 'NR>1 && $NF ~ /^[0-9]+$/ {s+=$NF} END {print s+0}'; }
wait_key() { # wait_key KEY TIMEOUT -> 0 once at least one record with KEY exists
  local i; for i in $(seq 1 "$2"); do [[ -n "$(records_for_key "$1")" ]] && return 0; sleep 1; done; return 1
}

# verify_record ORDER_ID LABEL -> asserts key/headers/value shape against the outbox row
verify_record() {
  local order="$1" label="$2" recs rec outbox_id payment_id
  recs="$(records_for_key "$order")"
  check_eq "$label: exactly one record with key=$order" 1 "$(grep -c . <<<"$recs" || true)"
  rec="$(head -1 <<<"$recs")"
  outbox_id="$(psql_q "SELECT id FROM outbox WHERE aggregateid='$order'")"
  payment_id="$(psql_q "SELECT id FROM payments WHERE order_id='$order'")"
  check "$label: outbox row exists (aggregatetype=payment, type=PaymentCompleted)" \
    test "$(psql_q "SELECT aggregatetype||'/'||type FROM outbox WHERE aggregateid='$order'")" == "payment/PaymentCompleted"
  check_eq "$label: header event_type" PaymentCompleted "$(jq -r '.headers[]|select(.key=="event_type").value' <<<"$rec")"
  check_eq "$label: header message_id == outbox.id" "$outbox_id" "$(jq -r '.headers[]|select(.key=="message_id").value' <<<"$rec")"
  local v; v="$(jq -r .value <<<"$rec")"
  check "$label: value is the expanded payload JSON object (no Debezium envelope)" \
    jq -e 'type=="object" and (has("payload")|not) and (has("schema")|not) and has("occurredAt")' <<<"$v" >/dev/null
  check_eq "$label: value.paymentId == payments.id" "$payment_id" "$(jq -r .paymentId <<<"$v")"
  check_eq "$label: value.orderId" "$order" "$(jq -r .orderId <<<"$v")"
  check_eq "$label: value.amount" 10000 "$(jq -r .amount <<<"$v")"
}

# kotlin_demo -> runs the Kotlin app with the demo profile (one payment, then exits), prints the orderId
kotlin_demo() {
  local out="$LOG_DIR/kotlin-demo-$RANDOM.log"
  java -jar "$JAR" --spring.profiles.active=demo --spring.main.banner-mode=off >"$out" 2>&1 \
    || { cat "$out" >&2; die "kotlin demo failed (log $out)"; }
  grep -o 'orderId=order-[0-9a-f-]*' "$out" | head -1 | cut -d= -f2
}

# ---------------------------------------------------------------------------------------------
log "1) infra: postgres + redpanda + connect (profile cdc)"
compose --profile cdc up -d postgres redpanda connect >/dev/null 2>&1 || die "compose up failed"
for i in $(seq 1 90); do curl -sf localhost:8083/connectors >/dev/null 2>&1 && break; sleep 2; done
curl -sf localhost:8083/connectors >/dev/null || die "Kafka Connect not ready on :8083"
check_eq "postgres wal_level" logical "$(psql_q 'SHOW wal_level')"

# An old Kotlin 03 may have left a `public.outbox` without Debezium columns (see 04 README).
if [[ "$(psql_q "SELECT to_regclass('public.outbox') IS NOT NULL")" == t ]] \
   && [[ "$(psql_q "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='outbox' AND column_name='aggregatetype'")" == 0 ]]; then
  die "public.outbox exists without 'aggregatetype' (old 03 table). DROP it first, see kotlin/04-outbox-cdc/README.md"
fi

log "2) Kotlin: build jar, run once with demo profile (creates tables, README step 2)"
JAR="$(ls "$REPO"/kotlin/04-outbox-cdc/build/libs/*.jar 2>/dev/null | head -1 || true)"
if [[ -z "$JAR" ]]; then (cd "$REPO/kotlin" && ./gradlew -q :04-outbox-cdc:bootJar) >&2; JAR="$(ls "$REPO"/kotlin/04-outbox-cdc/build/libs/*.jar | head -1)"; fi
K1="$(kotlin_demo)"; log "kotlin payment orderId=$K1"
check "kotlin demo printed an orderId" test -n "$K1"

log "3) register connector (infra/debezium/register.sh) unless it already exists"
if curl -sf "localhost:8083/connectors/$CONNECTOR" >/dev/null; then
  log "connector already registered"
else
  "$REPO/infra/debezium/register.sh" >"$LOG_DIR/register.json"
fi
for i in $(seq 1 30); do
  st="$(curl -s "localhost:8083/connectors/$CONNECTOR/status" | jq -r '[.connector.state, (.tasks[0].state // "none")]|join("/")')"
  [[ "$st" == RUNNING/RUNNING ]] && break; sleep 1
done
check_eq "connector + task state" RUNNING/RUNNING "$st"

log "4) Kotlin event on $TOPIC (snapshot or streaming, depending on whether the connector is new)"
check "kotlin event arrived within 30s" wait_key "$K1" 30
verify_record "$K1" kotlin

log "5) second Kotlin payment while connector is running -> streamed (latency measured)"
t0=$(date +%s); K2="$(kotlin_demo)"
check "kotlin streamed event arrived within 15s" wait_key "$K2" 15
log "kotlin streaming latency (incl. ~1s jvm exit + poll granularity): $(( $(date +%s) - t0 ))s"
verify_record "$K2" kotlin-streamed

log "6) Go: go run ./04-outbox-cdc/cmd (README step 3) - it reads its own message back"
GO_OUT="$LOG_DIR/go-demo.log"
if (cd "$REPO/go" && go run ./04-outbox-cdc/cmd) >"$GO_OUT" 2>&1; then go_rc=0; else go_rc=$?; fi
check_eq "go cmd exit code (0 = it saw its own event)" 0 "$go_rc"
G1="$(grep -o 'orderId=order-[0-9]*' "$GO_OUT" | head -1 | cut -d= -f2)"
log "go payment orderId=$G1"
check "go cmd printed the Debezium message for its key" grep -q "Debezium 이 보낸 메시지: key=$G1 .*event_type=PaymentCompleted" "$GO_OUT"
verify_record "$G1" go

log "7) failure case: stop connect -> pay -> start connect -> late delivery from the slot"
compose stop connect >/dev/null 2>&1; CONNECT_STOPPED=1
before="$(topic_count)"
lag_before="$(psql_q "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name='debezium'")"
K3="$(kotlin_demo)"; log "payment while connect is down: orderId=$K3"
sleep 3
check_eq "nothing published while connect is down (topic record count unchanged)" "$before" "$(topic_count)"
check_eq "replication slot 'debezium' inactive" f "$(psql_q "SELECT active FROM pg_replication_slots WHERE slot_name='debezium'")"
lag_after="$(psql_q "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name='debezium'")"
check "slot retains WAL: unconfirmed lag grew ($lag_before -> $lag_after bytes)" test "$lag_after" -gt "$lag_before"
check_eq "outbox row committed anyway" 1 "$(psql_q "SELECT count(*) FROM outbox WHERE aggregateid='$K3'")"
t0=$(date +%s)
compose start connect >/dev/null 2>&1; CONNECT_STOPPED=0
check "late event delivered after connect restart (<=120s)" wait_key "$K3" 120
log "late delivery took $(( $(date +%s) - t0 ))s after 'compose start connect'"
verify_record "$K3" late
check_eq "slot active again" t "$(psql_q "SELECT active FROM pg_replication_slots WHERE slot_name='debezium'")"
# CDC is at-least-once: a re-delivery after restart is allowed (README), so this is informational only.
for k in "$K1" "$K2" "$G1"; do
  n="$(records_for_key "$k" | grep -c . || true)"
  [[ "$n" == 1 ]] && log "info: $k still delivered once after restart" \
                  || log "info: $k now has $n copies (at-least-once re-delivery after restart)"
done

echo
echo "==== 04-cdc: $PASS passed, $FAIL failed   (logs: $LOG_DIR)"
for r in "${RESULTS[@]}"; do echo "  $r"; done
if [[ $FAIL -gt 0 ]]; then echo "RESULT: FAIL"; exit 1; fi
echo "RESULT: PASS"
