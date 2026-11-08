#!/usr/bin/env bash
# 03 transactional outbox + polling relay, against the real Redpanda.
#
# Kotlin (no HTTP endpoint in this module -> payments are created with the same single-transaction
# INSERT payments + INSERT outbox_polling that PaymentService.pay() does):
#   A. one relay: rows get published, message_id = outbox.id, event_type = PaymentCompleted, published_at set
#   B. broker unreachable (this instance only - points at a dead port, the shared Redpanda is untouched):
#      payments still commit, outbox rows stay unpublished, relay logs a warning
#   C. two relay processes at once on the same DB: the backlog from B is delivered after "recovery",
#      then N more rows -> topic holds every outbox id exactly once (no duplicate message_id)
# Go (cmd = 20 s demo: 3 payments, relay goroutine, consumer):
#   D. two cmd processes at once on the same DB + a pre-seeded backlog -> every outbox row published once
source "$(dirname "$0")/lib.sh"

DB=e2e_k03
TOPIC=e2e.k03.events
N="${N:-300}"
GDB=e2e_g03
GTOPIC=payment.events.go03   # fixed in go/03-outbox-polling/cmd/main.go (not configurable)

# insert_payments DB TABLE PREFIX COUNT  - payments + outbox rows in ONE transaction, like PaymentService.pay()
insert_payments() {
  local db="$1" table="$2" prefix="$3" n="$4"
  psql_q "$db" "
    BEGIN;
    WITH p AS (
      INSERT INTO payments (id, order_id, amount, status, pg_tx_id)
      SELECT gen_random_uuid(), '$prefix-' || g, 1000 * g, 'COMPLETED', 'pg_' || gen_random_uuid()
      FROM generate_series(1, $n) g
      RETURNING id, order_id, amount, pg_tx_id
    )
    INSERT INTO $table (id, aggregate_type, aggregate_id, event_type, payload)
    SELECT gen_random_uuid(), 'payment', order_id, 'PaymentCompleted',
           jsonb_build_object('paymentId', id::text, 'orderId', order_id, 'amount', amount, 'pgTxId', pg_tx_id,
                              'occurredAt', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))
    FROM p;
    COMMIT;" >/dev/null
}

unpublished() { psql_q "$1" "SELECT count(*) FROM $2 WHERE published_at IS NULL"; }
all_published() { [[ "$(unpublished "$1" "$2")" == 0 ]]; }

# compare_topic_with_outbox LABEL DB TABLE TOPIC
compare_topic_with_outbox() {
  local label="$1" db="$2" table="$3" topic="$4"
  local ids_db="$E2E_LOG_DIR/$label.outbox_ids" ids_topic="$E2E_LOG_DIR/$label.topic_ids"
  psql_q "$db" "SELECT id FROM $table ORDER BY 1" >"$ids_db"
  header_values "$topic" message_id | sort >"$ids_topic"
  local n_db n_topic n_distinct
  n_db="$(wc -l <"$ids_db" | tr -d ' ')"
  n_topic="$(wc -l <"$ids_topic" | tr -d ' ')"
  n_distinct="$(sort -u "$ids_topic" | wc -l | tr -d ' ')"
  check_eq "[$label] records on topic = outbox rows" "$n_db" "$n_topic"
  check_eq "[$label] distinct message_id on topic = records (no duplicates)" "$n_topic" "$n_distinct"
  check "[$label] set of message_ids on topic == set of outbox ids" diff -q "$ids_db" <(sort -u "$ids_topic")
  local dup; dup="$(uniq -d "$ids_topic" | head -3 | tr '\n' ' ')"
  [[ -z "$dup" ]] || echo "    duplicated message_ids: $dup" >&2
}

ensure_infra

# =============================== Kotlin =========================================================
reset_db "$DB"
reset_topic "$TOPIC" 3
KARGS=(--spring.datasource.url="$(jdbc_url $DB)" --lab.topic="$TOPIC"
       --logging.level.org.springframework.kafka.core.KafkaTemplate=TRACE)   # TRACE -> one "Sent ok:" line per record

# ---- A: one relay ----
start_kotlin 03-outbox-polling k03-a1 "${KARGS[@]}"; A1=$LAST_PID
wait_log "$E2E_LOG_DIR/k03-a1.log" "Started OutboxPollingApplication" 90 || die "Kotlin 03 did not start"
insert_payments "$DB" outbox_polling a 5
check "[kotlin A] relay published 5 rows within 15 s (published_at set)" wait_for 15 all_published "$DB" outbox_polling
LINE="$(dump_topic "$TOPIC" | head -1)"; echo "    sample record: $LINE" >&2
check_eq "[kotlin A] records on topic" 5 "$(topic_count "$TOPIC")"
check_eq "[kotlin A] every record has event_type=PaymentCompleted" 5 "$(header_values "$TOPIC" event_type | grep -c '^PaymentCompleted$')"
check_eq "[kotlin A] published_at set on all rows" 0 "$(unpublished "$DB" outbox_polling)"
compare_topic_with_outbox "kotlin A" "$DB" outbox_polling "$TOPIC"
kill "$A1"; wait "$A1" 2>/dev/null || true

# ---- B: broker unreachable for this instance only ----
start_kotlin 03-outbox-polling k03-down "${KARGS[@]}" --spring.kafka.bootstrap-servers=localhost:18099 \
  --spring.kafka.producer.properties.max.block.ms=2000 --spring.kafka.admin.properties.default.api.timeout.ms=3000 \
  --spring.kafka.admin.properties.request.timeout.ms=2000
DOWN=$LAST_PID
wait_log "$E2E_LOG_DIR/k03-down.log" "Started OutboxPollingApplication" 90 || die "Kotlin 03 (dead broker) did not start"
insert_payments "$DB" outbox_polling down 3
check "[kotlin B] relay logs a failure while the broker is unreachable" wait_log "$E2E_LOG_DIR/k03-down.log" "outbox relay 실패" 20
check_eq "[kotlin B] payments committed although the broker is down" 3 "$(psql_q $DB "SELECT count(*) FROM payments WHERE order_id LIKE 'down-%'")"
check_eq "[kotlin B] their outbox rows stay unpublished" 3 "$(unpublished "$DB" outbox_polling)"
check_eq "[kotlin B] nothing new on the topic" 5 "$(topic_count "$TOPIC")"
kill "$DOWN"; wait "$DOWN" 2>/dev/null || true

# ---- C: two relay processes at once ----
FAST=(--lab.outbox.relay.batch-size=10 --lab.outbox.relay.interval-ms=100)   # small batches -> both relays get work
start_kotlin 03-outbox-polling k03-r1 "${KARGS[@]}" "${FAST[@]}"
start_kotlin 03-outbox-polling k03-r2 "${KARGS[@]}" "${FAST[@]}"
wait_log "$E2E_LOG_DIR/k03-r1.log" "Started OutboxPollingApplication" 90 || die "relay 1 did not start"
wait_log "$E2E_LOG_DIR/k03-r2.log" "Started OutboxPollingApplication" 90 || die "relay 2 did not start"
check "[kotlin C] backlog from the broker outage delivered after recovery" wait_for 20 all_published "$DB" outbox_polling
insert_payments "$DB" outbox_polling c "$N"
check "[kotlin C] $N more rows published by the two relays within 60 s" wait_for 60 all_published "$DB" outbox_polling
sleep 1
S1="$(grep -c 'Sent ok:' "$E2E_LOG_DIR/k03-r1.log" || true)"; S2="$(grep -c 'Sent ok:' "$E2E_LOG_DIR/k03-r2.log" || true)"
echo "    relay 1 sent $S1, relay 2 sent $S2 (sum $((S1 + S2)), expected $((N + 3)))" >&2
check_eq "[kotlin C] sends across both relays = new rows (nothing sent twice)" $((N + 3)) $((S1 + S2))
if [[ "$S1" -gt 0 && "$S2" -gt 0 ]]; then check "[kotlin C] both relays did work (SKIP LOCKED let them share)" true
else skip "[kotlin C] only one relay got rows this run ($S1/$S2) - not a correctness failure"; fi
compare_topic_with_outbox "kotlin C" "$DB" outbox_polling "$TOPIC"

# =============================== Go =============================================================
reset_db "$GDB"
reset_topic "$GTOPIC" 1
psql_q "$GDB" "CREATE SCHEMA go_03" >/dev/null
psql_f "$GDB" -q -c "SET search_path=go_03" -f - <"$REPO/go/03-outbox-polling/schema.sql" >/dev/null
GB="${GO_BACKLOG:-50}"
compose exec -T postgres psql -U lab -d "$GDB" -qtAX -v ON_ERROR_STOP=1 >/dev/null <<SQL
SET search_path = go_03;
BEGIN;
WITH p AS (
  INSERT INTO payments (id, order_id, amount, status, pg_tx_id)
  SELECT gen_random_uuid(), 'seed-' || g, 1000 * g, 'COMPLETED', 'pg_' || gen_random_uuid() FROM generate_series(1, $GB) g
  RETURNING id, order_id, amount, pg_tx_id)
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
SELECT gen_random_uuid(), 'payment', order_id, 'PaymentCompleted',
       jsonb_build_object('paymentId', id::text, 'orderId', order_id, 'amount', amount, 'pgTxId', pg_tx_id)
FROM p;
COMMIT;
SQL
GBIN="$(go_bin 03-outbox-polling g03)"
start_bg g03-1 env DATABASE_URL="$(pg_dsn $GDB)" KAFKA_BROKERS="$KAFKA_BROKERS" "$GBIN"; G1=$LAST_PID
start_bg g03-2 env DATABASE_URL="$(pg_dsn $GDB)" KAFKA_BROKERS="$KAFKA_BROKERS" "$GBIN"; G2=$LAST_PID
rc1=0; wait "$G1" || rc1=$?
rc2=0; wait "$G2" || rc2=$?
check_eq "[go D] cmd instance 1 exit code" 0 "$rc1"
check_eq "[go D] cmd instance 2 exit code" 0 "$rc2"
check_eq "[go D] each cmd saw its own 3 records on the topic" 6 "$(cat "$E2E_LOG_DIR"/g03-*.log | grep -c '수신 key=')"
check_eq "[go D] payments (backlog $GB + 2 x 3)" $((GB + 6)) "$(psql_q $GDB 'SELECT count(*) FROM go_03.payments')"
check_eq "[go D] unpublished outbox rows left" 0 "$(unpublished "$GDB" go_03.outbox)"
check_eq "[go D] every record has event_type=PaymentCompleted" $((GB + 6)) "$(header_values "$GTOPIC" event_type | grep -c '^PaymentCompleted$')"
compare_topic_with_outbox "go D" "$GDB" go_03.outbox "$GTOPIC"
skip "broker-down against the shared Redpanda (would disrupt Agent A's Debezium) - Kotlin B uses an unreachable bootstrap address instead; Go cmd has no such knob beyond KAFKA_BROKERS"

summary "03-outbox-polling"
