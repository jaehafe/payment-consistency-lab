#!/usr/bin/env bash
# 05 idempotent consumer: the Kotlin app runs ConsumerRunner (background poll loop) when started.
#
#   A. rpk-produced events incl. a duplicate message_id -> points credited once per message_id,
#      processed_messages has exactly one row per message_id, orders flip to PAID
#   B. restart the consumer: committed offsets mean nothing is re-processed; a duplicate sent after the
#      restart is skipped, a new message is processed
#   D. dead letter topic: a poison payload (not JSON) and an event without a message_id header, both
#      on the same key/partition as a later good message -> both land on <topic>-dlt with the
#      kafka_dlt-* headers (poison after 3 attempts, missing message_id after 1), the later message is
#      still processed and the group lag returns to 0
#   C. if 03's topic (e2e.k03.events) exists, a second consumer group drains it into its own DB:
#      one processed_messages row and one point_ledger row per distinct message_id
# Go 05 is a library package (no cmd) -> skipped.
source "$(dirname "$0")/lib.sh"

DB=e2e_k05
TOPIC=e2e.k05.events
GROUP=e2e-k05
DLT="$TOPIC-dlt"              # Spring Kafka 3.3+ default naming, see kotlin/05 README
K03_TOPIC=e2e.k03.events
DB03=e2e_k05_from03

# produce MESSAGE_ID ORDER_ID AMOUNT
produce() {
  local ev
  ev="{\"paymentId\":\"p-$2\",\"orderId\":\"$2\",\"amount\":$3,\"pgTxId\":\"pg-$2\",\"occurredAt\":\"2026-11-08T00:00:00Z\"}"
  echo "$ev" | rpk topic produce "$TOPIC" -k "$2" -H "message_id:$1" -H "event_type:PaymentCompleted" >/dev/null
}

processed_count() { psql_q "$DB" "SELECT count(*) FROM processed_messages WHERE consumer_group='$GROUP'"; }
processed_in() { [[ "$(psql_q "$1" "SELECT count(*) FROM processed_messages")" == "$2" ]]; }   # DB03 is e2e_k05_from03 -> schema k05
wait_processed() { [[ "$(psql_q "$DB" "SELECT count(*) FROM processed_messages")" == "$1" ]]; }

ensure_infra
reset_db "$DB"
reset_topic "$TOPIC" 3
reset_topic "$DLT" 3
KARGS=(--spring.datasource.url="$(jdbc_url $DB)" --lab.topic="$TOPIC" --lab.consumer.group-id="$GROUP")

# schema first (the app applies schema.sql on start), then seed orders so the PAID update is visible
start_kotlin 05-idempotent-consumer k05-1 "${KARGS[@]}"; C1=$LAST_PID
wait_log "$E2E_LOG_DIR/k05-1.log" "Started ConsumerApplication" 90 || die "Kotlin 05 did not start"
psql_q "$DB" "INSERT INTO orders (id, status) SELECT 'o-' || g, 'PENDING' FROM generate_series(1, 7) g" >/dev/null

# ---- A ----
log "producing m1..m5 and a duplicate of m3"
for i in 1 2 3 4 5; do produce "m$i" "o-$i" 10000; done
produce m3 o-3 10000            # at-least-once duplicate: same message_id, same payload
check_eq "[A] records on topic (5 + 1 duplicate)" 6 "$(topic_count "$TOPIC")"
check "[A] consumer processed 5 distinct messages within 30 s" wait_for 30 wait_processed 5
sleep 3   # give the duplicate time to arrive and be skipped
check_eq "[A] processed_messages rows" 5 "$(processed_count)"
check_eq "[A] processed_messages: one row per message_id" 0 "$(psql_q $DB "SELECT count(*) FROM (SELECT message_id FROM processed_messages GROUP BY 1 HAVING count(*) > 1) d")"
check_eq "[A] point_ledger rows (o-3 credited once despite duplicate)" 5 "$(psql_q $DB 'SELECT count(*) FROM point_ledger')"
check_eq "[A] points for o-3" 100 "$(psql_q $DB "SELECT sum(points) FROM point_ledger WHERE order_id='o-3'")"
check_eq "[A] orders PAID" 5 "$(psql_q $DB "SELECT count(*) FROM orders WHERE status='PAID' AND paid_amount=10000")"
LAG="$(rpk group describe "$GROUP" 2>/dev/null | awk '/^TOTAL-LAG/ {print $2}')"
check_eq "[A] consumer group lag after processing (offsets committed)" 0 "${LAG:-?}"

# ---- B ----
log "restarting the consumer"
kill "$C1"; wait "$C1" 2>/dev/null || true
start_kotlin 05-idempotent-consumer k05-2 "${KARGS[@]}"
wait_log "$E2E_LOG_DIR/k05-2.log" "Started ConsumerApplication" 90 || die "Kotlin 05 did not restart"
sleep 5
check_eq "[B] restart re-processed nothing" 5 "$(processed_count)"
produce m1 o-1 10000            # late duplicate
produce m6 o-6 10000            # new message
check "[B] new message m6 processed" wait_for 30 wait_processed 6
sleep 3
check_eq "[B] point_ledger rows (late duplicate m1 skipped)" 6 "$(psql_q $DB 'SELECT count(*) FROM point_ledger')"
check_eq "[B] total points" 600 "$(psql_q $DB 'SELECT sum(points) FROM point_ledger')"
check_eq "[B] points for o-1" 100 "$(psql_q $DB "SELECT sum(points) FROM point_ledger WHERE order_id='o-1'")"
check "[B] no processing errors in consumer logs" bash -c "! grep -q '메시지 처리 실패' '$E2E_LOG_DIR'/k05-*.log"

# ---- D: poison message + missing message_id -> DLT, later messages on the same partition still flow ----
log "producing to key o-7: a poison payload (m-poison), an event without message_id, then m7"
echo 'not-json' | rpk topic produce "$TOPIC" -k o-7 -H "message_id:m-poison" >/dev/null
echo '{"paymentId":"p-o-7","orderId":"o-7","amount":10000,"pgTxId":"pg-o-7","occurredAt":"2026-11-08T00:00:00Z"}' \
  | rpk topic produce "$TOPIC" -k o-7 -H "event_type:PaymentCompleted" >/dev/null
produce m7 o-7 10000
dlt_has() { [[ "$(topic_count "$DLT")" == "$1" ]]; }
processed_has() { [[ "$(psql_q "$DB" "SELECT count(*) FROM processed_messages WHERE message_id='$1'")" == 1 ]]; }
check "[D] two records on $DLT within 60 s" wait_for 60 dlt_has 2
check "[D] m7 (after both bad records, same partition) processed" wait_for 30 processed_has m7
sleep 2
check_eq "[D] still exactly two records on $DLT" 2 "$(topic_count "$DLT")"

# rpk prints one JSON object per record; jq -s collects them (header values may contain newlines)
DLT_JSON="$(rpk topic consume "$DLT" -o :end | jq -cs '.')"
SRC_JSON="$(rpk topic consume "$TOPIC" -o :end | jq -cs '.')"
# pick SELECTOR JSON -> the first record matching (poison: message_id=m-poison, missing: no message_id)
pick() {
  local sel
  case "$1" in
    poison)  sel='any(.headers[]; .key == "message_id" and .value == "m-poison")' ;;
    missing) sel='(.key == "o-7") and all(.headers[]; .key != "message_id")' ;;
  esac
  jq -c "[.[] | select($sel)][0]" <<<"$2"
}
# dlt_h SELECTOR HEADER -> header value on the DLT record
dlt_h() { pick "$1" "$DLT_JSON" | jq -r --arg h "$2" '[.headers[] | select(.key == $h) | .value] | last // ""'; }

check_eq "[D] poison: key kept" o-7 "$(pick poison "$DLT_JSON" | jq -r .key)"
check_eq "[D] poison: value kept" not-json "$(pick poison "$DLT_JSON" | jq -r .value)"
check_eq "[D] poison: original message_id header kept" m-poison "$(dlt_h poison message_id)"
check_eq "[D] poison: kafka_dlt-original-topic" "$TOPIC" "$(dlt_h poison kafka_dlt-original-topic)"
check_eq "[D] poison: kafka_dlt-original-partition" "$(pick poison "$SRC_JSON" | jq -r .partition)" "$(dlt_h poison kafka_dlt-original-partition)"
check_eq "[D] poison: kafka_dlt-original-offset" "$(pick poison "$SRC_JSON" | jq -r .offset)" "$(dlt_h poison kafka_dlt-original-offset)"
check_eq "[D] poison: kafka_dlt-original-consumer-group" "$GROUP" "$(dlt_h poison kafka_dlt-original-consumer-group)"
check_eq "[D] poison: kafka_dlt-exception-fqcn" com.fasterxml.jackson.core.JsonParseException "$(dlt_h poison kafka_dlt-exception-fqcn)"
check "[D] poison: kafka_dlt-exception-message mentions the bad token" grep -q "Unrecognized token 'not'" <<<"$(dlt_h poison kafka_dlt-exception-message)"
check_eq "[D] poison: kafka_dlt-attempts (max-attempts)" 3 "$(dlt_h poison kafka_dlt-attempts)"
check_eq "[D] missing: kafka_dlt-exception-fqcn" lab.consumer.MissingMessageIdException "$(dlt_h missing kafka_dlt-exception-fqcn)"
check_eq "[D] missing: kafka_dlt-attempts (not retryable)" 1 "$(dlt_h missing kafka_dlt-attempts)"
check_eq "[D] missing: kafka_dlt-original-offset" "$(pick missing "$SRC_JSON" | jq -r .offset)" "$(dlt_h missing kafka_dlt-original-offset)"
check_eq "[D] points for o-7 (m7 only)" 100 "$(psql_q $DB "SELECT sum(points) FROM point_ledger WHERE order_id='o-7'")"
check_eq "[D] processed_messages rows (m1..m7)" 7 "$(processed_count)"
LAG="$(rpk group describe "$GROUP" 2>/dev/null | awk '/^TOTAL-LAG/ {print $2}')"
check_eq "[D] consumer group lag (offsets committed past the dead-lettered records)" 0 "${LAG:-?}"
check_eq "[D] 'DLT로 보냄' log lines" 2 "$(grep -c 'DLT로 보냄' "$E2E_LOG_DIR/k05-2.log")"

# ---- C: events produced by stage 03 ----
N03="$(topic_count "$K03_TOPIC" 2>/dev/null || echo 0)"
if [[ "${N03:-0}" -gt 0 ]]; then
  log "draining $N03 records of $K03_TOPIC (stage 03 output) with a fresh consumer group"
  reset_db "$DB03"
  start_kotlin 05-idempotent-consumer k05-from03 --spring.datasource.url="$(jdbc_url $DB03)" \
    --lab.topic="$K03_TOPIC" --lab.consumer.group-id="e2e-k05-from03-$$"
  wait_log "$E2E_LOG_DIR/k05-from03.log" "Started ConsumerApplication" 90 || die "Kotlin 05 (03 feed) did not start"
  DISTINCT03="$(header_values "$K03_TOPIC" message_id | sort -u | wc -l | tr -d ' ')"
  check "[C] all $DISTINCT03 distinct message_ids from 03 processed within 60 s" \
    wait_for 60 processed_in "$DB03" "$DISTINCT03"
  sleep 2
  check_eq "[C] processed_messages rows = distinct message_ids" "$DISTINCT03" "$(psql_q $DB03 'SELECT count(*) FROM processed_messages')"
  check_eq "[C] point_ledger rows = distinct message_ids" "$DISTINCT03" "$(psql_q $DB03 'SELECT count(*) FROM point_ledger')"
else
  skip "[C] $K03_TOPIC not found or empty - run e2e/03-outbox-polling.sh first to feed 05 with real outbox events"
fi
skip "Go 05: library package only (no cmd) - behaviour covered by go test ./05-idempotent-consumer/"

summary "05-idempotent-consumer"
