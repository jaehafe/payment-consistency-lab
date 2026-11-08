#!/usr/bin/env bash
# 07 payment state machine: does the janitor run on a schedule inside the app?
#
# Kotlin: JanitorScheduler (@Scheduled fixedDelay, lab.janitor.enabled / lab.janitor.interval).
# Go:     cmd/main.go -> Janitor.Run(ctx, JANITOR_INTERVAL) (fixed delay via time.After).
#
# The fake PG lives in the app's memory and this script cannot charge through it, so a row
# inserted from outside is NOT_FOUND at the PG. That still shows both janitor branches:
#   o-old   (created 1h ago, past not-found-grace) -> FAILED by the scheduled run
#   o-fresh (created now, within grace)            -> stays UNKNOWN, attempt_count back to 0,
#                                                     next_retry_at = created_at + 5m
# and the PENDING sweep (a payment that crashed after REQUESTED -> PENDING):
#   p-old   (PENDING, created 1h ago: older than pending-after 2m, past grace)
#                                                  -> PENDING -> FAILED by the scheduled run
#   p-fresh (PENDING, created now: younger than pending-after) -> untouched
# The -> APPROVED paths on a schedule are covered by the module tests
# (JanitorSchedulerTest / Test_run__reconciles_due_unknown_on_schedule /
#  Test_run__settles_stale_pending_on_schedule).
source "$(dirname "$0")/lib.sh"
ensure_infra

ROWS="INSERT INTO payments (id, order_id, amount, status, idempotency_key, next_retry_at, created_at) VALUES
  (gen_random_uuid(), 'o-old',   10000, 'UNKNOWN', 'idem-old',   now() - interval '10 minutes', now() - interval '1 hour'),
  (gen_random_uuid(), 'o-fresh', 10000, 'UNKNOWN', 'idem-fresh', now() - interval '1 second',   now()),
  (gen_random_uuid(), 'p-old',   10000, 'PENDING', 'idem-p-old',   NULL, now() - interval '1 hour'),
  (gen_random_uuid(), 'p-fresh', 10000, 'PENDING', 'idem-p-fresh', NULL, now())"
ROW_STATE="SELECT order_id || '|' || status || '|' || attempt_count || '|' || coalesce(to_char(next_retry_at - created_at, 'HH24:MI:SS'), '-')
             FROM payments ORDER BY order_id"
EXPECTED_STATE="o-fresh|UNKNOWN|0|00:05:00
o-old|FAILED|1|-
p-fresh|PENDING|0|-
p-old|FAILED|1|-"
HISTORY="SELECT p.order_id || '|' || h.from_status || '->' || h.to_status || '|' || h.reason
           FROM payment_status_history h JOIN payments p ON p.id = h.payment_id ORDER BY p.order_id"

# old_failed DB [search_path] - succeeds once o-old and p-old have been marked FAILED
# (default: what psql_q already sets for DB, i.e. k07 for e2e_k07)
old_failed() {
  [[ "$(psql_q "$1" "${2:+SET search_path=$2; }SELECT count(*) FROM payments
                     WHERE order_id IN ('o-old', 'p-old') AND status = 'FAILED'")" == 2 ]]
}

# ---------- Kotlin -----------------------------------------------------------------------------
DB=e2e_k07
reset_db "$DB"

# 1) scheduler off: nothing keeps the app alive and nothing touches the row
#    schema.sql creates schema k07 first; psql_q runs it with search_path=k07, like the app's hikari.schema
psql_q "$DB" "$(cat "$REPO/kotlin/07-payment-state-machine/src/main/resources/schema.sql")" >/dev/null
psql_q "$DB" "$ROWS" >/dev/null
rc="$(run_kotlin_fg 07-payment-state-machine k07-off 90 --spring.datasource.url="$(jdbc_url $DB)" \
      --lab.janitor.enabled=false)"
check "app context started (scheduler off)" grep -q "Started StateMachineApplication" "$E2E_LOG_DIR/k07-off.log"
check_eq "lab.janitor.enabled=false: app exits right after boot" 0 "$rc"
check_eq "lab.janitor.enabled=false: due UNKNOWN and old PENDING rows untouched" 0 \
  "$(psql_q $DB "SELECT count(*) FROM payments WHERE attempt_count <> 0
                  OR status <> CASE WHEN order_id LIKE 'p-%' THEN 'PENDING' ELSE 'UNKNOWN' END")"

# 2) scheduler on (default), short interval: the app processes due rows by itself
reset_db "$DB"
start_kotlin 07-payment-state-machine k07-on --spring.datasource.url="$(jdbc_url $DB)" \
  --lab.janitor.interval=1s --logging.level.lab.statemachine.JanitorScheduler=DEBUG
KPID=$LAST_PID
check "app context started (scheduler on)" wait_log "$E2E_LOG_DIR/k07-on.log" "Started StateMachineApplication" 90
check "idle runs are logged at DEBUG (scheduler ticking)" \
  wait_log "$E2E_LOG_DIR/k07-on.log" "janitor run: pending=0 unknown=0 cancelRequested=0" 10
psql_q "$DB" "$ROWS" >/dev/null
check "o-old and p-old become FAILED without any direct janitor call" \
  wait_for 20 old_failed "$DB"
check_eq "row states after scheduled run: order|status|attempts|retry-created" "$EXPECTED_STATE" "$(psql_q $DB "$ROW_STATE")"
check_eq "history written by the scheduled janitor" "o-old|UNKNOWN->FAILED|janitor: 유예 시간 후에도 PG 기록 없음
p-old|PENDING->FAILED|janitor: 오래된 PENDING, 유예 후에도 PG 기록 없음" "$(psql_q $DB "$HISTORY")"
check "run summary logged at INFO: pending=1 unknown=2" \
  grep -q "INFO.*janitor run: pending=1 unknown=2 cancelRequested=0" "$E2E_LOG_DIR/k07-on.log"
check "app stays alive (scheduler thread)" kill -0 "$KPID"
kill "$KPID"
check "app shuts down on SIGTERM" wait_for 15 bash -c "! kill -0 $KPID"

# ---------- Go ---------------------------------------------------------------------------------
GDB=e2e_g07
reset_db "$GDB"
gbin="$(go_bin 07-payment-state-machine g07)"
start_bg g07 env DATABASE_URL="$(pg_dsn $GDB)" JANITOR_INTERVAL=1s "$gbin"
GPID=$LAST_PID
check "go janitor started" wait_log "$E2E_LOG_DIR/g07.log" "janitor started (interval=1s, schema=go_07)" 30
psql_q "$GDB" "SET search_path=go_07; $ROWS" >/dev/null
check "go: o-old and p-old become FAILED without any direct janitor call" \
  wait_for 20 old_failed "$GDB" go_07
check_eq "go: row states after scheduled run" "$EXPECTED_STATE" "$(psql_q $GDB "SET search_path=go_07; $ROW_STATE")"
check_eq "go: history written by the scheduled janitor" "o-old|UNKNOWN->FAILED|janitor: pg has no record after grace
p-old|PENDING->FAILED|janitor: stale pending, no pg record after grace" \
  "$(psql_q $GDB "SET search_path=go_07; $HISTORY")"
check "go: run summary logged" grep -q "janitor run: pending{approved=0 failed=1 toUnknown=0} unknown{approved=0 failed=1 manualReview=0 rescheduled=1}" "$E2E_LOG_DIR/g07.log"
check "go: process stays alive" kill -0 "$GPID"
kill -INT "$GPID"
check "go: Run returns on SIGINT (ctx cancelled) and main exits" wait_for 10 bash -c "! kill -0 $GPID"
check "go: 'janitor stopped' logged" grep -q "janitor stopped" "$E2E_LOG_DIR/g07.log"
summary "07-state-machine"
