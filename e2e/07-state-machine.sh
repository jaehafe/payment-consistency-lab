#!/usr/bin/env bash
# 07 payment state machine: does the janitor run on a schedule when the app is started?
#
# Janitor.reconcileUnknown(now) / retryCancelRequested(now) exist, but nothing schedules them:
# no @Scheduled, no @EnableScheduling, no web endpoint, no runner (README "한계": "janitor는 여기서
# 테스트가 직접 호출한다"). This script inserts an UNKNOWN payment whose next_retry_at is in the past,
# starts the app and shows that the app exits right after boot and the row is left untouched.
# Go 07 is a library package (no cmd) -> skipped.
source "$(dirname "$0")/lib.sh"
DB=e2e_k07
ensure_infra
reset_db "$DB"

rc="$(run_kotlin_fg 07-payment-state-machine k07-1 90 --spring.datasource.url="$(jdbc_url $DB)")"
check "app context started" grep -q "Started StateMachineApplication" "$E2E_LOG_DIR/k07-1.log"
check_eq "app exits by itself with code 0" 0 "$rc"
check_eq "schema applied: payments, payment_status_history" "payment_status_history,payments" \
  "$(psql_q $DB "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema='public'")"
check_eq "janitor partial index exists" 1 "$(psql_q $DB "SELECT count(*) FROM pg_indexes WHERE indexname='payments_janitor_idx'")"

psql_q "$DB" "INSERT INTO payments (id, order_id, amount, status, idempotency_key, next_retry_at, created_at)
              VALUES (gen_random_uuid(), 'o-unknown', 10000, 'UNKNOWN', 'idem-unknown', now() - interval '10 minutes', now() - interval '1 hour')" >/dev/null
rc="$(run_kotlin_fg 07-payment-state-machine k07-2 90 --spring.datasource.url="$(jdbc_url $DB)" --lab.janitor.base-backoff=1s)"
check_eq "app with a due UNKNOWN row still exits right after boot (no scheduler keeps it alive)" 0 "$rc"
sleep 2
check_eq "due UNKNOWN row untouched: status|attempt_count" "UNKNOWN|0" \
  "$(psql_q $DB "SELECT status || '|' || attempt_count FROM payments WHERE order_id='o-unknown'")"
check_eq "no status history written" 0 "$(psql_q $DB 'SELECT count(*) FROM payment_status_history')"
check "no Janitor activity in the log" bash -c "! grep -qi janitor '$E2E_LOG_DIR/k07-2.log'"
check_eq "source has no @Scheduled/@EnableScheduling (janitor is test-driven only)" 0 \
  "$(grep -rE '^\s*@(Scheduled|EnableScheduling)' "$REPO/kotlin/07-payment-state-machine/src/main" | wc -l | tr -d ' ')"
skip "janitor-on-schedule: not implemented in Kotlin 07 (documented limitation) - nothing to observe"
skip "Go 07: library package only (no cmd) - behaviour covered by go test ./07-payment-state-machine/"
summary "07-state-machine"
