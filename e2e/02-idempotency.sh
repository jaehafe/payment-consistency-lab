#!/usr/bin/env bash
# 02 idempotency key: real HTTP against the Kotlin app and the Go cmd.
#
#   - same key twice          -> both 201, byte-identical body, 1 payments row
#   - same key, other body    -> 422
#   - 10 concurrent, same key -> only 201/409 (no 5xx), every 201 body identical, 1 payments row,
#                                1 distinct pg_tx_id, idempotency key ends 'finished' and unlocked
#   - no Idempotency-Key      -> 400, nothing written
#
# The fake PG lives in-process and exposes chargeCount()/callCount() only to tests, so "one PG charge"
# is observed indirectly: one payments row, one pg_tx_id shared by every 201 response.
source "$(dirname "$0")/lib.sh"

K_PORT=18080
G_PORT=18081

# run_scenarios LABEL BASE_URL DB SCHEMA
run_scenarios() {
  local label="$1" base="$2" db="$3" schema="$4"
  local P="$schema.payments" K="$schema.idempotency_keys"
  local r1 r2 r3 r4

  log "[$label] same key twice"
  r1="$(http POST "$base/payments" -H 'Idempotency-Key: e2e-k1' -H 'Content-Type: application/json' -d '{"orderId":"o-1","amount":10000}')"
  r2="$(http POST "$base/payments" -H 'Idempotency-Key: e2e-k1' -H 'Content-Type: application/json' -d '{"orderId":"o-1","amount":10000}')"
  echo "    1st: $r1" >&2; echo "    2nd: $r2" >&2
  check_eq "[$label] 1st request -> 201" 201 "${r1%% *}"
  check "[$label] 2nd request with same key -> identical status and body" test "$r1" == "$r2"
  check_eq "[$label] payments rows for o-1" 1 "$(psql_q "$db" "SELECT count(*) FROM $P WHERE order_id='o-1'")"

  log "[$label] same key, different body"
  r3="$(http POST "$base/payments" -H 'Idempotency-Key: e2e-k1' -H 'Content-Type: application/json' -d '{"orderId":"o-1","amount":99999}')"
  echo "    $r3" >&2
  check_eq "[$label] same key + different amount -> 422" 422 "${r3%% *}"
  check_eq "[$label] stored amount unchanged" 10000 "$(psql_q "$db" "SELECT amount FROM $P WHERE order_id='o-1'")"

  log "[$label] 10 concurrent requests, same key"
  local dir="$E2E_LOG_DIR/$label-concurrent"; mkdir -p "$dir"
  local i pids=()
  for i in $(seq 1 10); do
    curl -s -o "$dir/$i.body" -w '%{http_code}\n' -X POST "$base/payments" \
      -H 'Idempotency-Key: e2e-conc' -H 'Content-Type: application/json' \
      -d '{"orderId":"o-conc","amount":5000}' >"$dir/$i.code" &
    pids+=($!)
  done
  wait "${pids[@]}"   # only the curls - a bare `wait` would also wait for the app process
  local codes; codes="$(cat "$dir"/*.code | sort | uniq -c | awk '{printf "%sx%s ", $2, $1}')"
  echo "    status codes: $codes" >&2
  check "[$label] concurrent: only 201/409, no 5xx" bash -c "! cat '$dir'/*.code | grep -qv -E '^(201|409)$'"
  check "[$label] concurrent: at least one 201" bash -c "grep -l '^201$' '$dir'/*.code >/dev/null"
  local distinct_201
  distinct_201="$(for f in "$dir"/*.code; do if [[ "$(cat "$f")" == 201 ]]; then cat "${f%.code}.body"; echo; fi; done | sort -u | wc -l | tr -d ' ')"
  check_eq "[$label] concurrent: every 201 body identical" 1 "$distinct_201"
  check_eq "[$label] concurrent: payments rows for o-conc" 1 "$(psql_q "$db" "SELECT count(*) FROM $P WHERE order_id='o-conc'")"
  check_eq "[$label] concurrent: distinct pg_tx_id (= PG charged once)" 1 "$(psql_q "$db" "SELECT count(DISTINCT pg_tx_id) FROM $P WHERE order_id='o-conc'")"
  check_eq "[$label] concurrent: key row finished and unlocked" "1|finished|t" \
    "$(psql_q "$db" "SELECT count(*), max(recovery_point), bool_and(locked_at IS NULL) FROM $K WHERE idempotency_key='e2e-conc'")"
  # a request arriving after the burst replays the stored 201
  r4="$(http POST "$base/payments" -H 'Idempotency-Key: e2e-conc' -H 'Content-Type: application/json' -d '{"orderId":"o-conc","amount":5000}')"
  check_eq "[$label] concurrent: retry after the burst -> 201 replay" 201 "${r4%% *}"

  log "[$label] missing header"
  r4="$(http POST "$base/payments" -H 'Content-Type: application/json' -d '{"orderId":"o-nohdr","amount":1000}')"
  echo "    $r4" >&2
  check_eq "[$label] no Idempotency-Key -> 400" 400 "${r4%% *}"
  check_eq "[$label] no payment written without header" 0 "$(psql_q "$db" "SELECT count(*) FROM $P WHERE order_id='o-nohdr'")"
  check_eq "[$label] total payments rows" 2 "$(psql_q "$db" "SELECT count(*) FROM $P")"
}

ensure_infra
port_free $K_PORT || die "port $K_PORT busy"
port_free $G_PORT || die "port $G_PORT busy"

# ---- Kotlin ----
reset_db e2e_k02
start_kotlin 02-idempotency-key k02 --server.port=$K_PORT --spring.datasource.url="$(jdbc_url e2e_k02)"
wait_log "$E2E_LOG_DIR/k02.log" "Started IdempotencyApplication" 90 || die "Kotlin 02 did not start (see $E2E_LOG_DIR/k02.log)"
run_scenarios kotlin "http://localhost:$K_PORT" e2e_k02 k02   # spring.datasource.hikari.schema=k02

# ---- Go (cmd reads DATABASE_URL and PORT, uses schema go_02) ----
reset_db e2e_g02
GBIN="$(go_bin 02-idempotency-key g02)"
start_bg g02 env DATABASE_URL="$(pg_dsn e2e_g02)" PORT=$G_PORT "$GBIN"
wait_log "$E2E_LOG_DIR/g02.log" "listening on" 30 || die "Go 02 did not start (see $E2E_LOG_DIR/g02.log)"
run_scenarios go "http://localhost:$G_PORT" e2e_g02 go_02

summary "02-idempotency"
