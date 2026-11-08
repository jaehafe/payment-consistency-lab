#!/usr/bin/env bash
# 09 reconciliation: run the SQL the way the READMEs tell a human to.
#
# Kotlin (README "SQL 직접 실행"):
#   ./gradlew :09-reconciliation:bootRun        -> creates the tables and exits
#   psql ... -v business_date="'2026-11-07'"  +  SET search_path TO k09;  +  \i 09-reconciliation/src/main/resources/recon_queries.sql
#   against internal rows = ReconciliationTest.runMixed() and PG rows = fixtures/pg_settlement_2026-11-07.csv
# Go (README: no cmd -> \i schema.sql, then PREPARE q(date) AS <query body>; EXECUTE q('2026-11-08'))
#   against testdata/pg_settlement_2026-11-08.csv and a hand-made internal side.
# Both DBs are scratch DBs (the README uses DB "lab", which belongs to stage 04 here).
source "$(dirname "$0")/lib.sh"
KDB=e2e_k09
GDB=e2e_g09

# load_settlement_csv DB FILE  - recon_files row + COPY of the CSV into pg_settlement_records
load_settlement_csv() {
  local db="$1" file="$2"
  psql_q "$db" "CREATE TABLE e2e_stage (pg_tx_id text, order_id text, type text, amount bigint, fee bigint, transacted_at timestamptz)" >/dev/null
  _psql "$db" -v ON_ERROR_STOP=1 -qc "COPY e2e_stage FROM STDIN WITH (FORMAT csv, HEADER true)" <"$file"
  psql_q "$db" "
    WITH f AS (INSERT INTO recon_files (file_name, file_hash, row_count)
               SELECT '$(basename "$file")', 'e2e-' || md5(random()::text), count(*) FROM e2e_stage RETURNING id)
    INSERT INTO pg_settlement_records (file_id, pg_tx_id, order_id, type, amount, fee, transacted_at)
    SELECT f.id, s.pg_tx_id, s.order_id, s.type, s.amount, s.fee, s.transacted_at FROM e2e_stage s, f;
    DROP TABLE e2e_stage;" >/dev/null
}

# host psql like the README; falls back to psql inside the container
run_psql() {
  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD=lab psql -h localhost -p "$PG_PORT" -U lab "$@"
  else
    compose exec -T postgres psql -U lab "$@"
  fi
}

ensure_infra

# =============================== Kotlin =========================================================
reset_db "$KDB"
log "README step: ./gradlew :09-reconciliation:bootRun (pointed at $KDB)"
rc=0
(cd "$REPO/kotlin" && ./gradlew -q :09-reconciliation:bootRun --args="--spring.datasource.url=$(jdbc_url $KDB)") \
  >"$E2E_LOG_DIR/k09-bootRun.log" 2>&1 || rc=$?
check_eq "[kotlin] bootRun creates the tables and exits by itself" 0 "$rc"
check_eq "[kotlin] schema applied in k09" "internal_transactions,pg_settlement_records,recon_files,recon_results" \
  "$(psql_q $KDB "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema='k09'")"

# internal side = ReconciliationTest.runMixed()
psql_q "$KDB" "INSERT INTO internal_transactions (id, pg_tx_id, order_id, type, amount, occurred_at) VALUES
  (gen_random_uuid(), 'tx-ok-1',         'order-1', 'PAYMENT', 10000, '2026-11-07T01:00:00Z'),
  (gen_random_uuid(), 'tx-ok-2',         'order-2', 'PAYMENT', 20000, '2026-11-07T02:00:00Z'),
  (gen_random_uuid(), 'tx-mismatch',     'order-3', 'PAYMENT', 30000, '2026-11-07T03:00:00Z'),
  (gen_random_uuid(), 'tx-missing-pg',   'order-4', 'PAYMENT', 15000, '2026-11-07T03:30:00Z'),
  (gen_random_uuid(), 'tx-dup-pg',       'order-7', 'PAYMENT',  7000, '2026-11-07T05:00:00Z'),
  (gen_random_uuid(), 'tx-dup-internal', 'order-5', 'PAYMENT',  5000, '2026-11-07T06:00:00Z'),
  (gen_random_uuid(), 'tx-dup-internal', 'order-5', 'PAYMENT',  5000, '2026-11-07T06:00:01Z'),
  (gen_random_uuid(), 'tx-next-day',     'order-8', 'PAYMENT',  9000, '2026-11-07T15:30:00Z')" >/dev/null
load_settlement_csv "$KDB" "$REPO/kotlin/09-reconciliation/src/test/resources/fixtures/pg_settlement_2026-11-07.csv"
check_eq "[kotlin] fixture rows loaded" 8 "$(psql_q $KDB 'SELECT count(*) FROM pg_settlement_records')"

log "README step: psql -v business_date=\"'2026-11-07'\" ; SET search_path TO k09; \\i 09-reconciliation/src/main/resources/recon_queries.sql"
KOUT="$E2E_LOG_DIR/k09-psql.out"
(cd "$REPO/kotlin" && printf '%s\n' 'SET search_path TO k09;' '\i 09-reconciliation/src/main/resources/recon_queries.sql' \
  | run_psql "$KDB" -X -v ON_ERROR_STOP=1 -v business_date="'2026-11-07'" -qAt) >"$KOUT" 2>&1 || true
sed 's/^/    /' "$KOUT" >&2
KEXP="AMOUNT_MISMATCH|tx-mismatch|30000|29000|-1000
MISSING_IN_INTERNAL|tx-missing-internal||12000|12000
MISSING_IN_PG|tx-missing-pg|15000||-15000
DUPLICATE_IN_PG|tx-dup-pg||14000|7000|2
DUPLICATE_IN_INTERNAL|tx-dup-internal|10000||-5000|2
4"
check "[kotlin] \\i recon_queries.sql runs without error" bash -c "! grep -qi error '$KOUT'"
check_eq "[kotlin] discrepancies / duplicates / matched_count (latest_run_summary empty: no app run)" "$KEXP" "$(cat "$KOUT")"
check "[kotlin] tx-next-day (UTC 15:30 = KST 11-08 00:30) excluded" bash -c "! grep -q tx-next-day '$KOUT'"

# =============================== Go =============================================================
reset_db "$GDB"
log "Go README: \\i schema.sql (no cmd)"
(cd "$REPO/go/09-reconciliation" && echo '\i schema.sql' | run_psql "$GDB" -X -v ON_ERROR_STOP=1 -q) >/dev/null
check_eq "[go] schema applied via \\i schema.sql" 4 \
  "$(psql_q $GDB "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")"
psql_q "$GDB" "INSERT INTO internal_transactions (id, pg_tx_id, order_id, type, amount, occurred_at) VALUES
  (gen_random_uuid(), 'pg-001', 'order-001', 'PAYMENT', 10000, '2026-11-08T09:00:00+09:00'),
  (gen_random_uuid(), 'pg-001', 'order-001', 'PAYMENT', 10000, '2026-11-08T09:00:05+09:00'),
  (gen_random_uuid(), 'pg-002', 'order-002', 'PAYMENT', 24000, '2026-11-08T10:15:00+09:00'),
  (gen_random_uuid(), 'pg-004', 'order-004', 'REFUND',   5000, '2026-11-08T13:45:00+09:00'),
  (gen_random_uuid(), 'pg-005', 'order-005', 'PAYMENT', 12000, '2026-11-08T15:00:00+09:00'),
  (gen_random_uuid(), 'pg-006', 'order-006', 'PAYMENT',  7000, '2026-11-08T16:00:00+09:00'),
  (gen_random_uuid(), 'pg-007', 'order-007', 'PAYMENT',  3000, '2026-11-08T23:59:00+09:00'),
  (gen_random_uuid(), 'pg-009', 'order-009', 'PAYMENT',  1000, '2026-11-09T00:00:00+09:00')" >/dev/null
load_settlement_csv "$GDB" "$REPO/go/09-reconciliation/testdata/pg_settlement_2026-11-08.csv"

# body QUERY_NAME -> the query text between its "-- name:" line and the next one, without the final ';'
body() {
  awk -v n="$1" '/^-- name: /{p = ($3 == n); next} p' "$REPO/go/09-reconciliation/recon_queries.sql" \
    | sed -e :a -e '/^[[:space:]]*$/{$d;N;ba' -e '}' | sed '$ s/;[[:space:]]*$//'
}
GSQL="$E2E_LOG_DIR/g09-prepare.sql"
{
  echo "PREPARE cmp(date) AS"; body compare_by_pg_tx_id; echo ";"
  echo "PREPARE dpg(date) AS"; body duplicate_in_pg; echo ";"
  echo "PREPARE dint(date) AS"; body duplicate_in_internal; echo ";"
  echo "EXECUTE cmp('2026-11-08');"
  echo "EXECUTE dpg('2026-11-08');"
  echo "EXECUTE dint('2026-11-08');"
} >"$GSQL"
GOUT="$E2E_LOG_DIR/g09-psql.out"
run_psql "$GDB" -X -v ON_ERROR_STOP=1 -qAt <"$GSQL" >"$GOUT" 2>&1 || true
sed 's/^/    /' "$GOUT" >&2
GEXP="MATCHED|pg-001|10000|10000|0
AMOUNT_MISMATCH|pg-002|24000|25000|1000
MISSING_IN_INTERNAL|pg-003||8000|8000
MATCHED|pg-004|5000|5000|0
MATCHED|pg-005|12000|12000|0
MISSING_IN_PG|pg-006|7000||-7000
MATCHED|pg-007|3000|3000|0
DUPLICATE_IN_PG|pg-005||12000|12000
DUPLICATE_IN_INTERNAL|pg-001|10000||-10000"
check "[go] PREPARE/EXECUTE of the three query bodies runs without error" bash -c "! grep -qi error '$GOUT'"
check_eq "[go] compare / duplicate_in_pg / duplicate_in_internal" "$GEXP" "$(cat "$GOUT")"
check "[go] pg-009 at KST 11-09 00:00:00 excluded (half-open day)" bash -c "! grep -q pg-009 '$GOUT'"
skip "[go] results_by_run: needs a run_id from the Go service (no cmd), recon_results is empty"

summary "09-reconciliation"
