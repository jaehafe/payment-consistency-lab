#!/usr/bin/env bash
# 06 balance concurrency: library-style module. The Spring app only applies schema.sql; the five
# withdraw strategies (naive / pessimistic / optimistic / conditional update / serializable) are driven
# by BalanceConcurrencyTest only. This script proves the app boots against its own DB, applies the
# schema, is restart-safe (IF NOT EXISTS) and exits 0 because nothing keeps it running.
# Go 06 is a library package (no cmd) -> skipped.
source "$(dirname "$0")/lib.sh"
DB=e2e_k06
ensure_infra
reset_db "$DB"

rc="$(run_kotlin_fg 06-balance-concurrency k06-1 90 --spring.datasource.url="$(jdbc_url $DB)")"
check "app context started" grep -q "Started BalanceApplication" "$E2E_LOG_DIR/k06-1.log"
check_eq "app exits by itself with code 0 (no web/scheduler/runner)" 0 "$rc"
check_eq "schema applied in k06: accounts, withdrawals" "accounts,withdrawals" \
  "$(psql_q $DB "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema='k06'")"
check_eq "accounts has no CHECK (balance >= 0) - intentional, so the naive race can overdraw" 0 \
  "$(psql_q $DB "SELECT count(*) FROM pg_constraint WHERE conrelid='accounts'::regclass AND contype='c'")"

psql_q "$DB" "INSERT INTO accounts (id, balance) VALUES ('acc-1', 10000)" >/dev/null
rc="$(run_kotlin_fg 06-balance-concurrency k06-2 90 --spring.datasource.url="$(jdbc_url $DB)")"
check_eq "second start on the same DB also exits 0 (schema is idempotent)" 0 "$rc"
check_eq "existing data survives a restart" 10000 "$(psql_q $DB "SELECT balance FROM accounts WHERE id='acc-1'")"
skip "Go 06: library package only (no cmd) - behaviour covered by go test ./06-balance-concurrency/"
summary "06-balance-concurrency"
