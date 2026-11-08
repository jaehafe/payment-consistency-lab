#!/usr/bin/env bash
# 08 ledger + settlement: library-style module (LedgerService/SettlementService are test-driven).
# This script proves the app boots against its own DB and applies the schema, then checks the
# invariants the schema itself enforces with plain SQL, and runs the README's trial-balance query.
# Go 08 is a library package (no cmd) -> skipped.
source "$(dirname "$0")/lib.sh"
DB=e2e_k08
ensure_infra
reset_db "$DB"

rc="$(run_kotlin_fg 08-ledger-settlement k08 90 --spring.datasource.url="$(jdbc_url $DB)")"
check "app context started" grep -q "Started LedgerApplication" "$E2E_LOG_DIR/k08.log"
check_eq "app exits by itself with code 0 (no web/scheduler/runner)" 0 "$rc"
check_eq "schema applied" "journal_entries,ledger_accounts,ledger_lines,settlements,transactions" \
  "$(psql_q $DB "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema='public'")"

# A balanced entry: customer pays 10,000 -> pg:receivable Dr 10,000 / merchant payable Cr 9,700 / fee revenue Cr 300
psql_q "$DB" "
  INSERT INTO ledger_accounts (id, type) VALUES ('pg:receivable','ASSET'), ('merchant:m1:payable','LIABILITY'), ('platform:fee_revenue','REVENUE');
  INSERT INTO journal_entries (id, reference_type, reference_id) VALUES ('00000000-0000-0000-0000-000000000001', 'PAYMENT', 'pg-1');
  INSERT INTO ledger_lines (entry_id, account_id, debit, credit) VALUES
    ('00000000-0000-0000-0000-000000000001', 'pg:receivable', 10000, 0),
    ('00000000-0000-0000-0000-000000000001', 'merchant:m1:payable', 0, 9700),
    ('00000000-0000-0000-0000-000000000001', 'platform:fee_revenue', 0, 300);
  UPDATE ledger_accounts a SET balance = s.b FROM (SELECT account_id, sum(debit) - sum(credit) b FROM ledger_lines GROUP BY 1) s WHERE s.account_id = a.id;" >/dev/null

check "schema rejects a line with both debit and credit" bash -c "! docker compose -f '$COMPOSE_FILE' exec -T postgres psql -U lab -d $DB -v ON_ERROR_STOP=1 -qc \"INSERT INTO ledger_lines (entry_id, account_id, debit, credit) VALUES ('00000000-0000-0000-0000-000000000001','pg:receivable',1,1)\" 2>/dev/null"
check "schema rejects posting the same reference twice (ledger idempotency)" bash -c "! docker compose -f '$COMPOSE_FILE' exec -T postgres psql -U lab -d $DB -v ON_ERROR_STOP=1 -qc \"INSERT INTO journal_entries (id, reference_type, reference_id) VALUES (gen_random_uuid(),'PAYMENT','pg-1')\" 2>/dev/null"
log "README trial balance query:"
psql_f "$DB" -c "SELECT a.id, a.type, a.balance, sum(l.debit) - sum(l.credit) AS lines_balance
FROM ledger_accounts a LEFT JOIN ledger_lines l ON l.account_id = a.id
GROUP BY a.id, a.type, a.balance ORDER BY a.id;" >&2
check_eq "trial balance: sum(balance) = 0" 0 "$(psql_q $DB 'SELECT sum(balance) FROM ledger_accounts')"
check_eq "trial balance: cached balance = sum of lines for every account" 0 \
  "$(psql_q $DB 'SELECT count(*) FROM ledger_accounts a LEFT JOIN (SELECT account_id, sum(debit)-sum(credit) b FROM ledger_lines GROUP BY 1) s ON s.account_id = a.id WHERE a.balance <> coalesce(s.b, 0)')"
skip "Go 08: library package only (no cmd) - behaviour covered by go test ./08-ledger-settlement/"
summary "08-ledger"
