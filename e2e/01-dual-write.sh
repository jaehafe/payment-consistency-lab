#!/usr/bin/env bash
# 01 dual write: is the Kotlin app runnable, and what does it do when started?
#
# The module has no web endpoint, no scheduler and no runner. Its three failure modes
# (commitThenPublish / publishThenCommit / publishInsideTx) are only driven by DualWriteProblemTest.
# This script proves that starting the app = "apply schema + create topic + exit 0", i.e. test-only.
# Go 01 is a library package (no cmd) -> skipped.
source "$(dirname "$0")/lib.sh"

DB=e2e_k01
TOPIC=e2e.k01.events

ensure_infra
reset_db "$DB"
rpk topic delete "$TOPIC" >/dev/null 2>&1 || true

log "Kotlin 01: java -jar against $DB (topic $TOPIC)"
rc="$(run_kotlin_fg 01-dual-write-problem k01 90 \
  --spring.datasource.url="$(jdbc_url $DB)" --lab.topic="$TOPIC")"
LOG="$E2E_LOG_DIR/k01.log"

check "app context started" grep -q "Started DualWriteApplicationKt" "$LOG"
check_eq "app exits by itself with code 0 (nothing keeps it alive: no web/scheduler/runner)" 0 "$rc"
check_eq "schema.sql applied: payments table exists in schema k01" 1 "$(psql_q $DB "SELECT count(*) FROM information_schema.tables WHERE table_schema='k01' AND table_name='payments'")"
check_eq "nothing created in public (stage schema only)" 0 "$(psql_q $DB "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")"
check_eq "KafkaAdmin created the topic with 3 partitions" 3 "$(rpk topic describe "$TOPIC" -p | awk 'NR>1' | wc -l | tr -d ' ')"
check_eq "no payment written on startup" 0 "$(psql_q $DB 'SELECT count(*) FROM payments')"
check_eq "no event published on startup" 0 "$(topic_count "$TOPIC")"
skip "Go 01: library package only (no cmd) - behaviour covered by go test ./01-dual-write-problem/"

summary "01-dual-write"
