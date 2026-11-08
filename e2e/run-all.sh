#!/usr/bin/env bash
# Runs every e2e stage script in order and prints one summary line per stage.
#
#   e2e/run-all.sh            # all stages (10 runs with --quick)
#   e2e/run-all.sh 02 05 09   # only these stages
#
# Stages keep going after a failure; the exit code is 1 if any stage failed.
# 04-cdc.sh and 10-observability.sh are run only if they exist (they bring up the cdc/obs profiles themselves).
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pcl-e2e-all.XXXXXX")"

STAGES=(
  "01|01-dual-write.sh"
  "02|02-idempotency.sh"
  "03|03-outbox-polling.sh"
  "04|04-cdc.sh"
  "05|05-idempotent-consumer.sh"   # after 03: also drains 03's topic
  "06|06-balance-concurrency.sh"
  "07|07-state-machine.sh"
  "08|08-ledger.sh"
  "09|09-reconciliation.sh"
  "10|10-observability.sh --quick"
)

want() {
  [[ $# -eq 0 ]] && return 0
  local n="$1"; shift
  for s in "$@"; do [[ "$s" == "$n" ]] && return 0; done
  return 1
}

RESULTS=()
FAILED=0
for entry in "${STAGES[@]}"; do
  n="${entry%%|*}"; cmd="${entry#*|}"; script="${cmd%% *}"
  want "$n" "$@" || continue
  if [[ ! -x "$DIR/$script" ]]; then
    RESULTS+=("SKIP  $n  $script (not present)"); continue
  fi
  echo; echo "################ $cmd"
  start=$(date +%s)
  # shellcheck disable=SC2086
  "$DIR"/$cmd 2>&1 | tee "$LOG_DIR/$n.log"
  rc=${PIPESTATUS[0]}
  secs=$(( $(date +%s) - start ))
  line="$(grep -E '^==== ' "$LOG_DIR/$n.log" | tail -1 | sed 's/^==== //; s/ *(logs:.*//')"
  if [[ $rc -eq 0 ]]; then RESULTS+=("PASS  $n  ${secs}s  $line")
  else RESULTS+=("FAIL  $n  ${secs}s  ${line:-$script exited $rc}"); FAILED=1; fi
done

echo
echo "================ e2e summary (full output: $LOG_DIR)"
for r in "${RESULTS[@]}"; do echo "  $r"; done
[[ $FAILED -eq 0 ]] && echo "ALL PASS" || { echo "SOME STAGES FAILED"; exit 1; }
