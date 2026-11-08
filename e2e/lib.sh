# shellcheck shell=bash
# Shared helpers for the e2e scripts. Source it from a stage script:
#
#   source "$(dirname "$0")/lib.sh"
#
# It never runs `docker compose down` and never removes containers: the compose stack is shared.
# Each stage uses its own database (e2e_<lang><NN>) and its own Kafka topic, so stages do not see
# each other's rows or messages.

set -euo pipefail

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$E2E_DIR/.." && pwd)"
COMPOSE_FILE="$REPO/infra/docker-compose.yml"
PG_PORT="${PG_PORT:-15432}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:19092}"
E2E_LOG_DIR="${E2E_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pcl-e2e.XXXXXX")}"
mkdir -p "$E2E_LOG_DIR"

_PIDS=()
_PASS=0
_FAIL=0
_SKIP=0
_RESULTS=()

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

log()  { printf '\033[36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf '\033[31mFATAL:\033[0m %s\n' "$*" >&2; exit 1; }

# ---------- assertions ----------------------------------------------------------------------

# check "description" <command...>   - runs the command, records PASS/FAIL (never exits early)
check() {
  local desc="$1"; shift
  if "$@"; then
    _PASS=$((_PASS + 1)); _RESULTS+=("PASS  $desc"); printf '  \033[32mPASS\033[0m %s\n' "$desc" >&2
  else
    _FAIL=$((_FAIL + 1)); _RESULTS+=("FAIL  $desc"); printf '  \033[31mFAIL\033[0m %s\n' "$desc" >&2
  fi
}

# check_eq "description" expected actual
check_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [[ "$expected" == *$'\n'* || "$actual" == *$'\n'* ]]; then   # multi-line: show a diff instead
    if [[ "$expected" == "$actual" ]]; then
      check "$desc ($(printf '%s\n' "$actual" | wc -l | tr -d ' ') lines match)" true
    else
      diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") | sed 's/^/      /' >&2 || true
      check "$desc (output differs, see diff above)" false
    fi
  elif [[ "$expected" == "$actual" ]]; then
    check "$desc (= $actual)" true
  else
    check "$desc (expected '$expected', got '$actual')" false
  fi
}

skip() { _SKIP=$((_SKIP + 1)); _RESULTS+=("SKIP  $*"); printf '  \033[33mSKIP\033[0m %s\n' "$*" >&2; }

# summary: prints the table and returns 1 if anything failed. Call it as the last line.
summary() {
  local name="${1:-$(basename "$0")}"
  echo
  echo "==== $name: $_PASS passed, $_FAIL failed, $_SKIP skipped   (logs: $E2E_LOG_DIR)"
  for r in "${_RESULTS[@]}"; do echo "  $r"; done
  if [[ $_FAIL -gt 0 ]]; then echo "RESULT: FAIL"; return 1; fi
  echo "RESULT: PASS"
}

# ---------- infra ---------------------------------------------------------------------------

ensure_infra() {
  compose up -d postgres redpanda >/dev/null 2>&1 || die "docker compose up failed"
  local i
  for i in $(seq 1 60); do
    if compose exec -T postgres pg_isready -U lab -d lab >/dev/null 2>&1 \
       && compose exec -T redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'; then
      return 0
    fi
    sleep 1
  done
  die "postgres/redpanda not ready"
}

# psql_q DB "SQL"  -> unaligned, tuples only, runs inside the postgres container
psql_q() { compose exec -T postgres psql -U lab -d "$1" -v ON_ERROR_STOP=1 -qtAX -c "$2"; }

# psql_f DB [psql args...] < file   -> runs a script from stdin
psql_f() { local db="$1"; shift; compose exec -T postgres psql -U lab -d "$db" -v ON_ERROR_STOP=1 -X "$@"; }

# reset_db NAME - drops and recreates a scratch database. Only e2e_* names are allowed.
reset_db() {
  [[ "$1" == e2e_* ]] || die "reset_db refuses non-e2e database '$1'"
  psql_q lab "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null 2>&1
  psql_q lab "CREATE DATABASE $1" >/dev/null
}

jdbc_url() { echo "jdbc:postgresql://localhost:$PG_PORT/$1"; }
pg_dsn()   { echo "postgres://lab:lab@localhost:$PG_PORT/$1?sslmode=disable"; }

rpk() { compose exec -T redpanda rpk "$@"; }

# reset_topic NAME [partitions]
reset_topic() {
  rpk topic delete "$1" >/dev/null 2>&1 || true
  sleep 1
  rpk topic create "$1" -p "${2:-3}" -r 1 >/dev/null
}

# topic_count NAME -> total number of records (sum of high watermarks; topics here are never compacted)
topic_count() {
  rpk topic describe "$1" -p 2>/dev/null | awk 'NR>1 && $NF ~ /^[0-9]+$/ {s+=$NF} END {print s+0}'
}

# dump_topic TOPIC -> every record currently in the topic, one per line, then exits:
#   <partition>@<offset><TAB><h1>=<v1>;<h2>=<v2>;<TAB><key><TAB><value>
dump_topic() { rpk topic consume "$1" -o :end -f '%p@%o\t%h{%k=%v;}\t%k\t%v\n'; }

# header_values TOPIC HEADER -> the value of HEADER for every record, one per line
header_values() {
  dump_topic "$1" | awk -F'\t' -v h="$2" '{ n=split($2, a, ";"); for (i=1;i<=n;i++) { split(a[i], kv, "="); if (kv[1]==h) print substr(a[i], length(h)+2) } }'
}

# ---------- app processes -------------------------------------------------------------------

_cleanup() {
  local pid
  for pid in "${_PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  sleep 1
  for pid in "${_PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true
  done
}
trap _cleanup EXIT INT TERM

# kotlin_jar MODULE -> path to the boot jar, building it if needed (E2E_REBUILD=1 forces a build)
kotlin_jar() {
  local module="$1" jar
  jar="$(ls "$REPO/kotlin/$module/build/libs/"*.jar 2>/dev/null | head -1 || true)"
  if [[ -z "$jar" || "${E2E_REBUILD:-0}" == 1 ]]; then
    log "building $module boot jar"
    (cd "$REPO/kotlin" && ./gradlew -q ":$module:bootJar") >&2
    jar="$(ls "$REPO/kotlin/$module/build/libs/"*.jar | head -1)"
  fi
  echo "$jar"
}

# start_kotlin MODULE LOGNAME [spring args...]  -> sets LAST_PID
start_kotlin() {
  local module="$1" name="$2"; shift 2
  local jar; jar="$(kotlin_jar "$module")"
  java -jar "$jar" "$@" >"$E2E_LOG_DIR/$name.log" 2>&1 &
  LAST_PID=$!
  _PIDS+=("$LAST_PID")
  log "started $module as $name (pid $LAST_PID, log $E2E_LOG_DIR/$name.log)"
}

# run_kotlin_fg MODULE LOGNAME TIMEOUT_SEC [spring args...] -> runs to completion; prints exit code
# (for library-style modules whose app has nothing keeping it alive)
run_kotlin_fg() {
  local module="$1" name="$2" timeout="$3"; shift 3
  start_kotlin "$module" "$name" "$@"
  local pid=$LAST_PID i
  for i in $(seq 1 "$timeout"); do
    if ! kill -0 "$pid" 2>/dev/null; then
      local rc=0; wait "$pid" || rc=$?; echo "$rc"; return 0
    fi
    sleep 1
  done
  echo "running"
}

# go_bin PKG_DIR NAME -> builds ./go/<PKG_DIR>/cmd into the log dir, prints the binary path
go_bin() {
  local out="$E2E_LOG_DIR/bin/$2"
  mkdir -p "$E2E_LOG_DIR/bin"
  (cd "$REPO/go" && go build -o "$out" "./$1/cmd") >&2
  echo "$out"
}

# start_bg LOGNAME cmd... (env vars may be set by the caller with `env`) -> sets LAST_PID
start_bg() {
  local name="$1"; shift
  "$@" >"$E2E_LOG_DIR/$name.log" 2>&1 &
  LAST_PID=$!
  _PIDS+=("$LAST_PID")
  log "started $name (pid $LAST_PID, log $E2E_LOG_DIR/$name.log)"
}

# wait_for TIMEOUT_SEC cmd...  - polls until the command succeeds
wait_for() {
  local t="$1"; shift; local i
  for i in $(seq 1 "$t"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  return 1
}

# wait_log FILE PATTERN TIMEOUT_SEC
wait_log() { wait_for "$3" grep -q -- "$2" "$1"; }

# wait_http URL TIMEOUT_SEC  - any HTTP response (even 4xx/405) counts as "up"
wait_http() { wait_for "$2" curl -s -o /dev/null "$1"; }

# http METHOD URL [curl args...] -> prints "<status> <body>"
http() {
  local m="$1" u="$2"; shift 2
  local body code
  body="$(curl -s -w $'\n%{http_code}' -X "$m" "$u" "$@")"
  code="${body##*$'\n'}"; body="${body%$'\n'*}"
  echo "$code $body"
}

# port_free PORT -> fails if something already listens there
port_free() { ! lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }
