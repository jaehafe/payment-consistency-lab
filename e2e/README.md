# e2e: run the real apps, not the tests

The unit and integration tests (`./gradlew test`, `go test ./...`) start their own Postgres and Redpanda with Testcontainers.
The scripts in this directory work differently. They start the **actual Kotlin boot jars and Go `cmd` binaries** against the shared compose stack, then check the main claim of each stage. They check it with real HTTP calls, Kafka records (read with `rpk`) and database rows.

## Running

```bash
# from the repo root. Needs Docker, Java 21, Go, bash (macOS bash 3.2 is fine), curl. psql on the host is optional.
e2e/run-all.sh              # every stage, in order (10 runs with --quick)
e2e/run-all.sh 02 03 05     # only some stages
e2e/03-outbox-polling.sh    # one stage
```

- Each script starts `postgres` and `redpanda` itself with `docker compose -f infra/docker-compose.yml up -d`. It never runs `down` and never removes containers. 04 and 10 also bring up the `cdc` / `obs` profiles.
- Kotlin jars are built on first use (`./gradlew :<module>:bootJar`). Set `E2E_REBUILD=1` to force a rebuild after changing code.
- Each run writes its app logs to a fresh `$TMPDIR/pcl-e2e.*` directory. The path is printed in the summary.
- A script exits 1 if any check fails. Every script ends with a `PASS/FAIL/SKIP` table.
- Apps started by a script are killed on exit by a trap.

### Isolation (so stages and people do not step on each other)

| | Kotlin | Go |
|---|---|---|
| Database | `e2e_k01` … `e2e_k09` (dropped and recreated per run). Stage 04 uses `lab`, because the Debezium connector is fixed to `lab.public.outbox` | `e2e_g02`, `e2e_g03`, `e2e_g09`, plus the cmd's own schema (`go_02`, `go_03`) |
| Topic | `e2e.k01.events`, `e2e.k03.events`, `e2e.k05.events`, `e2e.k05.events-dlt` | `payment.events.go03` (hard-coded in the cmd) |
| Ports | 18080 (Kotlin 02) | 18081 (Go 02) |

Stage 04 uses 8083 and `payment.events`. Stage 10 uses 8090 and 8091, because `prometheus.yml` scrapes those ports.

Inside each Kotlin scratch DB `e2e_kNN` the app keeps its tables in schema `kNN` (`spring.datasource.hikari.schema`); `psql_q`/`psql_f` set `search_path=kNN` for those DBs automatically (`schema_for_db`), without `public` as a fallback, so a stage that wrote to `public` again would fail loudly. Stage 04 uses DB `lab`, schema `public` (the Debezium connector watches `public.outbox`).

`lib.sh` has the shared helpers: `check`/`check_eq`/`summary`, `reset_db`, `psql_q`, `rpk`, `dump_topic`, `header_values`, `start_kotlin`, `go_bin`, `wait_for`, `http`. Scripts 04 and 10 keep their helpers inline.

## What each script proves

| Script | Runs | Proves |
|---|---|---|
| `01-dual-write.sh` | Kotlin jar | The app is **test-only**. On start it applies `schema.sql`, creates the topic and exits 0. It has no endpoint, runner or scheduler, so it writes no payments or events. The three dual-write failures are reproduced only by `DualWriteProblemTest`. Go 01 has no cmd. |
| `02-idempotency.sh` | Kotlin jar :18080, Go cmd :18081 | `POST /payments`. **Same key twice**: 201 both times with a byte-identical body, and 1 row. **Same key with a different amount**: 422, and the stored amount is unchanged. **10 concurrent requests with one key**: only 201/409 come back (no 5xx), every 201 body is identical, there is 1 `payments` row and 1 distinct `pg_tx_id` (one PG charge), and the key ends `finished` and unlocked. A request after the burst replays 201. **No header**: 400 and nothing is written. |
| `03-outbox-polling.sh` | Kotlin jar ×4 runs, Go cmd ×2 | **Kotlin A**: the relay publishes rows. Each record has `message_id` = `outbox_polling.id` and `event_type=PaymentCompleted`, and `published_at` gets set. **Kotlin B**: this instance points at an unreachable broker (the shared Redpanda is not touched). Payments still commit, outbox rows stay unpublished and the relay logs `outbox relay 실패`. **Kotlin C**: two relay processes share one DB. They drain the backlog from B, then N=300 more rows. The topic holds every outbox id exactly once, and the per-relay "Sent ok" counts add up to the row count with both relays doing work. **Go D**: two cmd processes plus a seeded backlog of 50. Every outbox row is published exactly once. |
| `04-cdc.sh` | Agent A | See the header of that script. |
| `05-idempotent-consumer.sh` | Kotlin jar (ConsumerRunner) | **A**: rpk-produced events, including a duplicate `message_id`. Points are credited once, `processed_messages` has one row per id, orders become PAID and group lag reaches 0. **B**: after a restart nothing is reprocessed. A late duplicate is skipped and a new message is processed. **C**: if 03 ran first, a fresh group drains `e2e.k03.events`. It ends with one `processed_messages` row and one `point_ledger` row per distinct `message_id`. **D**: a poison payload and an event with no `message_id` land on `e2e.k05.events-dlt` with the `kafka_dlt-*` headers (3 attempts and 1 attempt), and the next message on the same partition is still processed. Go 05 has no cmd. |
| `06-balance-concurrency.sh` | Kotlin jar | Library-style. The app boots, applies the schema (deliberately no `CHECK (balance >= 0)`), exits 0 and can be restarted on the same DB. |
| `07-state-machine.sh` | Kotlin jar ×2 runs, Go cmd | **The janitor runs on a schedule.** With `--lab.janitor.enabled=false` the app exits right after boot, and the due UNKNOWN rows and the old PENDING row are untouched. With the scheduler on (`--lab.janitor.interval=1s`), rows inserted after boot are processed with no direct call. `o-old` (created 1h ago, so past the grace) becomes FAILED with a history row. `o-fresh` stays UNKNOWN with `attempt_count` 0 and `next_retry_at = created_at + 5m`. **PENDING sweep**: `p-old` (PENDING created 1h ago, so older than `pending-after` 2m and past the grace) becomes FAILED with a `PENDING->FAILED` history row. `p-fresh` (PENDING created now) stays PENDING with `attempt_count` 0. The app logs `janitor run: pending=1 unknown=2`, stays alive, and stops on SIGTERM. The Go cmd (`JANITOR_INTERVAL=1s`, schema `go_07`) shows the same row states and a `pending{...} unknown{...}` run summary, and on SIGINT `Run` returns and logs `janitor stopped`. The fake PG is in process memory, so the → APPROVED paths on a schedule are covered by `JanitorSchedulerTest` / `Test_run__reconciles_due_unknown_on_schedule` / `Test_run__settles_stale_pending_on_schedule`. |
| `08-ledger.sh` | Kotlin jar + psql | The app boots and applies the schema. The schema rejects a ledger line with both debit and credit, and rejects posting the same reference twice. The README's trial-balance query gives a sum of 0 and cached balances equal to the line sums. |
| `09-reconciliation.sh` | `./gradlew :09-reconciliation:bootRun` + psql | **Kotlin**: follows the README exactly (`bootRun`, then `psql -v business_date="'2026-11-07'"` with `SET search_path TO k09;` and `\i recon_queries.sql`) against the test fixture. The output is exactly MISMATCH −1,000 / MISSING_IN_INTERNAL +12,000 / MISSING_IN_PG −15,000 / DUP_PG +7,000 / DUP_INTERNAL −5,000 / matched 4, and the KST next-day row is excluded. **Go**: `\i schema.sql`, then the README's `PREPARE q(date) AS <body>; EXECUTE q('2026-11-08')` for the three query bodies against `testdata/*.csv`. The expected rows come back, and the half-open day excludes 11-09 00:00:00 KST. |
| `10-observability.sh --quick` | Agent A | See the header of that script. |

## What is not covered (by design)

- **Go 01, 05, 06, 08, 09 have no `cmd`.** The scripts mark these as SKIP and point to `go test`. For 09, the SQL is still exercised through psql.
- **Fault injection that only exists in tests** cannot be triggered from outside the process. That covers PG timeout after charge, a DB failure inside the transaction, and a relay or consumer crash between publish/commit. Those toggles are Spring beans or Go struct fields with no HTTP or env switch. The fake PG's `chargeCount()` is likewise visible only to tests, so "one PG charge" is observed through `payments`/`pg_tx_id`.
- **Stopping the shared Redpanda** is not done, because it would disrupt the Debezium connector of stage 04. Script 03 simulates the outage per instance instead.
