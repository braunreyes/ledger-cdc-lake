# Ledger CDC Lake: v1 Design

**Date:** 2026-10-23 · **Status:** draft for review

## Goal

A portfolio project that replicates a Postgres payments ledger into Iceberg with off-the-shelf OSS, then **proves** the copy is correct and **shows** how it fails and recovers. Built for data-infra applications (TRM Labs, Figma, Netflix, Block, Cohere).

The OSS does the moving. The author's own code is the **correctness and operations layer**: a reconciler, failure drills, and a runbook.

## Non-Goals (v1)

- A hand-written logical decoding client. Debezium already does this, and a custom one invites "why not Debezium?"
- Live (non-quiesced) reconciliation, backfill under load, and dashboards. All three are v2.
- Multi-node Kafka, Flink HA, or cloud deploys. Everything runs in one `docker compose` stack.

## Success Criteria

1. `make up && make demo` on a clean laptop: the stack starts, the generator writes, data lands in Iceberg, and `recon run` exits `0`.
2. Each of the 4 drills runs end to end with `chaos run <name>` and ends verified (`recon run` exits `0`).
3. CI is green on every PR: lint, types, and unit tests, plus an integration job with drills 03 and 04.
4. The README has an architecture diagram, a decisions and tradeoffs section, a failure-modes table linked to `RUNBOOK.md`, and a "what changes at 100×" section.
5. The author can explain every line of the Flink SQL, the reconciler, and the drills without notes.

## Architecture

```text
generator ──writes──▶ Postgres 16 (wal_level=logical)
                          │  pgoutput slot
                          ▼
                 Kafka Connect + Debezium ──▶ Kafka (KRaft, 1 broker)
                                                 │ debezium-json
                                                 ▼
                                          Flink SQL (upsert)
                                                 │
                                                 ▼
                          Iceberg v2 tables ── REST catalog ── MinIO (S3)
                                                 ▲
reconciler ──reads PG (read-only role) + Iceberg (pyiceberg → DuckDB)
drills/    ──break things on purpose; RUNBOOK.md says how to detect and recover
```

| Component | Choice | Notes |
|---|---|---|
| Source | Postgres 16 | `wal_level=logical`, one publication, one slot owned by Debezium |
| CDC | Debezium Postgres connector on Kafka Connect | `plugin.name=pgoutput`; `decimal.handling.mode=string` so Flink's JSON format can read amounts |
| Transport | Kafka, KRaft, 1 broker | One topic per table |
| Processing | Flink SQL | Kafka source in `debezium-json` format → Iceberg sink with `PRIMARY KEY ... NOT ENFORCED` and `write.upsert.enabled = true` |
| Lake | Iceberg format v2 on MinIO | REST catalog; Flink commits a snapshot on each checkpoint |
| Query | pyiceberg + DuckDB | Used by the reconciler and for ad hoc reads |

Image and jar versions get pinned in the implementation plan.

## Source Schema

| Table | Key | Changes it produces |
|---|---|---|
| `users` | `user_id` | `kyc_tier` updates; deletes (GDPR) |
| `accounts` | `account_id` | `balance` updates |
| `transfers` | `transfer_id` | `status`: `pending → settled → reversed` |
| `ledger_entries` | `entry_id` | Append-only, double-entry: each transfer's legs sum to 0 |

Money is `NUMERIC(18,2)` in Postgres, `decimal(18,2)` in Iceberg, and `Decimal` in Python. Timestamps are `timestamptz`, in UTC.

The generator (plumbing) writes a steady mix of inserts, updates, and deletes at a configurable rate, and supports `--pause` / `--resume` so the reconciler can quiesce it.

## Ownership

| Author writes (core) | Coach writes (plumbing) |
|---|---|
| Flink SQL pipeline | `docker-compose.yml`, connector configs |
| Reconciler | Load generator |
| Drill scripts and `RUNBOOK.md` | Makefile, CI workflows |
| README decisions, tradeoffs, and 100× section | Stubs, reviews, mutation tests |

The author writes the unit tests for the reconciler and the drills; the coach mutation-tests them.

## Reconciler

### Fence: Wait Until the Lake Has Caught Up

Postgres is always ahead of the lake, so v1 compares only after quiescing:

1. Pause the generator and record `fence = pg_current_wal_lsn()`.
2. Wait until all three hold, each with a timeout:
   1. The Debezium slot's `confirmed_flush_lsn >= fence`.
   2. The Flink consumer group's Kafka lag is 0 on every table topic. Flink commits offsets to the group only on checkpoint, so the source sets a fixed `group.id`, and lag is read after a checkpoint.
   3. Each Iceberg table has a snapshot committed after step 2 held.
3. Compare. Then resume the generator, even if the compare fails.

A timeout exits `2` ("couldn't fence"), which is different from drift.

### Checks per Table

| Check | Catches |
|---|---|
| Row count | Gross loss or duplication |
| Key diff: `missing_in_lake`, `extra_in_lake` | Dropped inserts; deletes never applied |
| Row hash of the business columns, per key | Missed updates |
| Daily `SUM(amount)` as `Decimal` | Money drift |
| Lake-side invariant: legs sum to 0 per `transfer_id` | Partial transactions landing |

The compare logic is pure functions over iterables of typed rows. I/O (Postgres, Iceberg) sits behind two small reader functions passed in, so tests use plain lists.

### Interface

```bash
recon run --tables all --timeout 300 --report out.json
```

- A Rich table on stdout and a JSON report on disk
- Exit codes: `0` clean · `1` drift · `2` couldn't fence or errored
- Postgres access through a `recon_ro` role: `SELECT` on the four tables, plus reads of `pg_replication_slots` and `pg_current_wal_lsn()`. The plan confirms the minimum grants on Postgres 16
- Config through `pydantic-settings`; the password is a `SecretStr` and never logged

## Failure Drills

| # | Name | Inject | Detect | Recover |
|---|---|---|---|---|
| 01 | `wal-bloat` | Stop Kafka Connect and keep writing; variant with a small `max_slot_wal_keep_size` | Retained WAL from `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`; `wal_status = 'lost'` | Restart the connector; if the slot is lost, re-snapshot |
| 02 | `schema-change` | `ADD COLUMN memo`, then an incompatible type change | Flink job fails, or the column is missing in the lake | Evolve the Iceberg schema; restart Flink from a savepoint |
| 03 | `connector-kill` | `kill -9` Kafka Connect mid-stream | Duplicate events in Kafka | None needed: upsert by PK absorbs replays |
| 04 | `silent-delete-drop` | Run the sink in append mode, then delete users | Only the reconciler's `extra_in_lake` | Fix the sink mode; repair the affected keys |

### Drill Contract

```bash
chaos run <name> --dry-run   # print every step, touch nothing
chaos run <name>             # preflight → inject → observe → recover → verify
```

- **Preflight:** the stack is healthy and `recon run` is clean; otherwise exit `2` without injecting
- **Restore:** a `finally` undoes the injection even when a step fails, so a re-run is safe
- **Verify:** `recon run` exits `0`. Drill 04 must also show exit `1` before the fix
- **Exit codes:** `0` recovered and verified · `1` verify failed · `2` preflight failed

### `RUNBOOK.md`

One section per drill, always in this shape: **Symptom · Detect · Impact · Recover · Verify · Prevent**. Each section links to its drill script, and the README's failure-modes table links to each section.

## Repo Layout

```text
ledger-cdc-lake/
├── docker-compose.yml
├── Makefile                 # up, down, demo, check, recon, chaos
├── connect/                 # Debezium connector JSON
├── flink/pipeline.sql       # author
├── sql/schema.sql           # source schema, recon_ro role
├── src/ledger_cdc_lake/
│   ├── generator/           # coach
│   ├── recon/               # author
│   └── chaos/               # author
├── tests/                   # unit; integration/ needs the stack
├── RUNBOOK.md               # author
└── README.md
```

## CI

| Job | When | Runs |
|---|---|---|
| `check` | Every push | ruff, mypy strict, unit tests with 80% coverage, markdownlint |
| `integration` | Every PR | `compose up` → generator → `recon run` → drills 03 and 04 |
| `drill-wal-bloat` | Manual dispatch | Drill 01 (slow and disk-heavy) |

## Milestones

| # | Deliverable | Done when |
|---|---|---|
| M1 | Stack plus Flink SQL | All four tables land in Iceberg and update in place |
| M2 | Reconciler | `recon run` exits `0` on a clean stack and `1` on a hand-corrupted lake row |
| M3 | Drills 03 and 04 | Both verified locally and in CI |
| M4 | Drills 01 and 02, plus `RUNBOOK.md` | All four runbook sections written |
| M5 | README and publish | Success criteria 1–4 met; the repo is public on GitHub |

## v2 Backlog

- Live reconciliation with a grace window
- Hash-bucketed compare: aggregate by `hash(key) % 1024`, then drill into mismatched buckets only
- Backfill with a Debezium incremental snapshot under load
- Prometheus and Grafana: slot lag, retained WAL, consumer lag, Iceberg freshness
- Drill: a Flink TaskManager crash and checkpoint restore

## Risks

| Risk | Mitigation |
|---|---|
| Debezium's `decimal`, timestamp, and delete encodings don't match what Flink's `debezium-json` format expects | Spike it in M1 before writing any reconciler code; pin the converter settings |
| The stack is too heavy for a GitHub runner (about 7 GB of RAM) | Cap JVM heaps; one TaskManager; integration job runs on PRs only |
| Iceberg upserts write equality deletes, so reads slow down over time | Note it in the 100× section; compaction is v2 |
| Scope creep before anything ships | v1 is this spec; everything else goes in the v2 backlog |
