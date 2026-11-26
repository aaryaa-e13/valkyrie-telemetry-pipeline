# Project VALKYRIE

A fault-tolerant telemetry pipeline for a fictional game, built in phases as a data engineering
portfolio project. Planned flow: simulator -> Kafka -> Spark Structured Streaming -> Delta Lake
(bronze/silver/gold) -> dbt -> PostgreSQL -> dashboard.

**Status:** Phase 1 (ingestion) and Phase 2 (Delta, dedupe, watermark, idempotent MERGE) are
complete. Later phases are not built yet.

Design decisions and their tradeoffs are in [docs/DECISIONS.md](docs/DECISIONS.md). Problems hit
along the way, with cause and fix, are in [docs/WHAT_BROKE.md](docs/WHAT_BROKE.md). The run
outputs behind the Phase 2 numbers are in [docs/evidence/phase2/](docs/evidence/phase2/).

## Phase 1: ingestion

This section describes Phase 1 as tagged `phase-1`. Phase 2 (next section) replaced the raw
Parquet output with Delta tables; the commands below now run the Phase 2 job.

### What it does

```
simulator --> Kafka topic `game-events` --> Spark Structured Streaming --> raw Parquet
    |          (6 partitions, key = match_id)   (10 s micro-batches)       (by ingest_date)
    +--> run manifest (ground truth: which event_ids Kafka acknowledged, and at which offsets)
```

- **Simulator** (`simulator/`): generates matches (start, champion picks, kills, item purchases,
  end) as JSON events. Generation is deterministic: the same seed gives the same events and the
  same `event_id`s. It can send as fast as possible (`backfill`, the default) or at the pace of
  the wall clock (`realtime`), and can send a share of events deliberately late.
- **Kafka**: one broker in KRaft mode, started with Docker Compose.
- **Spark job** (`streaming/raw_to_parquet.py` in Phase 1): reads the topic, parses the JSON,
  and appends every message to Parquet. Messages that do not parse are kept and flagged, not
  dropped. In Phase 2 the same job is `streaming/bronze.py` and writes a Delta table.
- **Manifest**: each simulator run writes a JSON file listing the events the broker
  acknowledged. Every check below compares the pipeline's output with this file.

### How to run it

Requirements: Docker with Compose, GNU make, and [uv](https://docs.astral.sh/uv/) for the Python
3.11 virtual environment. Developed on WSL2 with about 10 GB of memory for Docker.

```bash
make venv     # once: create .venv with pinned dependencies
make up       # start Kafka and create the topic
```

| Command | What it does | Time in Phase 1 (16 GB WSL2 laptop) |
|---|---|---|
| `make demo` | Full run with explanations: about 1,250 events through Kafka and Spark, then verified against the manifest | about 40 s |
| `make smoke` | The same check with a tiny run (10-16 events) and one `SUMMARY` JSON line | about 30-45 s |
| `make smoke-restart` | Stops Spark gracefully, sends a run while it is down, restarts it, and checks nothing was lost or read twice | about 50 s |
| `make sim-realtime` | Tiny run sent at wall-clock pace, game time 10x faster | about 15 s |
| `make sim-late` | The same with 30% of events sent deliberately late | up to 2 min |
| `make timing-check MANIFEST=manifests/run_X.json` | Reads a run back from Kafka and compares each message's Kafka timestamp with its `event_time` | a few seconds |
| `make verify MANIFEST=manifests/run_X.json` | Compares raw Parquet with one run's manifest | about 20 s |
| `make test-unit`, `make test-spark`, `make lint` | Unit tests, Spark tests and linter | seconds |

`make help` lists every target. `make demo`, `make smoke` and `make smoke-restart` start the
Spark job themselves and use a fresh seed each time. Phase 2 times are in the next section.

### What is tested, and how (Phase 1)

Each claim is checked by a command you can run, not by reading logs.

| Claim | How it is checked |
|---|---|
| Every event Kafka acknowledged is in raw Parquet | `make smoke` / `make demo`: the set of `event_id`s in Parquet is compared with the manifest. The manifest only lists events the broker confirmed, and it is written by the simulator, so it does not depend on Kafka or Spark being correct. The check waits until Spark's committed offsets have passed the run's offsets, then requires 0 missing ids and 0 duplicate rows. |
| A graceful stop and restart loses nothing and re-reads nothing | `make smoke-restart`: the first batch after the restart must start exactly at the offsets where the job stopped, the rows read must equal the run's event count, and the Parquet row count (read from file footers, independently of Spark) must grow by exactly that count. Eight checks; a check that cannot be proven fails. |
| No event is sent before its own `event_time` | `make timing-check`: 0 events ahead (1 ms tolerance) in backfill, realtime and late runs. On a run from before pacing was added, the same check fails with every event ahead by up to 2 minutes. |
| Late events arrive out of order but are not lost | `make sim-late`, then `make timing-check` (reports out-of-order arrivals) and `make verify` (all ids present). |
| Old manifests stay readable | Manifest versions 1, 2 and 3 are all accepted; unknown versions are rejected. Real files of each version are committed under `tests/fixtures/manifests/` and tested. |
| Event identity is stable | The `event_id`s of two seeds are pinned in a unit test; pacing and lateness settings cannot change them. |

### Known limits

- **Single broker, replication factor 1.** A lost broker disk loses data. `acks=all` only means
  "the one broker has it". This is a laptop setup, not a durable cluster.
- **The raw layer keeps duplicates.** If the same event is sent twice (for example the same
  seed run twice), both copies are stored. Spark's checkpoint gives exactly-once *processing of
  Kafka offsets*; it knows nothing about two messages that carry the same event. Phase 1 is
  therefore at-least-once per event. Phase 2 is planned to make the pipeline effectively-once
  via idempotent sinks (Delta `MERGE` on `event_id`). Nothing here is claimed to be exactly-once.
  *Phase 2:* silver keeps one row per `event_id` (D2.3); bronze still keeps every message, on
  purpose.
- **Only a graceful stop is tested.** `kill -9`, a crash in the middle of a batch, a Kafka
  restart and a recreated topic are not tested yet (planned for the Phase 4 chaos tests).
  *Phase 2:* a SIGKILL at each of the four silver write boundaries and once inside bronze is
  tested (D2.6). Kafka restarts and a recreated topic are still not tested.
- **Recreated topics are only partly detected.** The checks record the topic name, not Kafka's
  topic ID, so a topic that was recreated and has already grown past the old offsets would not
  be noticed (D1.16).
- **The end-to-end checks assume a single writer.** Another producer writing to the topic during
  a run makes the check stop with an error instead of giving a result (D1.16).
- **Spark's status file is not a heartbeat.** Spark 4.1.4 reports nothing while idle, so an old
  timestamp does not mean the job is unhealthy (D1.16).
- **Many small files.** A 10-second trigger writes small Parquet files; there is no compaction
  yet (D1.8). *Phase 2:* still true for the Delta tables.
- **No watermark, no late-data handling, no dead-letter queue yet.** The simulator can already
  produce late events for Phase 2 to handle. Its delays assume a 30-second watermark, which is a
  placeholder. Late events sent in `backfill` mode only test ordering; tests of dropping late
  data need `realtime` mode (D1.17). *Phase 2:* a watermark (W = 60 s) and a late table
  (D2.4); the dead-letter queue is Phase 3.
- **Small scale.** Runs so far are tens to about 1,250 events. The simulator holds a whole run
  in memory. No benchmark has been run.
- **JSON events.** Avro and a schema registry are planned for Phase 5.

## Phase 2: Delta, dedupe, watermark, idempotent MERGE

### What it does

```
Kafka --> bronze   Delta, append-only, one row per Kafka message
            |
            +--> silver   Delta, one row per event_id (on-time rows)
            +--> late     Delta, rows at or behind the watermark: kept, not dropped
            +--> control  Delta, one row per silver micro-batch
```

One Spark process runs two streaming queries, each with a 10 s trigger.

- **Bronze** (`streaming/bronze.py`): the Phase 1 job, now writing an append-only Delta table.
  Each Delta commit records the query id and the batch id (`txn`), so a batch that Spark runs
  again after a crash is skipped instead of appended twice (D2.1, D2.2).
- **Silver** (`streaming/silver.py`): reads bronze as a stream. In each micro-batch it sets
  aside rows without a usable key (counted; they stay in bronze), keeps the first copy of each
  `event_id`, and writes with an insert-only `MERGE` on `event_id`: a row is inserted only if
  its id is not in the table yet. A batch that runs again, or a full re-read of bronze, adds
  nothing (D2.3).
- **Watermark** (`streaming/watermark.py`), W = 60 s, computed in the job's own code:
  wm(N) = the largest event time of the earlier batches (capped at its Kafka time) minus W. A
  row with `event_time <= wm(N)` goes to the **late** table instead of silver. Nothing is
  dropped, and silver and late never share an id (D2.4).
- **Control table**: one row per silver batch with its counts and the watermark it used,
  written once per batch (Delta `txn`). At start-up, a guard compares bronze's table and the
  control table with Spark's checkpoints and refuses to start on a mismatch (D2.2, D2.3).
- **verify** (`make verify`): compares bronze and silver ∪ late with a run's manifest, checks
  the late split, and re-checks the job's watermark bookkeeping (rules R0-R7, D2.4, D2.5).

### How to run it

`make up` first; the e2e targets start Spark themselves and use a fresh seed each time.

| Command | What it does | Writes to Kafka | Time (this laptop) |
|---|---|---|---|
| `make smoke` | Tiny run; verifies bronze and silver ∪ late; one `SUMMARY` JSON line | 10-16 events | 2.3-5.7 min |
| `make smoke-dupes` | Sends the same seed twice: bronze keeps 2 rows per id, silver 1 (the first send's) | 2 x 10-16 events | 2.6 min |
| `make smoke-restart` | Graceful stop, a run sent while Spark is down, restart: nothing re-read, skipped or stored twice | 10-16 events | 3.2-6.7 min |
| `make smoke-late` | Realtime run (4 matches, 30% of events late), judged by R1-R6 | about 250 events | 7.4-9.0 min |
| `make smoke-faults` | SIGKILL at each silver write boundary and once inside bronze, then restart and judge; `E2E_ARGS="--point after_late"` runs one point | about 50 events per silver point, 14 per bronze attempt | about 10 min per silver point, 5 min for bronze |
| `make verify MANIFEST=... [LAYER=bronze\|silver\|all]` | Read-only check of one run against its manifest | nothing | not measured separately |
| `make timing-check MANIFEST=...` | Reads a run back from Kafka: was any event sent before its `event_time`? | nothing | seconds |
| `make faults-config-check` | Counts mentions of the fault hooks in the default Compose config (must print 0) | nothing | seconds |
| `make delta-compat` | Delta compatibility gate, checks C1-C14 | nothing | about 4 min |
| `make test-unit`, `make test-spark`, `make lint` | 519 unit tests; 111 Spark tests in the Spark image; ruff | nothing | seconds; 28-47 min; seconds |

Every e2e target ends with a `SUMMARY {json}` line and exits 0 on PASS (exit codes: D1.16,
D2.5, D2.6). Times are from single runs on the WSL2 laptop, not benchmarks. They vary between
runs: for `make smoke`, `make smoke-restart` and `make test-spark` the upper end is from the
final Phase 2 gate, run after `make down && make up` (so smoke also waited for Spark to start);
about 4 of its 5.7 smoke minutes were the final verify (`docs/evidence/phase2/gate/`).

### What is tested, and how

| Claim | Tested by | Result |
|---|---|---|
| Every event Kafka acknowledged for a run is in bronze, and no Kafka message is stored twice | Every e2e run: verify against the manifest (0 missing ids) and R0 over the whole bronze table (0 duplicate `(topic, partition, offset)`) | PASS in every run in `docs/evidence/phase2/` |
| Silver ∪ late holds one copy of every acknowledged `event_id` of a run: none missing, none twice | Every e2e run: verify R1-R3. `make smoke-dupes` sends a seed twice; silver keeps the first send's copy | PASS in every run in `docs/evidence/phase2/` |
| Only very late events go to the late table | `make smoke-late`, 3 runs: R4 (late ids are all very late) and R5 (every on-time or slightly late event is in silver). 13 of 16, 14 of 14 and 13 of 17 very late events were routed, 0 slightly late (D2.5) | PASS in all 3 |
| The job follows its own watermark rule | R6a-d over every control-table batch written since slice 4, in every verify. Spark tests tamper with a copy of the tables so that each rule fails on purpose, and assert exactly which rules fail (D2.5) | PASS |
| A batch that runs again after a crash adds nothing and leaves one control row | Spark tests: a crash at each of the 4 silver write boundaries, then the re-run equals a crash-free run (D2.4). `make smoke-faults`: one SIGKILL of the whole container at each of the 4 boundaries, with the row counts, the start-up guard's state and the control row checked before and after (D2.6) | PASS, one kill per boundary |
| A SIGKILL after bronze planned a batch, before its Delta commit, loses nothing and stores nothing twice | `make smoke-faults`, inside_bronze: the first attempt hit that window; after the restart the Delta log added exactly the run's 14 rows (D2.6) | PASS once. A kill after bronze's Delta commit but before Spark's checkpoint commit was not hit live: Spark tests cover it (D2.2), and silver's `after_control` kill is the same situation for silver |
| A graceful stop and restart re-reads nothing and skips nothing | `make smoke-restart`: 11 checks, including the Delta-log row count against Spark's own count and the `txn` ids (D2.2, D2.3) | PASS |
| Start-up refuses a checkpoint that does not match its table | Spark tests create each mismatch for real: a deleted, replaced or restored table, a reset checkpoint (D2.2, D2.3) | PASS |
| The fault hooks are off in normal runs | Unit tests (inert: no env read, no marker folder), Compose isolation tests, `make faults-config-check` prints 0, and the full Spark suite ran with the hooks in place (D2.6) | PASS |
| Delta 4.4.1 works on this stack | `make delta-compat`, 14 checks (D2.1) | PASS |

**Effectively-once, not exactly-once.** Spark's checkpoint records which Kafka offsets each
micro-batch covers, so after a crash it runs the same batch again over the same offsets: that
is exactly-once *processing of Kafka offsets*. It does not stop duplicate rows: the same event
can arrive as two Kafka messages, and a batch that runs again repeats its writes. Phase 2
makes the stored result **effectively-once**: every event has a stable `event_id`, bronze skips
a batch it already committed (Delta `txn`), silver and late insert an id only if it is not
there yet, and the control row is written once per batch. The table above shows this for the
cases it lists (graceful restarts, duplicate sends, a SIGKILL at the four silver write
boundaries and once inside bronze), not for every possible failure.

### Known limits (Phase 2)

- **R4/R5 are often "not checkable".** They are judged only when no other run's rows are
  within W of the run. Back-to-back smokes and all fault runs are not isolated like that, and
  verify then prints "not checkable" instead of a verdict (D2.5). The three smoke-late runs were
  checkable.
- **Faults are injected at chosen points.** One kill at each silver write boundary and one
  inside bronze, each on a tiny run. Not tested: kills at random instants or inside one file
  write, Kafka restarts, a recreated topic, a full disk, network partitions, clock jumps,
  losing the broker's data (D2.6).
- **A silver failure also stops bronze.** Both queries run in one process; Kafka's 7-day
  retention is the buffer until the job is restarted (D2.3).
- **Batches can take longer than the 10 s trigger.** In smoke-late run 2, 9 of 25 silver
  batches took longer (the slowest 15.3 s). The first silver batch after a Spark start took
  22-23 s in the final gate. The next batch then starts later; nothing is lost (D2.4, D2.5).
- **Memory is close to the limit.** The Spark container peaked at 2.59-2.67 GiB in the
  smoke-late runs, with a 3 GiB limit. Above 2.8 GiB, smoke-late prints a `STOP (memory)`
  line and smoke-faults fails the run (D2.5, D2.6).
- **Dedupe horizon.** Two copies of one id more than 7 days apart (by `event_time`) are both
  inserted; verify reports the largest spread and says "not checkable" past 7 days (D2.3).
- **Empty leftover files.** The late and control tables collect 0-row data files that no Delta
  commit lists. Why is not known; they hold no rows and are never deleted (D2.6).
- **A recreated topic** makes R0 report false duplicates, because bronze does not store
  Kafka's topic id (D2.2).
- **Small scale.** Runs are tens to about 260 events. No benchmark yet (Phase 10).
- The Phase 1 limits on a single broker, a single writer and the status file still apply.

## Real-data check (research, not part of the pipeline)

Before the Phase 3 quality gates, the simulator's events were compared with 50 real ranked
matches from a public game API (match summary + timeline per match): missing fields, nulls,
odd values, timestamp encoding, event types with no simulator equivalent, and the reverse.
The game, champions and simulator of VALKYRIE stay fictional. Code, method and the findings
(counts and field paths only, no ids or names) are in
[research/real_match_data/](research/real_match_data/), see its
[FINDINGS.md](research/real_match_data/FINDINGS.md). Raw data is never committed. The sample
is small and not representative: recent games of top-ladder players on one server.

VALKYRIE isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games, and all associated properties are trademarks or registered trademarks of Riot Games, Inc.

## Repository layout

| Path | Contents |
|---|---|
| `simulator/` | Event generator, send planning (pacing, lateness), Kafka producer, manifest writer |
| `streaming/` | Spark jobs (bronze, silver), watermark and oracle rules, start-up guards, verify, test-only fault hooks |
| `common/` | Manifest reader, Delta log reader and logging, shared by the laptop and the containers |
| `e2e/` | Orchestrators for `demo`, `smoke`, `smoke-dupes`, `smoke-restart`, `smoke-late`, `smoke-faults`, and the timing check |
| `bench/` | Timing probes used for D2.4 |
| `tests/` | Unit tests, Spark tests, manifest fixtures |
| `research/` | One-off research outside the pipeline: real match data vs the simulator (counts only) |
| `docker/`, `docker-compose.yml` | Images and services (Compose profiles keep memory use low) |
| `docker-compose.faults.yml` | Test only: turns the fault hooks on; used only by `make smoke-faults` |
| `requirements/` | Pinned Python dependencies: laptop (`dev.txt`), simulator image, Spark image |
| `docs/` | `DECISIONS.md`, `WHAT_BROKE.md`, `evidence/` |
