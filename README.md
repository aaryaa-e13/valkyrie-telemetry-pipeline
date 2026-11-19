# Project VALKYRIE

A fault-tolerant telemetry pipeline for a fictional game, built in phases as a data engineering
portfolio project. Planned flow: simulator -> Kafka -> Spark Structured Streaming -> Delta Lake
(bronze/silver/gold) -> dbt -> PostgreSQL -> dashboard.

**Status:** Phase 1 (ingestion) is complete. Later phases are not built yet.

Design decisions and their tradeoffs are in [docs/DECISIONS.md](docs/DECISIONS.md). Problems hit
along the way, with cause and fix, are in [docs/WHAT_BROKE.md](docs/WHAT_BROKE.md).

## Phase 1: ingestion

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
  dropped. Since Phase 2 slice 2 the same job is `streaming/bronze.py` and writes an
  append-only Delta table instead (DECISIONS D2.2). Since slice 3 a second query in the
  same process writes **silver**, one row per `event_id` (`streaming/silver.py`, D2.3). This
  section describes Phase 1 (tag `phase-1`).
- **Manifest**: each simulator run writes a JSON file listing the events the broker
  acknowledged. Every check below compares the pipeline's output with this file.

### How to run it

Requirements: Docker with Compose, GNU make, and [uv](https://docs.astral.sh/uv/) for the Python
3.11 virtual environment. Developed on WSL2 with about 10 GB of memory for Docker.

```bash
make venv     # once: create .venv with pinned dependencies
make up       # start Kafka and create the topic
```

| Command | What it does | Time (16 GB WSL2 laptop) |
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
Spark job themselves and use a fresh seed each time.

### What is proven, and how

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
- **Only a graceful stop is tested.** `kill -9`, a crash in the middle of a batch, a Kafka
  restart and a recreated topic are not tested yet (planned for the Phase 4 chaos tests).
- **Recreated topics are only partly detected.** The checks record the topic name, not Kafka's
  topic ID, so a topic that was recreated and has already grown past the old offsets would not
  be noticed (D1.16).
- **The end-to-end checks assume a single writer.** Another producer writing to the topic during
  a run makes the check stop with an error instead of giving a result (D1.16).
- **Spark's status file is not a heartbeat.** Spark 4.1.4 reports nothing while idle, so an old
  timestamp does not mean the job is unhealthy (D1.16).
- **Many small files.** A 10-second trigger writes small Parquet files; there is no compaction
  yet (D1.8).
- **No watermark, no late-data handling, no dead-letter queue yet.** The simulator can already
  produce late events for Phase 2 to handle. Its delays assume a 30-second watermark, which is a
  placeholder. Late events sent in `backfill` mode only test ordering; tests of dropping late
  data need `realtime` mode (D1.17).
- **Small scale.** Runs so far are tens to about 1,250 events. The simulator holds a whole run
  in memory. No benchmark has been run.
- **JSON events.** Avro and a schema registry are planned for Phase 5.

### Repository layout

| Path | Contents |
|---|---|
| `simulator/` | Event generator, send planning (pacing, lateness), Kafka producer, manifest writer |
| `streaming/` | Spark job, schemas, status file, manifest verification |
| `common/` | Manifest reader and logging shared by the laptop and the containers |
| `e2e/` | Orchestrators for `demo`, `smoke`, `smoke-restart`, and the timing check |
| `tests/` | Unit tests, Spark tests, manifest fixtures |
| `docker/`, `docker-compose.yml` | Images and services (Compose profiles keep memory use low) |
| `docs/` | `DECISIONS.md`, `WHAT_BROKE.md` |
