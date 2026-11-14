# Architecture Decisions

Short decision records: context, decision, tradeoffs. Newest phase last.

---

## Phase 1: Simulator -> Kafka -> Spark -> Parquet

### D1.1 Pin Spark 4.1.4, not the newest Spark
- **Decision:** Spark/PySpark 4.1.4, `spark-sql-kafka-0-10_2.13:4.1.4`, Java 17, Python 3.11, Scala 2.13.
- **Why:** Phase 2 needs Delta Lake. Delta's official compatibility table (checked 2026-11-14)
  lists Spark 4.0/4.1 only. Delta 4.4.x adds Spark 4.2, and no Delta release supports Spark 4.3 yet.
  4.1.x is the newest line that every source agrees on.
- **Tradeoff:** We miss 4.2/4.3 features, but we avoid a forced Spark downgrade in Phase 2.

### D1.2 Kafka in KRaft mode, single broker, `apache/kafka:4.3.2`
- **Decision:** One node acts as both broker and controller. It has no ZooKeeper and uses a fixed
  `CLUSTER_ID` with a named volume for data.
- **Why:** KRaft is the only mode in Kafka 4.x. One broker fits the 10 GB laptop budget.
  4.3.2 is a patch release on a mature, still-supported line. 4.4.0 is a brand-new .0 release.
- **Tradeoff:** Replication factor 1 means losing the broker's disk loses data. A broker
  *restart* is safe because data persists on the `kafka-data` volume. In production you would
  run 3+ brokers with RF=3 and `min.insync.replicas=2`.

### D1.3 Explicit topic creation; auto-create disabled
- **Decision:** `auto.create.topics.enable=false`. The one-shot `kafka-init` container creates
  `game-events` and then checks the partition count, failing loudly on a mismatch.
- **Why:** With auto-create, a typo in a topic name quietly creates a new topic with default
  settings. `--if-not-exists` also never fixes an existing topic, so we verify it.

### D1.4 Topic `game-events`: 6 partitions, 7-day retention
- **Why 6 partitions:** A partition is Kafka's unit of parallelism and ordering. Spark creates
  one read task per partition. With `local[4]`, 6 partitions keep the cores busy without the
  overhead of many tiny partitions.
- **Why 7 days:** This keeps enough history to replay into Spark (Phase 3) after a bug fix.
- **Tradeoff:** Increasing the partition count later changes which partition a key maps to, which
  breaks per-key ordering across the change. So we pick a sensible number up front.

### D1.5 Kafka message key = `match_id`
- **Why:** Kafka orders messages only within a partition, and the same key always goes to the
  same partition. So all events of one match stay in order and land together.
- **Alternatives:** `player_id` keeps per-player order (useful for anti-cheat) but splits a match
  across partitions. No key gives perfect balance but no ordering.
- **Risk:** Hot partitions are a risk only if a few keys dominate. Matches are many and similar in
  size, so the risk is low.

### D1.6 Event envelope with `event_id` and `match_seq`
- `event_id` is a **UUID v5 derived from `match_id:match_seq`** (fixed namespace). This changed
  from the planned random UUID v4 so that generation is deterministic: the same seed gives the same
  ids, and a logical event always has the same id however often it is resent or regenerated. It
  is the Phase 2 dedupe key. Consequence: re-running the simulator with the same seed sends
  *duplicates* on purpose, so use a new seed for genuinely new data.
- Determinism: one `random.Random(seed)` drives all choices in a fixed order. `event_time` starts
  at `start_time` (default: now, so Phase 2 watermarks don't treat the data as ancient). Pass
  `--start-time` for byte-identical runs. A golden unit test guards against accidental changes.
- `match_seq` is a per-match sequence number starting at 1, with no gaps. It lets us (a) detect
  missing events (a gap), (b) detect duplicates (a repeated seq), and (c) restore the true order
  after shuffling, independent of clock timestamps. Two events in one match can share an
  `event_time`, but they never share a `match_seq`.
- Three timestamps: `event_time` (when it happened in the game, which can be late),
  `kafka_timestamp` (when it reached the broker), and `ingest_time` (when Spark processed it).
  Lateness = ingest_time - event_time. This drives Phase 2 watermarks.
- `patch_version` is stored on every event because the patch at play time is a fact about the
  event and cannot be looked up correctly later (patches change). It feeds the SCD2 table in Phase 6.

### D1.7 Simulator writes a ground-truth manifest per run
- **Decision:** Each run writes a manifest file. It contains per-match event counts plus the full
  list of unique `event_id`s and an order-independent checksum of them.
- **Why:** Phase 4's chaos tests must prove "unique events in == rows out". The manifest is
  ground truth that does not depend on Kafka or Spark being correct.
- **Tradeoff:** Listing every id costs disk (about 37 bytes per event, so roughly 1.8 GB at 50M
  events). The checksum is tiny but only says *whether* sets differ, not *which* ids. We will
  store both, and the id list can be switched off for very large benchmark runs.
- **Only broker-acknowledged events count as delivered.** The manifest is written after
  `flush()`, from the delivery callbacks. Failed ids are listed separately, so ground truth never
  includes an event Kafka didn't confirm. It is written atomically (temp file + rename).

### D1.7b Producer settings
- `acks=all` + `enable.idempotence=true`: the broker discards duplicates caused by the producer's
  *internal* retries. This does not cover the simulator being re-run or crashing mid-run, which is
  why downstream dedupe on `event_id` is still needed.
- `partitioner=murmur2_random`: librdkafka (the Python client) defaults to a CRC32-based
  partitioner, while Java clients use murmur2. Without this, the same `match_id` would land on
  different partitions depending on which client produced it. Verified: the Java console
  producer put both test keys on the same partitions (3 and 5) as the Python simulator.
- `linger.ms=20` + lz4 compression: small batching delay for better throughput.
  `delivery.timeout.ms=120000`: a message that can't be delivered in 2 min is reported as failed.
- Preflight `list_topics` check: fail fast with exit code 2 if Kafka or the topic is missing,
  instead of waiting for delivery timeouts.

### D1.7c Known limitation: burst mode sends events from the future (found in slice b)
- **Observation:** The generator builds the whole match timeline up front
  (`event_time = start_time + game offset`), and the simulator then sends every event
  immediately. So `event_time` is simulated game time running *ahead* of wall-clock time.
  Measured on the topic on 2026-11-14 (tiny config): 36/36 events had `event_time` later than
  their Kafka `CreateTime`, up to +124.5 s (a `match_end` sent two minutes before it
  "happened"). With the default config (20-40 min matches, staggered starts) the gap would be
  tens of minutes.
- **Why it matters for Phase 2:**
  - The watermark is `max(event_time seen) - allowed delay`. Future timestamps push it ahead of
    real time, so normal events can be wrongly treated as too late and dropped.
  - Lateness (`ingest_time - event_time`) becomes negative, which makes latency and freshness
    metrics meaningless.
  - Late and out-of-order data become impossible to test on purpose, because the "lateness" is
    an accident of how events are sent.
- **Acceptable for Phase 1:** The raw Parquet job just appends and doesn't use event time.
- **Decision:** Add a pacing mode before Phase 2 watermarks (plan in D1.13).

### D1.8 Small-files tradeoff of the 10-second trigger
- **Decision:** The Spark job uses `trigger(processingTime="10 seconds")` and
  `maxOffsetsPerTrigger=50000`.
- **The problem:** Every micro-batch writes at least one new Parquet file per output partition
  touched, per write task. At one batch every 10 s that means at least 8,640 batches per day. Each
  can write several files, many of them only a few KB. Lots of small files hurt because:
  - each file has fixed overhead (open, footer and metadata read, listing), so reads slow down;
  - Parquet compression and column statistics work best on large files (128 MB-1 GB is the
    usual target);
  - the file listing and Spark's `_spark_metadata` log grow without limit.
- **Why accept it in Phase 1:** A short trigger gives low latency (data is queryable within
  about 10 s) and makes the demo easy to observe. Event volume on a laptop is small.
- **Mitigations:** (1) Phase 2 moves to Delta, which supports `OPTIMIZE` compaction.
  (2) A scheduled compaction job runs in Airflow (Phase 7). (3) `coalesce` within a batch limits
  files per batch. (4) For benchmarks, a longer trigger (for example 1 min) trades latency for
  fewer, larger files.
- **Interview line:** "Trigger interval is a latency-vs-file-size dial. Streaming sinks always
  need a compaction story."

### D1.9 Raw Parquet partitioned by `ingest_date`
- **Why:** In the raw layer, late events always land in today's folder and old folders never
  change. That makes reprocessing ("redo yesterday's arrivals") and compaction simple.
  Partitioning by `event_date` moves to the silver layer.
- **Why not also by `event_type`:** That multiplies folders and makes the small-files problem
  (D1.8) worse.
- **Tradeoff:** A query by event date may scan two ingest folders, because late events can
  arrive the next day.

### D1.10 Spark in its own container, local mode, hard memory limits
- **Decision:** `local[4]`, `spark.driver.memory=2g`, container `mem_limit: 3g`,
  `spark.sql.shuffle.partitions=8`. A custom image (python:3.11-slim + OpenJDK 17 + pyspark 4.1.4)
  pins Python to exactly 3.11.
- **Why:** Without hard limits, Spark can exhaust WSL2 memory and freeze the machine. The 1 GB
  headroom covers JVM off-heap memory and Python worker processes.
- **Budget:** Kafka 1 GB + Spark 3 GB + simulator 256 MB is about 4.3 GB of the 10 GB available.

### D1.11 Compose profiles
- Kafka (and `kafka-init`) are *core* services with no profile. Spark (`spark`) and the simulator
  (`sim`) each get a profile, so only what is needed runs.

### D1.12 Raw job details (slice c)
- **Connector JARs are baked into the image at build time** (`docker/spark/fetch_kafka_jars.sh`).
  It resolves `spark-sql-kafka-0-10_2.13:4.1.4`, adds `kafka-clients-3.9.1`, `commons-pool2-2.12.1`
  and the token provider, and skips JARs Spark already ships (lz4, snappy, slf4j, hadoop), so a
  library is never on the classpath in two versions. The container needs no internet at runtime.
  The alternative, `--packages` at startup, downloads on every fresh container and fails offline.
- **Connector vs broker:** `kafka-clients` 3.9.1 with broker 4.3.2 is verified working. Kafka 4.x
  dropped support only for clients older than 2.1.
- **`failOnDataLoss=true`:** if retention deletes offsets we have not read yet, the job stops
  instead of silently skipping data. Losing data should be loud.
- **`startingOffsets=earliest`** applies only to the very first run. After that the checkpoint
  decides where to start. Changing the option later has no effect, a common surprise.
- **Parse failures never fail a batch.** `from_json` runs in PERMISSIVE mode, and timestamps use
  `try_to_timestamp`, because Spark 4 has ANSI mode on by default, where `to_timestamp` throws on
  bad input. `parse_ok=false` and `raw_value` is kept. A real dead-letter queue comes in Phase 3.
- **`ingest_time = current_timestamp()`** is one value per micro-batch, not per row. The session
  time zone is forced to UTC so `ingest_date` doesn't depend on the container's time zone.
- **Started with plain `python -m`, not `spark-submit`:** Python is then PID 1 (via `init: true`)
  and receives SIGTERM, so it can stop the query cleanly (exit 0, "query stopped").
  `spark.driver.memory` still applies, because PySpark passes builder configs to the JVM launcher.
- **Measured (2026-11-14, tiny data):** Spark 626 MiB of 3 GiB, Kafka 523 MiB of 1 GiB.
- **Small files observed:** the first batch wrote 5 Parquet files for 37 rows (one per Kafka
  partition with data). See D1.8.

### D1.14 Phase 1 produces duplicate rows. This is why Phase 2 exists.
- **Shown in slice (c):** seed 501 was sent twice (two producer sessions, identical `event_id`s).
  The raw job wrote both copies. `verify_manifest` against the first seed-501 manifest:
  13 expected ids, 13 found, **26 rows, 13 duplicate rows** (every id twice). That is PASS for
  "nothing lost" but **FAIL with `--fail-on-duplicates`**.
- **Why nothing upstream prevented it:**
  - Producer idempotence removes duplicates only from one producer session's internal retries.
    A re-run, a crashed and restarted simulator, or a client that retries at the application
    level creates a new session.
  - Spark's checkpoint plus the file sink prevent duplicate *files* from a re-run *batch*
    (exactly-once *processing* of Kafka offsets). They know nothing about two different Kafka
    messages that carry the same event.
- **Consequence:** the raw layer is "at-least-once per event". Counting rows would double-count.
- **Phase 2 fix:** Delta Lake + `MERGE` on `event_id` (idempotent upsert) and streaming dedupe
  with a watermark. The Phase 2 bar is `verify_manifest --fail-on-duplicates` = PASS after
  deliberately sending duplicates.
- **How to say it:** "Kafka plus Spark checkpoints give exactly-once *processing* of offsets.
  End-to-end *effectively-once* needs a stable event id and an idempotent sink."

### D1.13 Plan: simulator pacing modes + deliberate lateness (not built yet; before Phase 2)
Rule: **the event content stays deterministic; only *when* it is sent changes.** The generator
stays a pure function that builds the timeline. A new *scheduler* layer decides send times.

- **`pace: realtime`** (new default for streaming demos):
  - Each event is sent when the wall clock reaches its `event_time`. The simulator sleeps until
    then, so `event_time ≈ send time` (within milliseconds).
  - A `time_scale` factor (e.g. 30 = 30x faster) shrinks the in-game time gaps *before*
    `event_time` is stamped, so `event_time` still matches the real send time. `duration_s`
    keeps the game value, because it is "game clock" data, not a timestamp.
  - Throughput comes from many matches overlapping (`match_interval_s`), not from bursts.
- **`pace: backfill`** (replaces today's burst behaviour, for bulk loads and benchmarks):
  - Sends as fast as possible, but the timeline is placed so its last event is at or before
    *now* (`start_time = now - total span`). Every event is then in the past, like replaying
    history. No more timestamps from the future.
- **Deliberate lateness (`late_fraction`, `late_delay_s`):**
  - A *separate* seeded RNG (`Random(seed, "lateness")`) picks which events are late and by how
    much. Turning lateness on or off doesn't change any event or event_id, only the send order.
  - A late event keeps its `event_time`, but its send time becomes `event_time + delay`. The
    scheduler is a priority queue ordered by send time, so late events really do arrive after
    newer ones (out of order within the partition). `match_seq` lets us detect and restore the
    true order.
  - The delay distribution is chosen to straddle the planned Phase 2 watermark (e.g. most events
    5-60 s late, plus a small tail of about 10 min). That way the tests cover "late but accepted"
    and "too late" cases.
- **Manifest additions:** pacing mode, `time_scale`, and for each late event its planned delay.
  Phase 2 tests can then assert exactly which events must appear (delay within the watermark)
  and which are expected to be dropped or routed elsewhere.
- **Guardrail:** In realtime mode, a check reports any event sent more than a small tolerance
  *before* its `event_time`. In Phase 3, Spark will also flag `event_time > ingest_time + 5 min`
  as a data-quality issue (clock skew or a bad client).
- **Out of scope here:** Injecting duplicates and shuffling order belongs to Phase 4. Those will
  reuse the same scheduler.
