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
- `event_id` (UUID v4) is created once, by the producer. It is the Phase 2 dedupe key: a retried
  send keeps the same id, so duplicates are detectable.
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
