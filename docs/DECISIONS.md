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
  `--start-time` for byte-identical runs. *(Updated in slice e: by default the timeline
  now ENDS at "now" instead of starting there, see D1.17. Content and ids are unchanged.)* A golden unit test guards against accidental changes.
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
- **Status: RESOLVED in slice e (2026-11-15), see D1.17.** The default pace is now `backfill`,
  which places the timeline so that it *ends* now; `realtime` sends each event at its
  `event_time`. Measured with `make timing-check` (Kafka `CreateTime` vs `event_time`): 0 events
  ahead in backfill, realtime and realtime-with-lateness runs; realtime events matched to 0 ms.
  The same check run on a manifest from before the fix reports the old bug. The text below is
  kept as the record of the problem.
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
*Phase 2 slice 2: the raw layer is now the bronze Delta table (D2.2). The `ingest_date`
partitioning and the reasons below are unchanged.*
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
*Phase 2 slice 2: the job is now `streaming/bronze.py` and writes Delta (D2.2). Parsing,
`ingest_time`/`ingest_date`, `failOnDataLoss`, `startingOffsets` and the SIGTERM handling below
are unchanged.*
- **Connector JARs are baked into the image at build time** (`docker/spark/fetch_kafka_jars.sh`,
  renamed `fetch_jars.sh` in Phase 2; jar set, exclusions and SHA-256 lock: D2.1).
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
- **Fixed in Phase 2 slice 3 (D2.3):** silver keeps one row per `event_id`. Seed 501: bronze
  still has 26 rows (by design), silver has 13, each the first send; verify `--layer all`
  PASS. `make smoke-dupes` repeats the test with a fresh seed on every run.
- *Updated in Phase 2 slice 7:* the "Phase 2 fix" above is not what was built. Phase 2 uses
  no Spark streaming dedupe (`dropDuplicates*` drop rows behind the watermark silently, see the
  D1.17 note). It dedupes inside each micro-batch and across batches with an insert-only
  `MERGE` (insert if absent, not an upsert, D2.3), and routes rows behind its own watermark to
  a late table instead of dropping them (D2.4).

### D1.15 End-to-end checks: how we know Spark has processed a run (slice d)
Built in steps: d1 = manifest offsets + status file (this entry); d2/d3 add the orchestrator.

**Finding: the progress event fires only after the batch is committed (Spark 4.1.4).**
- *Source* (tag `v4.1.4`, `MicroBatchExecution.executeOneBatch`): `runBatch`, then
  `sink.addBatch`. For the Parquet sink, `FileStreamSink.addBatch` then `FileFormatWriter.write`
  then `ManifestFileCommitProtocol.commitJob` then `fileLog.add(batchId)`, so
  **`_spark_metadata/N`** is written before `addBatch` returns. Next, `markMicroBatchEnd` writes
  `commitLog.add`, which is **`commits/N`**. Only then does `finishTrigger` call
  `updateProgress`, which posts the `QueryProgressEvent`. The listener bus delivers events
  asynchronously, which can only make it later still.
- *Experiment* (2026-11-14, `rate` source to Parquet, 1 s trigger, listener checks files when
  the event arrives): **29 batches, 0 violations.** Both files existed every time.
- *Gotcha found by the experiment:* every 10th batch (default `compactInterval=10`) the sink log
  entry is `_spark_metadata/9.compact`, not `9`. A tool that looks for plain `N` files would
  wrongly conclude "not committed". That is one more reason not to parse Spark's internal
  files, and to read Parquet only through Spark.
- *Decision:* the progress event is a safe "batch durable" signal, so we use it. Fallback if a
  future Spark version changes the order: poll the checkpoint's `commits/N`.

**Status file** (`data/status/raw_to_parquet.json`, written by the job's progress listener).
- Contents: `query_id`, `last_batch` (batch id, run id, row count, start and end offsets), and
  the last 100 batches. It is written atomically (temp file + rename), so readers never see a
  partial file. A write error only logs a warning and never stops the streaming query.
- Why our own file instead of parsing `offsets/N` and `commits/N`: the checkpoint layout is
  internal and undocumented (see the `.compact` gotcha). Our file is small, documented and
  unit-tested. Cost: one more moving part in the job.
- History survives restarts if `query_id` is unchanged (it is stored in the checkpoint). A new
  `query_id` means the checkpoint was reset, so history starts fresh rather than mixing in
  offsets from a different query.

**Manifest v2** adds `kafka_offsets {partition: {min, max, count}}` from the delivery callbacks
(acknowledged messages only).
- **Catch-up rule:** a run is processed when, for every partition it wrote to,
  `committed end_offset > max`. Spark end offsets are exclusive ("next offset to read"), hence
  `>` and not `>=`.
- Checked live: run offsets p2 5..10 and p4 7..13; status end offsets p2=11, p4=14, so caught up.
  Verify: 13/13, 0 duplicates.
- **Readers accept v1 and v2** (`common/manifest_reader.py`). v1 manifests (seeds 42 to 601) still
  verify; they just cannot be used for offset-based waiting.

**Duplicates stay a report, not a failure, unless `--fail-on-duplicates` is given.** Plain
`make verify` on seed 501 reports "13 duplicate rows reported, not failing" and passes. That is
the Phase 2 motivation (D1.14) and must stay visible. demo and smoke will pass
`--fail-on-duplicates`, because they use a fresh seed and expect zero duplicates.

### D1.16 Orchestrator, `spark-tools`, `make demo` / `make smoke` (slice d2)

**Flow** (`e2e/orchestrator.py`, laptop side, stdlib + `common/` only): seed in Python (epoch ms)
-> check Kafka, start Spark (idempotent `up -d`) -> **baseline** status snapshot -> simulator
run -> find the one manifest for that seed (v2 or later: it needs the Kafka offsets) ->
**contiguity check** -> **pre-run guard** -> wait for catch-up -> verify in `spark-tools` with
`--fail-on-duplicates`. The pure logic lives in `e2e/catchup.py` and is unit-tested (34 tests
in slice d2). Docker calls are thin wrappers in `e2e/docker_ops.py`, each with a timeout.

**Exit codes:** 0 pass, 1 verify failed, 2 setup problem, 3 Spark did not catch up (timeout *or*
container stopped; we don't wait out the timeout if the container is gone), 4 contiguity
violation. `make` itself always exits 2 on failure, so the real code is in the final
`SUMMARY {json}` line. Scripts and CI should read that line, or call the Python module directly.

**Finding: Spark 4.1.4 posts no progress events while idle.** Experiment (2026-11-15, our job,
10 s trigger, status file polled every 1-2 s):
- After a restart with no new data: 150 s, **0** status updates, **0** `batch progress` lines.
- After a batch with data (batch 9): 139 s idle, **0** further updates.
- So the status file is event-driven, not a heartbeat. `updated_at` can be hours old on a
  perfectly healthy idle job. (Older Spark versions sent "no-data" progress events every 10 s;
  4.x does not, at least with our listener. We ignore `onQueryIdle`; using it as a heartbeat is
  a possible later improvement.)
- **Consequence for the hints:** an old `updated_at` is never evidence on its own. "Status file
  not updating" is only diagnosed when the *logs* show a batch committed during the wait that
  the file does not show, or a `could not write status file` warning.

**Timeout diagnosis** (always prints container state, Kafka state, status file
MISSING/UNREADABLE/age, the lag table and the last 40 log lines, then one hint):

| Hint | Rule |
|---|---|
| stopped | container exited with code 0 (e.g. `make spark-stop`) |
| crashed | container missing, non-zero exit, or OOMKilled |
| status file not updating | logs show a batch_id newer than the file, or status write warnings |
| slow | batches committed during the wait, or remaining offsets went down |
| too early to tell | none of the above, and waited < 2 trigger intervals |
| stuck | none of the above, after >= 2 trigger intervals |

All six were exercised live, except "slow" and "stuck" (unit-tested only).

**Contiguity check.** For every partition: `count == max - min + 1`. If it isn't, another
producer wrote into the run's offset range, and we stop with exit 4 before waiting. Shown live
with a real second simulator writing at the same time: "partition 0: run wrote 20 messages but
its offsets span 493..563 (71 slots) -> 51 offsets in that range came from another producer".
Caveat: if we ever use Kafka transactions, commit markers take up offsets, so this rule would
need to change.

**Stale-status guard (freshness).** Offsets alone are not enough. If the topic is recreated,
offsets restart at 0, and an old status file with end offsets of, say, 900 would "prove" any new
run is caught up. Two checks:
1. *Pre-run guard (exit 2):* the baseline is taken **before** the run is sent. If the baseline
   already has a committed end offset greater than the run's first offset, the status cannot
   describe this topic's history ("topic probably recreated"). Shown live with a faked status
   file (end offsets 99999).
2. *Freshness:* "caught up" also requires (a) the manifest's topic name in the status offsets,
   and (b) a batch committed **after** the baseline (higher `batch_id`, same `query_id`), or a
   new `query_id` (checkpoint reset: the new query re-reads the topic, so its offsets are new
   evidence).
- **Limitation:** neither the status file nor the manifest records Kafka's topic **ID** (a UUID
  that changes when a topic is recreated), only its name. So the guard can't catch a recreated
  topic that has *already* grown past the old offsets before the run. Also, we haven't tested
  what Spark itself does when the topic is recreated under an existing checkpoint (we expect
  `failOnDataLoss=true` to stop it, but that is unverified). Recording the topic ID in both
  files would close this gap. Deferred to Phase 4 (chaos).

**`spark-tools` service** (profile `tools`): same image, `pull_policy: never`, 1.5 GB limit, no
ports or `container_name` (runs next to the streaming job), no `depends_on` Kafka, and **`./data`
and `./manifests` mounted read-only**. Spark's scratch space is `SPARK_LOCAL_DIRS=/tmp/spark-local`
inside the container's own filesystem, not a host mount. Proven live: verify PASSes, `touch
/data/x` fails with "Read-only file system", and `mount` shows `/data ... (ro,...)`. Reading
the sink's `_spark_metadata` needs no writes. `make verify` now uses it too.
*Updated in Phase 2:* verify reads the Delta tables now (D2.2), still with `./data` read-only.

**demo vs smoke:** same code. smoke = tiny config, 90 s timeout, short output. demo = 20 matches
(~1250 events), 180 s timeout, verbose (lag table, full verify report, where to look). Both
build images quietly first (`make e2e-images`). Live numbers on this laptop: smoke ~31 s total
(catch-up 4-6 s, verify ~20 s, mostly JVM start-up), demo ~36 s.

**Restart check: `make smoke-restart` (slice d3, `e2e/smoke_restart.py` + pure `e2e/restart.py`).**
- *Scenario:* baseline -> graceful stop (assert exit 0) -> new-seed run sent while Spark is
  down -> restart -> catch-up -> checks. About 48 s if Spark is already running, ~64 s if it
  has to be started first. The job is down for ~20-30 s and **always left running**: a
  `finally` block restarts it, and if that fails, the last line says `Start it with: make spark-up`
  and the SUMMARY has `"spark_left_running": false`.
- *Baseline (E0):* Kafka's real end offsets (`kafka-get-offsets.sh --time -1`) vs the status
  file's committed end, not "status stopped changing". Because idle Spark is silent, that's the
  only way to know the baseline is complete. E0, B0 (batch id), Q0 (query_id) and R0 (run_id)
  are read from the status file *after* the stop, when nothing can change them.
- *First post-restart batch:* the history entry with batch_id **B0 + 1** under the same Q0.
  Batch ids go up by exactly 1, so if B0 + 1 isn't there we can't prove anything (e.g. evicted
  from the 100-entry history). Its run_id must differ from R0 and equal the run_id in the new
  process's `query started` log line. A batch that was planned but not committed before the stop
  is re-run as B0 + 1 and must also start at E0.
- *Checks:* no re-read (start == E0 in every partition; later batches chain end -> start), no
  skip (rows read after restart == run count; run.min == E0 and final end == E0 + count per
  written partition; untouched partitions don't move), and verify `--fail-on-duplicates`.
  "Cannot prove" (E0 missing for a written partition, `start_offset` null, wrong run_id) fails.
  It never passes.
- *Independent data check (not our listener):* Parquet rows counted from **file footers** with
  pyarrow (no JVM, ~0.03 s for 76 files) after the stop and after catch-up; the delta must equal
  the run count. Footers also count files Spark never committed (aborted batches), so we
  cross-check the "after" total against Spark's committed total, which verify already reports
  (`parquet_total_rows`), at no extra JVM cost. If they're equal, there are no uncommitted files
  after. The file sink only adds files, so there were none before either, and the footer delta
  is exactly the committed delta. Checked once by hand: footers 4677 == Spark 4677. Cost: pyarrow
  26.0.1 added to dev requirements (no transitive deps).
  *Superseded in Phase 2 slice 2 (D2.2): the footer count is replaced by a Delta-log count, the
  footer-vs-committed cross-check by log-vs-Spark time travel, and a txn identity check is new.*
- *Why the rewind test matters:* verify only looks at **this** run's ids. A restart that re-read
  the previous run's last message would still pass verify. Only start == E0, the row count and
  the Parquet delta catch it (shown with a faked evidence file).
- *Evidence file:* every run writes `data/e2e/smoke_restart_<run_id>.json` with all inputs to
  the checks. `--check-evidence FILE` re-runs the checks offline. The faked failure paths are
  edited copies, so there are no fault-injection hooks in the live code.
- *Exit codes:* d2's 0-4, plus **5 = restart rule failed** (stop exit code, re-read, skip,
  Parquet delta, cannot prove). Verify failing on its own gives 1.
- *Assumption:* nobody else writes to the topic. If someone does, run.min != E0 stops the test
  with exit 4 (shown live with a second simulator: "p5: run starts at 966 but E0 is 888").
- *Out of scope until Phase 4:* `kill -9`, killing a batch midway, Kafka restarts, topic
  recreation.
- *Update (Phase 2 slice 6, D2.6):* two statements above are now narrowly out of date: "no
  fault-injection hooks in the live code" (*Evidence file*) and "`kill -9`, killing a batch
  midway" (*Out of scope*). Test-only hooks exist at the silver write boundaries
  (`streaming/faults.py`, inert unless `docker-compose.faults.yml` sets them), and
  `make smoke-faults` SIGKILLs the job at each of them plus once inside bronze. Kafka restarts
  and topic recreation are still not tested.
- *Phase 2 run times* (bronze + silver, verify of both layers; `docs/evidence/phase2/`):
  `make smoke` 139.5 s and 225.1 s, `make smoke-restart` 189.6 s (Phase 1: ~31 s and ~48 s).
  In the final Phase 2 gate (after `make down && make up`): smoke 340.9 s, smoke-restart 400.6 s.

### D1.13 Plan: simulator pacing modes + deliberate lateness (BUILT in slice e, see D1.17)
> **Status: built on 2026-11-15.** This is the original plan, kept for the record. D1.17
> describes what was actually built. Differences from this plan: (1) the default pace is
> `backfill`, not `realtime`; (2) lateness comes from a per-event sha256 hash, not from a
> second `Random` stream (`Random(seed, "lateness")` is not valid Python anyway); (3) delays
> come from two bands with a gap between them, not "5-60 s plus a 10 min tail"; (4) the send
> order is a plain sort, not a priority queue (all events exist up front).

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

### D1.17 Pacing modes, deliberate lateness, manifest v3 (slice e, as built)
Rule: **what an event is never changes; only when it is sent.** The generator is untouched.
`tests/unit/test_event_id_pins.py` pins the event_ids of seeds 42 and 7, captured from the code
before this slice; no pacing or lateness setting may change them.

Code: `simulator/schedule.py` (pure planning), `simulator/sender.py` (the only place that
waits), `simulator/main.py::prepare_run`, `simulator/manifest.py`, `common/manifest_reader.py`,
`e2e/timing_check.py`.

**Pace modes (`pace`, `--pace`)**
- `backfill` (default): send as fast as possible. With no `start_time` the timeline is placed
  so that its last `event_time` is "now", so every event is in the past, like replaying
  history. With an explicit `start_time` the run is refused (exit 2) if its last event would be
  in the future.
- `realtime`: each event is sent when the wall clock reaches its send time. The timeline starts
  1 s after "now". `start_time` (except with `--dry-run`) and `rate_per_s` are refused.
- `time_scale` (default 1): divides all gaps in the timeline before `event_time` is stamped, so
  `event_time` still equals the real send time. `duration_s` is not scaled (it is game-clock
  data, not a timestamp). Delays of late events are NOT scaled either: a watermark is measured
  in real seconds.
- *Why backfill is the default:* it finishes at once, so `make smoke`, `make demo` and
  `make smoke-restart` keep their run times, and it already removes the future timestamps.
  *Tradeoff:* with the default config, event times are up to ~50 minutes old on arrival.
- *Realtime waiting:* due times are tracked on the monotonic clock (the wall clock can jump,
  e.g. when WSL2 resumes), the wait is `producer.poll(timeout)` so delivery callbacks keep being
  served, and just before each send the wall clock is checked so that no event leaves before
  its own `event_time`. If the laptop falls behind, events go out late but in the same order.

**Deliberate lateness (`late_fraction`, default 0 = off)**
A late event keeps its `event_time` but is sent `delay` later, so it arrives after newer events
of the same match (out of order within the partition). Exact formula:

```
digest = hashlib.sha256(f"{seed}:lateness:{event_id}".encode("utf-8")).digest()
u1 = int.from_bytes(digest[0:8],   "big") / 2**64
u2 = int.from_bytes(digest[8:16],  "big") / 2**64
u3 = int.from_bytes(digest[16:24], "big") / 2**64
late     = u1 < late_fraction
band     = "very" if u2 < late_too_late_share else "slight"
lo, hi   = (2.0, 3.0) if band == "very" else (0.1, 0.5)      # multiples of W
delay_ms = round((lo + u3 * (hi - lo)) * assumed_watermark_s * 1000)
```

- `hashlib` on purpose, never Python's `hash()`: `hash()` is salted per process
  (PYTHONHASHSEED), so the plan would change on every run. A test checks two processes with
  different hash seeds give the same result.
- It never touches the generator's RNG. An event's lateness depends only on
  (seed, event_id, settings), so adding matches or changing pace or `time_scale` never changes
  it, and raising `late_fraction` only adds late events.
- `late_too_late_share` (default **0.2**) is the share of the *late* events that land in the
  `very` band. It is a setting, not a side effect.
- `assumed_watermark_s` = W (default **30**, a placeholder). Phase 2 chooses the real
  watermark; the bands are multiples of W, so one number rescales every delay.
  **Changed in Phase 2 slice 4:** the default is now **60**, the silver job's
  `WATERMARK_DELAY_S` (D2.4). Bands at W = 60: slight 6-30 s, very 120-180 s, so `make
  sim-late` takes up to about 3.5 min. Old manifests keep their recorded 30. A unit test
  (`tests/unit/test_watermark_defaults_agree.py`) checks that the simulator, job and compose
  defaults agree; there is no shared code on purpose.

| Band | Delay | `expected` |
|---|---|---|
| on time | 0 | `keep` |
| `slight` | 0.1 W to 0.5 W (3-15 s at W = 30) | `keep` |
| `very` | 2 W to 3 W (60-90 s at W = 30) | `may_drop` |

**The band rule.** `expected` is decided by the band, i.e. by the planned delay, not by
`stream_lag_ms`.
- `stream_lag_ms` (recorded per late event) is how far the newest `event_time` sent before the
  event is ahead of the event's own `event_time`. That is the quantity a watermark compares,
  and it is never larger than the delay.
- `keep` for `slight` events **relies on the dead zone between 0.5 W and W**. Sending is
  asynchronous: a message handed to the client slightly later can reach the broker slightly
  earlier (another partition, batching, a retry). So a neighbouring newer event may overtake a
  late one and push the watermark a little further than planned. A slight event is at most
  0.5 W behind the stream; the other 0.5 W absorbs that reordering. No delay is ever generated
  between 0.5 W and 2 W, so no test outcome depends on millisecond timing.
- `may_drop` means *allowed*, not *obliged*, to be rejected. A very late event with a small
  lag (for example at the very end of a run, when nothing newer exists) will normally be kept.
  It is still labelled `may_drop`, because "nothing newer overtakes it" cannot be promised.

**What lateness can and cannot show**
- Spark's watermark is `max(event_time seen in EARLIER micro-batches) - W`. It never looks at
  the wall clock.
- **Backfill lateness cannot demonstrate drops.** In backfill the events are sent in late
  order but without waiting, so a whole run lands in one or two micro-batches and the watermark
  hardly moves while they are processed. Backfill lateness only tests out-of-order handling.
  **Phase 2 late-drop tests must use `pace: realtime`.**
  *Updated in Phase 2:* realtime alone was not enough: the tiny realtime timeline (~15 s) is
  shorter than W, so `make smoke-late` uses a longer config (`simulator/config.late.yaml`,
  D2.5). `make smoke-faults` also routes a whole backfill run to late, by sending it with a
  timeline 30 minutes in the past after newer data was processed (D2.6). That is routing
  across runs, not lateness inside one run, so the sentence above still holds.
- **UNVERIFIED claim, to re-check against Spark 4.1.4 at the start of Phase 2:** "Structured
  Streaming has no side output for too-late rows; stateful operators drop them silently." This
  comes from general knowledge of Spark, not from a test in this repo. If it is true, Phase 2
  must route too-late events to a table itself, because silent drops would break the oracle.
  *Checked at the start of Phase 2* by reading the Spark `v4.1.4` source (commit
  `45b9b176e6e2`, `sql/core/.../execution/streaming/operators/stateful/statefulOperators.scala`):
  rows behind the watermark are removed with `filterNot` and only counted in
  `numRowsDroppedByWatermark` (`:504-512`); the late test is `event_time <= watermark`
  (`:642-648`); streaming dedupe (`dropDuplicates`, `dropDuplicatesWithinWatermark`) applies
  that filter before dedupe (`:1308-1311`). No side output exists, so Phase 2 routes late rows
  to its own table (D2.4).
- **Oracle (Phase 4) with lateness.** Late events are Kafka-confirmed, so they stay in the
  manifest's delivered ids, totals and checksum; v3 only annotates them. The raw layer must
  contain every one of them (`make verify`, unchanged). For a watermark-gated layer the rule
  becomes: `accepted + late table == confirmed`, no overlap, and anything missing from
  `accepted` must be a `may_drop` event.
  *Built in Phase 2:* R1-R3 (D2.4) and R4/R5 (D2.5) are this rule, with silver as `accepted`.

**Manifest v3** (v1 and v2 stay readable; unknown versions are rejected)
- `pacing`: mode, `time_scale`, `assumed_watermark_s`, `late_fraction`, `late_too_late_share`,
  both band definitions, and what the send loop measured (`sent`, `future_events`,
  `max_ahead_ms`, `max_behind_ms`).
- `late_counts`: delivered late events per band. Always written.
- `late_events`: one entry per delivered late event, in send order: `event_id`, `match_id`,
  `delay_ms`, `band`, `expected`, `stream_lag_ms`. Only delivered events are listed.
- The list is left out (`null`) in two cases, and `late_events_omitted_reason` says which:
  `no_manifest_ids` (`--no-manifest-ids`) or `too_many_late_events` (more than 100,000, about
  20 MB of JSON).
- *Why a new version for an additive change:* the reader rejects unknown versions on purpose,
  and the version tells a reader what a missing entry means.

What the reader (`common/manifest_reader.py`) returns, and what it means:

| Manifest | `pacing` | `late_events` | Meaning |
|---|---|---|---|
| v1, v2 | `None` | `None` | lateness was **not recorded**; nothing can be concluded |
| v3, list present | set | mapping, possibly empty `{}` | an event **not in it was on time**; `{}` = nobody was late |
| v3, list omitted | set | `None`, reason set | counts known (`late_counts`), individual events not |

Never treat `None` as "on time": the e2-era v2 fixture is a run that *did* send late events
and has no record of them.

**Guardrails**
- The send loop counts events sent before their `event_time` (`future_events`, WARNING if > 0).
- `make timing-check MANIFEST=...` is the independent check: it reads the run back from Kafka
  and compares `CreateTime` with `event_time`. Tolerance 1 ms, because both timestamps are
  whole milliseconds, so rounding alone can differ by 1 ms. Measured on this machine: 0 ms.
- Seed reuse: if a manifest for the same seed already exists, the simulator logs a WARNING
  (it does not fail, because sending duplicates on purpose is a valid test, D1.6).
- `make verify` rebuilds the Spark image first (cached), so it never runs a stale reader.

**Known limits**
- All events are held in memory and sorted (unchanged). Fine for tests; large benchmarks will
  need streaming generation.
- A very large realtime run starts slightly behind, because the plan is built after "now" is
  taken. `max_behind_ms` shows it.
- Out of scope, Phase 4: injected duplicates and shuffled order (they will reuse the schedule).
  *Phase 2 note:* `make smoke-dupes` re-sends a whole seed (D2.3); duplicates of single events
  and shuffled order are still Phase 4.

---

## Phase 2: Delta, dedupe, watermark, idempotent MERGE

### D2.1 Delta Lake pin: `io.delta:delta-spark_4.1_2.13:4.4.1` (slice 1, compatibility gate)

**Decision.** Pin Delta 4.4.1, Spark-4.1 build. The jars are baked into the image at build time
(like the Kafka connector, D1.12). pip `delta-spark==4.4.1` provides only the Python API
(`delta.tables`); `configure_spark_with_delta_pip` is not used, so nothing downloads at runtime.
- **Not** the unsuffixed `delta-spark_2.13`: in Delta 4.4.x that artifact is built for Spark 4.2.
  C1 fails if any `delta-spark_2.13-*` jar is on the classpath.
- Session settings: `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`,
  `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`. Both are
  static (set before the session starts), so `tests/spark/conftest.py` sets them for all tests.

**Gate result (2026-11-18): `make delta-compat` PASS, all 14 checks, about 218 s**
(`streaming/delta_compat_check.py`; temp folders in the container's `/tmp`, `./data` read-only).

| Check | Result |
|---|---|
| C1 versions | Spark 4.1.4, Scala 2.13.17, Java 17.0.21, pip delta-spark 4.4.1; `DeltaLog` loaded from `delta-spark_4.1_2.13-4.4.1.jar` |
| C2 classpath | 299 jars, 13 added. Added jars: 0 same-name, 0 duplicate classes. Jackson aligned. Lock verified. Details below |
| C3 create | 3 rows; `engineInfo` = `Apache-Spark/4.1.4 Delta-Lake/4.4.1` |
| C4 MERGE twice | 5 rows, then 5; the replay wrote **no new version** |
| C5 pruning (R) | insert-only MERGE reports no file-skipping metrics (see facts) |
| C6 checkpoint | `00000000000000000010.checkpoint.parquet` after 12 commits; fresh read 12 rows |
| C7 numRecords | live `add.stats.numRecords` sum == `count()` on 4 tables, incl. a DELETE |
| C8 sink | 3 + 2 batches, 50 rows, 50 distinct; `txn` versions 0..4, one stable `appId` |
| C9 sink replay | deleted `commits/4`: batch 4 re-ran, Delta version stayed 4, still 50 rows |
| C10 foreachBatch | Delta source 31 rows (23 distinct, dups inside and across commits) -> target 23 |
| C11 fB replay | deleted `commits/3`: batch 3 re-ran, MERGE wrote no version, target still 23 |
| C12 event order | 11 progress events (6 sink, 5 foreachBatch), 0 violations |
| C13 log scan | 0 linkage/class-loading errors (LinkageError family + ClassNotFoundException) |
| C14 Jackson | jdk8 module 2.21.2 on databind 2.21.2: `Optional` written and read back correctly |

**pip.** pip's own resolution (dry run in the old image) added exactly `delta_spark 4.4.1`,
`importlib_metadata 8.7.1`, `zipp 4.1.2`; all three are pinned. Two gates:
- *Every build:* `docker/spark/pip_gate.py` fails the build unless `pip freeze` equals
  `requirements/spark.txt` exactly (nothing moved, nothing unpinned) and pyspark is 4.1.4.
- *Slice 1, one-off:* `pip freeze` of the old and new image differ only by those three added
  lines; all 7 old lines, including `pyspark==4.1.4`, are unchanged.

**Jar set** (three `fetch_jars.sh` runs: Kafka, Delta, Jackson; list in `/opt/valkyrie/jars-*.txt`)
- Added by Delta: `delta-spark_4.1_2.13`, `delta-storage`, `delta-kernel-api/defaults/unitycatalog`
  (4.4.1), `unitycatalog-client/hadoop` (0.6.0), `javax.annotation-api` 1.3.2. Plus
  `jackson-datatype-jdk8` 2.21.2. The Kafka set is unchanged from D1.12. 13 added jars in total.
- **Excluded from the Delta resolution** (`--exclude-packages`), because the name-based skip
  skips a jar but still adds that jar's own dependencies (WHAT_BROKE):
  - `commons-logging` 1.3.0 (via `hadoop-client-runtime`): 5 of its classes are also in Spark's
    `jcl-over-slf4j`, so which copy wins would depend on the order of `jars/*` on the classpath,
    and Java does not specify that order. Its other 22 classes are JCL's own backends (e.g.
    `impl.LogFactoryImpl`, `impl.Log4jApiLogFactory`). Stock Spark 4.1.4 has never shipped
    them. A scan of all jars found one reference: the *string* `...impl.Jdk14Logger` in
    `hadoop-client-runtime`'s shaded Jetty `JettyLogHandler`, already there before Delta.
  - `shims` 0.9.25 (via RoaringBitmap 0.9.25): its only class, `ArraysShim`, is in Spark's
    `RoaringBitmap-1.3.0`. RoaringBitmap 0.9.25 itself was skipped.
  - `jackson-datatype-jdk8` 2.13.5 (via Delta Kernel): replaced by 2.21.2, see below.
- **Jackson alignment.** All 7 jars with real `com/fasterxml/jackson` classes are 2.21.2, except
  `jackson-annotations` 2.21. Since 2.20 Jackson versions annotations as major.minor only
  (Maven Central lists 2.20, 2.21, 2.22 and no 2.21.x). C2 enforces exactly this rule.
  Embedded Jackson metadata in `hadoop-client-runtime` (2.12.7) and `parquet-jackson`
  (2.19.2) belongs to relocated classes; it is printed, not checked.
  - *Why aligned:* Jackson modules should match databind's version. The probe passed with both
    2.13.5 and 2.21.2, so this is consistency, not a fix for an observed failure.
  - *Who uses it:* only Delta Kernel (`JsonMetadataDomain`, `MetricsReportSerializer`). C14 found
    that none of these classes were loaded during C3-C12 (path tables, MERGE, streaming).
  - *Accepted risk:* Delta Kernel was compiled against Jackson 2.13.x. A Jackson API change
    that Kernel relies on would surface only on a Kernel code path we do not exercise yet.
- **Skips where Spark's version wins** (Delta declared a newer one): commons-pool 1.6 -> 1.5.4,
  log4j 2.25.3 -> 2.24.3, jsr305 3.0.2 -> 3.0.0. C13 found no linkage errors. This is verified
  **only on the code paths C3-C14 exercise** (path tables, MERGE, checkpoints, the streaming
  sink and source, `foreachBatch`, the Jackson probe). A Delta code path that needs an API only
  the newer version has would fail at runtime, and the gate would not have seen it.
- Image size: 1,501.6 MB -> 1,529.2 MB (+27.5 MB).

**Classpath baselines (stock pip pyspark 4.1.4, measured in slice 1).** C2 fails only on
problems involving jars we add. Stock findings are printed, compared with these baselines, and
only WARN on drift, so a Spark upgrade gets noticed without breaking the build.
- 1,364 duplicate classes in 7 jar groups of Spark's own jars (735 ORC shaded protobuf, 382
  shaded fastutil in parquet-column/hadoop, 180 jdo, 42 java-diff-utils/scala-compiler, 18
  jta/transaction-api, 4 jline/leveldbjni, 3 hive-common/storage-api). Ignore list for added
  jars: only Spark's empty `org/apache/spark/unused/UnusedStubClass`.
- 2 same-name pairs: `hive-shims-0.23-2.3.10` / `hive-shims-2.3.10` (a misread name, see the
  next point), `jline-2.14.6` / `jline-3.29.0-jdk8` (groupIds `jline` vs `org.jline`).

**File-name rule limitation (fetch_jars.sh skip logic and C2).** The artifact name is the file
name before the first `-<digit>`. That misreads names with a digit after a dash:
`spark-sql-kafka-0-10_2.13-4.1.4.jar` -> `spark-sql-kafka-0`, `hive-shims-0.23-...` ->
`hive-shims`. C2 now audits every add/skip decision against the real coordinate recorded from
Ivy's cache. In this build 2 of 45 names were misparsed (both Kafka connector jars), and **0
decisions changed**: with the real name they would still be added. 10 skips could be
confirmed only by name (Spark's jar has no `pom.properties`); all 9 distinct pairs share 100%
of the fetched jar's classes with Spark's jar (one-off check). 10 skips but 9 pairs, because
`hadoop-client-runtime-3.4.2` is skipped twice (Kafka list and Delta list, same Spark jar).

**Integrity: Ivy does not verify checksums; a SHA-256 lock does.**
- Tested in the image: Ivy (via `spark-submit --packages`) accepted a jar whose `.sha1` was all
  zeros, from a `file://` repo and over HTTP. Over HTTP it never even requested the `.sha1`.
  Ivy's default `ivy.checksums=sha1,md5` has no effect on this path.
- Resolvers (from Ivy's "tried" list for a missing artifact): `local-m2-cache`
  (`file:~/.m2`), `local-ivy-cache`, `central` (`https://repo1.maven.org/maven2/`),
  `spark-packages` (`https://repos.spark-packages.org/`). Network resolvers are HTTPS only; all
  44 jar downloads in the build log were `https://`. The two local caches are empty in a fresh
  build container.
- **Lock:** `docker/spark/jars.sha256` lists every jar `fetch_jars.sh` adds (sha256, file,
  group:artifact:version). After the three fetches, `jars_lock.py verify` fails the build on a
  hash or coordinate mismatch, an unlisted jar, or a listed jar that was not added. A passing
  build writes `/opt/valkyrie/lock-verified.txt`, and C2 fails without it.
- **Regenerating on purpose:** `make jars-lock` builds a separate tag with verification off,
  hashes the added jars, downloads Maven Central's published `.sha1` for each over HTTPS
  (Python, certificate checked, `http://` refused), prints the comparison and writes the lock
  only if all match. First run: 13 of 13 equal. An upgrade is then a reviewable diff.
- *Trust model:* trust on first use, cross-checked with Central's `.sha1` (same origin as the
  jar). This protects against anything that changes later (a replaced artifact, a resolver
  drifting to another version or dependency). It does not protect against a compromise that
  was already present when the lock was generated. Signature (`.asc`) checks would; not done.

**Recorded facts for later slices**
- Delta sink idempotency (D1 bronze): every sink commit carries `txn(appId = query id, version =
  batch id)`. On replay the sink logs `DeltaSink: Skipping already complete epoch N` and writes
  nothing (C9).
- Delta source offset (D8 catch-up): `{"sourceVersion":1,"reservoirId":"<table id>",
  "reservoirVersion":V,"index":I,"isStartingVersion":true|false}`.
- MERGE `operationMetrics` keys (22): `numSourceRows`, `numTargetRowsInserted`,
  `numTargetFilesAdded/Removed`, `numTargetRowsMatchedUpdated`, `numTargetBytesAdded`,
  `executionTimeMs`, `scanTimeMs`, `rewriteTimeMs` and others (full list in the report).
- **An insert-only MERGE that inserts nothing writes no commit** (C4, C11). Code that reads
  "the version this batch produced" must handle "no new version".
- **Insert-only MERGE has no file-skipping metrics** (`numTargetFilesBeforeSkipping/
  AfterSkipping` absent in C5). Pruning evidence for D2 must come from somewhere else in slice
  3, e.g. the query plan or scan metrics. UNVERIFIED which. *Resolved in slice 3 (D2.3):*
  Spark's SQL execution metrics of the MERGE's target scan.
- Checkpoints are classic `<v>.checkpoint.parquet` every 10 commits, plus `_last_checkpoint`.
- `DELETE` rewrote files (2 removes) and made no deletion vectors: they are off by default
  here. The Delta-log count in slice 2 still subtracts `deletionVector.cardinality`, in case
  they are turned on later.
- D1.15 holds for Delta: at every progress event, `commits/N` and the batch's Delta version
  already existed (Delta sink and `foreachBatch`).

**Not verified (limits of this gate)**
- `availableNow` runs few, small batches: C12 shows the order, it is not a stress test.
- Deletion vectors (and RoaringBitmap 1.3.0 under Delta's 0.9.25 API), Unity Catalog and Delta
  Kernel code paths were not exercised.

### D2.2 Bronze Delta table replaces raw Parquet (slice 2)

**Decision.** The Phase 1 job becomes `streaming/bronze.py`. It writes Kafka to an
**append-only Delta table** `data/lake/bronze/game_events`, partitioned by `ingest_date`
(D1.9), with a new checkpoint `data/checkpoints/bronze` and status file
`data/status/bronze.json`. `to_raw_rows` is byte-for-byte the Phase 1 code. Bronze still holds
**one row per Kafka message**, so duplicate events stay visible (D1.14) until silver (slice 3).
The Phase 1 Parquet, checkpoint and status file are left on disk untouched; tag `phase-1` keeps
the code. Env: `BRONZE_PATH`, `BRONZE_CHECKPOINT_PATH`, `BRONZE_ALLOW_REBUILD` (the `RAW_*`
names are gone).

**Table created explicitly, `delta.appendOnly = true`.** At start-up the job creates the table
if absent (`DeltaTable.createIfNotExists`, schema = `to_raw_rows` output with every column
nullable) and then checks, on every start, that it is partitioned by `ingest_date` only and is
append-only. Otherwise it exits 2.
- Why: bronze is the record of what Kafka delivered. Delta itself now refuses `UPDATE`,
  `DELETE` and row-removing `MERGE`, and the Delta-log count below can treat a data-changing
  `remove` as an error.
- All columns nullable: a DataFrame write would create them nullable too; NOT NULL would only
  make Phase 5 and replays stricter. R0 and `parse_ok` already cover the key columns.
- Tested (`tests/spark/test_bronze_table.py`): `DELETE` and `UPDATE` refused; **`OPTIMIZE`
  works** (its removes are `dataChange=false`); **adding a nullable column works** three ways
  (`ALTER TABLE ... ADD COLUMNS`, then an append without the column, an append that fills it,
  and a `mergeSchema` append with another new column). The start-up check accepts the grown
  table, because it only creates when no table exists.
- Cost: a deliberate delete (e.g. GDPR) first needs `ALTER TABLE ... SET TBLPROPERTIES
  (delta.appendOnly = false)`, a visible, logged step.

**Start-up guard, both directions** (`streaming/start_guard.py`; first built as a one-way
checkpoint-reset guard in slice 2, extended in the slice 2 follow-up). Before the table is
created or touched, the job compares the checkpoint with the table. On a mismatch it logs
`start-up refused` (ERROR) and exits 2. `BRONZE_ALLOW_REBUILD=1` overrides it with a WARNING
(`REBUILD: ...`).
- **One rule.** Let C = the checkpoint's newest `commits/N`, P = its newest `offsets/N`,
  Q = its query id (`metadata`), and T = the table's newest `txn` version with `appId == Q`.
  Start only if **T == C**, or **T == C + 1 with P >= C + 1**. With no batch in the
  checkpoint (P missing), start only if the table has no rows.
- **Why one *ahead* is legal and one *behind* is not.** For each batch N the order is
  `offsets/N` -> Delta commit with `txn(Q, N)` -> `commits/N` (D1.15, C12). A crash between
  the last two leaves T = C + 1. Spark then re-runs N, and the sink skips it ("Skipping
  already complete epoch"); tested by deleting `commits/1`. T < C cannot happen, because
  `commits/N` is written only after the Delta commit, and the sink commits a `txn` even for
  a batch with zero output rows (tested). So "behind" always means the table lost commits
  (e.g. restored from an older copy).
- **What it catches:**

  | Case | T vs C | Why starting is wrong |
  |---|---|---|
  | checkpoint missing/reset, table has data | no batches | new query id: Delta's `txn` cannot match the old batches, so every message would be appended again (below) |
  | table deleted, checkpoint kept | T missing | Spark resumes at the checkpoint's offsets: every row before them is silently gone |
  | table replaced by another (no txn for Q) | T missing | same |
  | table restored from an older copy | T < C | batches T+1..C would be lost |
  | another writer used Q as `txnAppId` | T > P | the table claims batches the checkpoint never planned |
  | batch 0 planned, not committed; table has another query's data | T missing, C missing | same as a reset |

- **Why Delta's `txn` cannot protect a checkpoint reset.** The sink's idempotency (D2.1, C9) is
  a lookup: "does the table already have `txn(appId = this query's id, version = this batch
  id)`?" The query id is stored in the checkpoint. A new or emptied checkpoint makes Spark
  generate a **new query id**, so every old `txn` entry has a different `appId` and nothing
  matches. Batch 0 of the new query re-reads the topic from `earliest` and Delta appends all
  of it again. `txn` protects replays *within* one query, not a reset that creates a new one.
  Afterwards R0 would show it, but by then every message is in bronze twice, and append-only
  makes the clean-up a deliberate operation.
- **Reading T.** `common/delta_log.latest_txn_in_commits` scans commit JSONs from newest to
  oldest (a query's txn version only grows, so the first hit is the maximum). If log cleanup
  (30 days by default) has removed older commits and none of the remaining ones has it, the
  job reads the newest Delta checkpoint Parquet with Spark, where `txn` actions are kept.
  Tested by deleting commits 0..10 after a checkpoint at version 10.
- Tested in Spark (`tests/spark/test_bronze_table.py`), each state created for real: fresh
  (no table / empty table); refused with no batches (missing checkpoint, `metadata` only,
  empty `offsets/`); normal resume; T = C + 1 after deleting `commits/1`, then a re-run that
  writes nothing; table deleted; table replaced; table restored from an older copy; T ahead
  (a write with `txnAppId = Q, txnVersion = 5`); a zero-row batch still commits its `txn`;
  T found in the checkpoint after log cleanup; `run()` returns 2 without starting a query;
  the override. The pure decision table has one unit test per branch
  (`tests/unit/test_start_guard.py`).
- Live (2026-11-19, first version of the guard): a one-off container with
  `BRONZE_CHECKPOINT_PATH=/data/checkpoints/bronze_guard_demo` against the populated table
  exited 2, created no checkpoint folder, and wrote no commit.

**R0: `(kafka_topic, kafka_partition, kafka_offset)` is unique in bronze** (parent plan D6).
`verify_manifest` reports `duplicate_kafka_offsets` over the whole table, and any value > 0 is
FAIL (duplicate event ids are only reported, as before). It checks that no Kafka message is
stored twice, over the whole table and across restarts. That every message of a run is there
is a separate check, per run, against its manifest.
- A row with a NULL `kafka_topic`, `kafka_partition` or `kafka_offset` also fails R0
  (`null_kafka_offset_rows`). Every Kafka message has all three, so such a row cannot be tied
  to one message. It is counted separately because SQL `DISTINCT` treats NULLs as equal and
  would hide two such rows as one.
- **Known limit: false positives after a topic is deleted and recreated.** Offsets restart at
  0, so new messages reuse `(topic, partition, offset)` keys already in bronze and R0 fails
  although nothing was written twice. Bronze does not store Kafka's topic id. Until it does,
  recreating the topic means starting a new bronze table.
- Naming: `e2e/restart.py` uses "R0" for the pre-stop `run_id`. That is unrelated; the docstring
  says so.

**verify reads Delta at one pinned version.** It takes the newest version first and reads
everything with `versionAsOf` that version (`table_version` in the report), so all numbers come
from one snapshot. `--versions V0,V1` adds time-travel row counts. Report keys renamed:
`total_rows`, `unparsed_rows` (were `parquet_*`). Runs in `spark-tools` with `./data`
read-only: **reading Delta wrote nothing** (VERIFIED 2026-11-19: `_delta_log` listing unchanged,
no error on the read-only mount). `--layer` was added in slice 3 (D2.3).

**smoke-restart on Delta (evidence_version 2).** The status-file checks (stop, baseline, first
batch, no re-read, no skip) are unchanged. Replaced and new:

| Check | How |
|---|---|
| delta log rows == run | `common/delta_log.appended_between(V0, V1)`: sum of `add.stats.numRecords` (dataChange=true) in commit JSONs V0+1..V1 == run messages |
| delta log == spark time travel | Spark `count()` at V1 minus at V0 (verify `--versions`) == the log delta. Catches a bug in our log parser. |
| txn appId == query id | every commit in (V0, V1] has exactly one `txn`; `appId` == Q0, the query **`id`** (stable, from the checkpoint), not the `runId` (new per start); versions == post-restart batch ids with rows |

- The log reader fails loudly (exit 5, "cannot prove") on a missing commit file, an `add`
  without `numRecords`, a data-changing `remove` or a deletion vector. It never counts 0.
- Why the Phase 1 "footers == committed" check is gone: a Parquet file could exist without
  being committed; a Delta data file counts only if a commit JSON lists it, and we read only
  commit JSONs.
- The evidence file prints both ids (`txn_identity`: `query_id`, `run_id_before`,
  `run_ids_after`, `txn_app_ids`, `txn_versions`).
- Phase 1 evidence (version 1) is refused by `--check-evidence` with exit 2 and a pointer to tag
  `phase-1`.
- `common/delta_log.py` is stdlib only (laptop side, D1.16). `read_commits`, `reconcile` and
  `txn_actions` moved there from the slice 1 gate (one copy); `make delta-compat` still PASSes
  with them (2026-11-19, 245.5 s, all 14 checks). `e2e/parquet_count.py` was removed, and in
  the follow-up `pyarrow` was dropped from `requirements/dev.txt` (nothing imports it; unit
  tests pass in a fresh venv without it).
  *Updated in Phase 2 slice 7:* "nothing imports it" stopped being true in slice 6
  (`e2e/faults_check.py` and its unit test import pyarrow), and the pin was not re-added, so a
  fresh clone failed `make test-unit`. `pyarrow==26.0.1` is pinned again (WHAT_BROKE).

**Measured (2026-11-19, laptop, WSL2; single runs, not benchmarks)**

| What | Value |
|---|---|
| First start, batch 0 (whole topic) | 7,085 rows, 6 files, trigger 6.65 s |
| Start-up: "starting" log -> "bronze table ready" | 17.7 s first start (incl. create), 18.4 s on restart (guard + table check, first Delta use in the JVM; split not measured) |
| `make verify` seed 501 (bronze) | 13/13 ids, 26 rows, 13 duplicates reported, R0 0, PASS, 47 s |
| `make smoke` | PASS, 13/13, 64.3 s total, catch-up 15.3 s (slice 1, Phase 1 job: 46.2 s) |
| `make smoke-restart` | PASS, all 9 checks, 93.5 s total, caught up 34.6 s after restart; V0 2 -> V1 3, +12 rows |
| Spark container memory | 1.09 GiB after batch 0; 1.36 GiB after smoke + restart (limit 3 GiB; Phase 1 measured 626 MiB on tiny data), as `docker stats` reports it; not broken down |
| Bronze on disk | 1.4 MB for 7,110 rows; checkpoint 84 KB |
| `make test-spark` | 36 tests, 566 s (the 13 bronze tests are about 4.5 min of it) |

### D2.3 Silver: one row per `event_id`, insert-only MERGE (slice 3)

**What it is.** A second streaming query in the same Spark process reads the bronze Delta
table (Delta source) and writes `data/lake/silver/game_events`: Delta, partitioned by
`event_date` (UTC date of `event_time`), `delta.appendOnly=true`, created explicitly at start-up
(same pattern as bronze, D2.2). Per micro-batch (`streaming/silver.py`, `foreachBatch`):

1. **Key check (D9):** `parse_ok AND event_id IS NOT NULL AND event_time IS NOT NULL`. Other rows
   are excluded and counted per reason (`excluded_unparsed`, `excluded_null_event_id`,
   `excluded_null_event_time`, `excluded_total`; one row can hit several reasons).
2. **In-batch dedupe:** per `event_id` keep the lowest `(kafka_topic, kafka_partition,
   kafka_offset)`. That key is unique in bronze (R0), so a replay keeps the same copy: the
   first send.
3. **MERGE bounds** from keyed rows only: `[min(event_date) - H, max(event_date) + H]`,
   H = `DEDUPE_HORIZON_DAYS` (7). No keyed rows: no MERGE.
4. **Insert-only MERGE:** `ON t.event_id = s.event_id AND t.event_date BETWEEN DATE'lo' AND
   DATE'hi' WHEN NOT MATCHED THEN INSERT *`. A replayed batch finds every id and commits
   nothing (C4/C11).
5. **One control row per batch**, also for zero-row and all-keyless batches, in
   `data/lake/_control/silver_batches` (Delta, appendOnly), written as a plain append with
   `txnAppId = silver query id`, `txnVersion = batch id`. A replay writes nothing.

**Two idempotency mechanisms, on purpose (parent plan D5).** Silver rows are
**insert-if-absent** (step 4): it does not depend on the batch id, so it also holds after a
checkpoint reset. Control rows use **`txnAppId`/`txnVersion`**: Delta's documented pattern
(Delta docs, "Table streaming reads and writes", section "Idempotent table writes in
foreachBatch", docs.delta.io, page delta-streaming.html). A control row has no natural key,
its idempotency should follow the batch, and the start-up guard reads that txn. The txn
options are used only for this append, never for a MERGE. The Databricks rules for
foreachBatch are followed: no exception is caught or retried inside `process_batch` (only
`try/finally` to unpersist), and `txnVersion` is the batch id Spark passes in. Tested:
replaying a batch from the stream (delete `commits/C` and its `.crc`, re-run) leaves exactly
one control row for C, batch ids 0..C, and the batch's silver row once.

Silver columns: the event fields, `event_date`, the kept copy's Kafka lineage, bronze
`ingest_time` (as `bronze_ingest_time`), `silver_query_id`, `silver_batch_id`,
`silver_inserted_at`. Not copied: `raw_value` and `parse_ok` (bronze keeps the raw message).
The claim is **effectively-once**: silver is insert-if-absent by `event_id` within the date
window, so a replayed batch, or a full re-read of bronze after a checkpoint reset, never adds a
second row for an id. It is not "exactly-once processing".

**The NULL-bound rule (D9), enforced in one place.** In the planning spike, `BETWEEN NULL AND
NULL` made the whole ON clause NULL and **re-inserted an id already in the table** (1,002 ->
1,003 rows). So `streaming/silver_merge.py` (stdlib) builds the bounds and the ON clause:
None, a `datetime` (a subclass of `date`), `min > max` or a negative horizon raise
`MergeBoundsError`; only `DATE'YYYY-MM-DD'` literals rendered from `date` objects reach the SQL.
Min/max come from keyed rows only, so a NULL `event_time` cannot reach them. If keyed rows
existed and a bound were still None, the batch fails, the query stops and nothing is written.
Tests: unit tests per case; Spark tests for the all-keyless batch (no MERGE, no silver commit),
the per-reason counts, a forced missing bound (fails, writes nothing), and a **hazard
regression test** that runs the NULL-bound MERGE and asserts that it duplicates. If a future
Delta changes that, the test says so.

**Known limit (parent plan D2):** two copies of one id more than H days apart (re-stamped
`event_time`) are both inserted. A Spark test documents it. Verify reports the largest spread
and says "not checkable" when it exceeds H. Today's largest spread is 0 days.

**MERGE pruning evidence.** Insert-only MERGE has no files-skipped metric in Delta's
`operationMetrics` (slice 1). Evidence comes from **Spark's SQL execution metrics** (public
REST API, `common/spark_rest.py`): the target `Scan parquet` node of the MERGE execution reports
`number of files read` and `number of partitions read`. The independent expectation is the
number of live files whose `event_date` is in `[lo, hi]`, counted from the Delta log
(`common/delta_log.live_files_in_range`) at the version before that MERGE.
- Spark test (10 date partitions, 1 file each, a batch in 2 of them, H = 0): **files read 2 ==
  files in window 2 < 10 live**, partitions read 2. VERIFIED.
- Live (`make smoke`, SUMMARY `merge_scan`): read 4 files, 4 in window, 4 live; smoke-dupes: 6,
  6, 6. Correct, but **no pruning is visible yet**: all keyed data spans 2026-11-14..19, inside
  every window. Phase 10 measures it at 1-50M rows. Limit: only `Scan parquet` nodes are
  counted (the MERGE source is a persisted micro-batch, not a Parquet scan), and a batch
  running while the orchestrator reads can mismatch; then `matches_log` is false (never PASS).

**Silver start-up guard: the same rule as bronze, with the control table as `txn`.** C = the
silver checkpoint's newest `commits/N`, T = the control table's newest txn version for the
checkpoint's query id. Start only if T == C, or T == C + 1 with P >= C + 1 (the same
`decide()` as bronze). Order per batch: `offsets/N` ->
MERGE -> control row with `txn(Q, N)` -> `commits/N`. A crash after the control row, before
`commits/N`, leaves T == C + 1: the replay's MERGE finds every id and the control write is
skipped (tested by deleting `commits/C`, then re-running: no new silver or control version;
live with a SIGKILL in D2.6, `after_control`).
**T == C - 1 is never a normal state**: `commits/N` is written only after the control row, and
every batch writes one (condition: also zero-row and all-keyless batches, tested; the
streaming test asserts control batch ids == committed batch ids, including a batch after an
OPTIMIZE of bronze). So "behind" means the control table was restored from an older copy:
refused (tested), and so are a missing control table and a missing silver table while the
checkpoint has batches. Override: `SILVER_ALLOW_REBUILD=1` (WARNING). The same reasoning for
bronze is in D2.2 ("Why one ahead is legal and one behind is not").
- **Not refused: a reset silver checkpoint on a populated silver table.** Unlike bronze, this is
  safe by design: the new query re-reads bronze and every MERGE is insert-if-absent. It logs
  `silver rebuild from bronze (idempotent)` (WARNING). Spark test: same silver rows after a
  reset. Cost: one full re-read of bronze.
- Live: after smoke-restart the guard logged `resume` with committed 3, control txn 3.

**One process, two queries, and the failure policy (tradeoff).** `streaming/pipeline.py` is the
container's `CMD`. Both guards run before either query starts, so a refusal never leaves one
query running alone (exit 2). **If either query fails, the other is stopped and the process
exits 1** (`supervise()`, Spark test with a deliberately failing query). So **a silver failure
also stops bronze ingestion.** We accept that: the pipeline fails loudly instead of bronze
running on while silver silently falls behind, and nothing is lost because **Kafka's 7-day
retention is the buffer**. After a fix, bronze resumes from its checkpoint
(`failOnDataLoss=true` stops it if retention deleted unread data). The cost: one bad silver
batch pauses all ingestion until someone looks. The alternative (separate containers, bronze
keeps running) needs a second JVM (~1 GiB more on this laptop) and a lag alert (Phase 8).
`python -m streaming.bronze` still runs bronze alone.

**Status files v2 and the silver catch-up rule.** `streaming/status.py` stores a `source`
(`kafka` | `delta`). Delta offsets are kept as `{reservoir_id, reservoir_version, index,
is_starting_version}`; v1 files still read as `kafka`. One listener (`streaming/progress.py`)
routes by query name: `data/status/bronze.json`, `data/status/silver.json`. **Rule** (spike Q4,
and a Spark test against real offsets): a committed silver end offset `{V, i, not starting}`
means every bronze version < V is consumed, so silver has consumed version X iff V > X.
Conservative: an offset in the middle of X waits for the next batch, and a starting-version
offset never counts. The orchestrator waits for bronze (unchanged), reads bronze's current
version from its Delta log, then waits for silver.

**verify `--layer bronze|silver|all`** (default `all`). Silver always FAILs on missing run ids
(no late table yet), duplicate rows for the run's ids, **R3** (a duplicate `event_id` anywhere in
silver), **R7** (a NULL `event_id`/`event_time` in silver) and silver ids that bronze lacks.
Consistency is pinned to what silver has consumed: its newest committed batch C ended at bronze
version V, so bronze is read at V - 1, and silver rows from batches > C of the current query are
ignored. Then silver rows == distinct keyed bronze ids (only if no copies are > H days apart),
`sum(control.excluded_total)` == bronze keyless rows (R7), and control batch ids == 0..C. If the
preconditions do not hold it says "not checkable", never "pass".
*Updated in slice 4 (D2.4):* the late table exists now. Missing ids and duplicates are checked
over silver ∪ late (R1-R3), and the whole-table count is silver rows + late rows == distinct
keyed bronze ids.

**Measured (2026-11-19, laptop, `local[4]`, heap 2 GiB, container limit 3 GiB):**

| What | Result |
|---|---|
| First start (silver reads all of bronze, 10 files, in batch 0) | 7,110 rows in, 1 keyless excluded, 51 duplicates dropped, **7,058 inserted** (= distinct keyed ids, spike Q5) |
| Seed 501 | bronze 26 rows (13 duplicates reported), **silver 13 rows**, all 13 the lower offset / earlier `event_time`; verify `--layer all` PASS |
| `make smoke` | PASS 14/14 both layers, 92.8 s; silver caught up 17.3 s after bronze (corrected in the follow-up; first written as 7.3 s) |
| `make smoke-dupes` | PASS: 12 ids, bronze 24 rows, silver 12 rows all from the first send; the second copies came in a later silver batch (inserted 0), so the cross-batch MERGE did the work |
| `make smoke-restart` (evidence v3) | PASS, all 11 checks; silver resumed at bronze version 7 and added exactly the run's 13 ids |
| MERGE duration per batch (trigger 10 s) | batch 0: 4.6 s (7,110 rows); batches 1-4 (12-14 rows): 3.5-5.0 s |
| Whole silver batch (`addBatch`) / trigger | batch 0: 9.7 s / **13.9 s (over 10 s, Spark warned "falling behind")**; batches 1-4: 7.5-9.6 s / 7.6-9.9 s |
| Spark container memory (`docker stats`, peak) | first start 1.84 GiB, smoke 1.97, smoke-dupes 2.05, smoke-restart 2.00 (bronze alone: 1.09-1.36) |
| JVM heap peak / GC share (REST `/executors`) | 0.61-0.83 GiB / 0.3-0.5 % of uptime; no threshold crossed (2.5 GiB, 1.6 GiB, 10 %) |
| On disk | silver 712 KB, control 240 KB, silver checkpoint 100 KB |
| `make test-spark` / `make test-spark-fast` | 62 tests, 6 min 45 s / 33 tests, 3 min 42 s |

Honest reading of the timing: the MERGE itself is well under the trigger, but **a silver batch
of ~12 rows costs 7.5-9.9 s in total**, mostly fixed Delta overhead (several snapshot rebuilds
per batch, the aggregation, the control write). Silver keeps up at this rate, but it has almost
no headroom at a 10 s trigger. Not tuned in this slice. The likely lever is
`spark.databricks.delta.snapshotPartitions` (50 by default; 2 in the test session cut the Spark
suite from 14 to under 7 minutes). Changing it in the job is a decision for the next slice
(made in the follow-up below: 4).

**Slice 3 follow-up (2026-11-19): where a small silver batch spends its time, and
`DELTA_SNAPSHOT_PARTITIONS=4`.**

*Method.* A throwaway harness (not in the repo) called the real `process_batch` with the
job's settings (`local[4]`, 2g, 8 shuffle partitions) on copies of the live tables (silver v4,
control v5, bronze v8) in a one-off container. The pipeline was stopped, and no Kafka write was
made. The source was 12-row batches read as Parquet, like the Delta streaming source. There
were 5 "insert" batches (new ids) and 5 "no-op" batches (ids already in silver) after a
warm-up. Spark jobs came from the REST API (`/jobs`), bucketed by the step they were
submitted in. Each configuration ran in its own JVM, in the order default, 8, 4, default.

*Breakdown with Delta's default (median of 5, insert / no-op):*

| Step | Wall ms | Spark jobs | Tasks | Job ms | Of which snapshot rebuild | Driver time outside jobs |
|---|---|---|---|---|---|---|
| persist + counts/min/max | 443 / 394 | 4 / 4 | 7 / 7 | 121 / 88 | 0 | ~320 |
| MERGE | 1,768 / 4,658 | 5 / 8 | 64 / 176 | 924 / 3,618 | (inside MERGE jobs) | ~850-970 |
| control write | 4,121 / 4,042 | 4 / 4 | 114 / 117 | 3,379 / 3,374 | 3,011 / 3,005 | ~620 |
| whole `process_batch` | 6,823 / 9,108 | | | | | |

- **Counts and min/max are already one aggregation pass** (one `agg`, one action). Spark runs
  it as 4 short jobs from the same call site (1, 1, 2, 3 stages; 7 tasks), about 0.1 s of job
  time. Folding anything further would save at most a few hundred ms, so no change.
- The time goes to **Delta snapshot rebuilds with ~51 tasks** (one job of 2.7 s in the control
  write alone) and to the MERGE's own jobs. A no-op MERGE runs more jobs than an inserting one
  (8 vs 5).

*`spark.databricks.delta.snapshotPartitions` (median `process_batch` ms, insert / no-op):*

| Setting | Insert | No-op | Control step | MERGE tasks | Harness container peak |
|---|---|---|---|---|---|
| default (50), run 1 | 6,823 | 9,108 | 4,121 | 64 / 176 | 2.82 GiB |
| 8 | 4,022 | 4,491 | 1,795 | | 2.17 GiB |
| **4** | **3,253** | **3,910** | **1,553** | 18 / 38 | 1.75 GiB |
| default (50), run 2 | 6,505 | 8,441 | 3,954 | | 2.23 GiB |

The two default runs agree within 4.7 % / 7.3 % (the noise). Against their average, 4 cuts the
median by 51 % / 55 % and 8 by 40 % / 49 %. Rule agreed beforehand: apply the faster value if
it cuts at least 20 % and the default runs agree within 10 %. So **4**. (The harness container
also copied tables and built batch tables, so its peaks are not the job's.)

*Applied as one change:* env `DELTA_SNAPSHOT_PARTITIONS` (compose default 4; empty = Delta's
default; must be an int >= 1), passed to `build_spark` by `streaming/pipeline.py` and
`streaming/bronze.py`, logged at start-up. Nothing else changed. It applies to the whole
session, so bronze and silver both use it. `spark-tools` (verify) keeps Delta's default.

*Live, before (slice 3, snapshotPartitions 50) and after (4), 11-16 row batches, 3 x `make smoke`:*

| | Before | After |
|---|---|---|
| silver MERGE | 3.5-5.0 s (median 4.1) | 1.9-2.6 s (median 2.4) |
| silver whole batch (`addBatch`) | 7.5-9.6 s (median 8.7) | 4.3-7.6 s (median 5.3, -39 %) |
| silver trigger (10 s interval) | 7.6-9.9 s | 4.4-7.8 s |
| bronze `addBatch` | 4.0-4.2 s | 2.9-5.1 s (3 batches; no clear change) |
| silver caught up after bronze (orchestrator wait, starts when bronze has the run) | 17.3 s (smoke), 14.2 / 13.1 s (smoke-dupes), 12.1 s (restart) | 13.9, 11.7, 9.6 s (smoke 1-3) |
| Spark container peak (`docker stats`) | 2.05 GiB (slice 3 max) | 1.53 GiB |
| JVM heap peak / GC share | 0.83 GiB / 0.3 % | 0.55 GiB / 0.3 % |
| correctness | PASS | PASS x3, both layers; silver rows == distinct keyed bronze ids each time |

The slowest "after" batch (7.6 s) was the first one after the restart. Silver now has headroom
at a 10 s trigger. **Caveat for Phase 10:** a lower value means fewer, larger tasks when a big
table with a long log is rebuilt. On 2026-11-19 the logs were under 15 commits; after the last
silver fault run of slice 6 they were at versions 133 (bronze), 114 (silver) and 128 (control).
Re-measure at 1-50M rows.

### D2.4 Watermark W = 60 s, late table, snapshot rule (slice 4)

**What it is.** Silver now splits every micro-batch into **on-time** rows (to silver, as in
D2.3) and **late** rows (to `data/lake/silver/late_events`, Delta, appendOnly, not
partitioned). Nothing is dropped: a late row is kept, in its own table. Code:
`streaming/watermark.py` (pure rules), `streaming/silver.py` (`plan_batch`, `write_late`,
`write_silver`, `control_row`).

**The watermark, per batch N of the silver query with id q:**
- `m_k` = max over the keyed rows (D9) of batch k of `least(event_time, kafka_timestamp)`;
  rows without a Kafka time do not count. Stored as `batch_max_clamped_event_time`.
- `wm(N) = max{m_k : k < N, same q} - W`, or **-infinity** (`watermark_used = NULL`) if
  there is no `m_k`, **or if batch N starts inside the Delta source's initial snapshot**
  (`in_initial_snapshot = true`; the snapshot rule below).
- A keyed row is **late iff `event_time <= wm(N)`** (raw event_time). `wm` only goes up.
- W = `WATERMARK_DELAY_S`, default 60, recorded in every control row. Start-up refuses a W
  that differs from the W this query id recorded (not overridable): W can only change with a
  silver checkpoint reset (new query id). That keeps "same wm on replay" exact.

**Why the clamp.** A row with a future event_time would otherwise push the wm ahead and route
normal rows to late. The clamp caps it at the producer's Kafka CreateTime. Measured on the live
bronze (2026-11-19): 5,419 of 7,202 rows have Kafka time < event_time, by up to 3,157 s, all
sent before Phase 1e2 (burst mode sent events from the future, D1.7c). Every row sent since
2026-11-15 11:00 has Kafka time >= event_time. So for current runs the clamp triggers only in
tests; on the live data it matters when silver re-reads those old rows after a reset. `least()`
skips NULL arguments in Spark 4.1.4 (pinned by a test), which is why a NULL Kafka time is
excluded explicitly instead of relying on `least`.

**The snapshot rule (found in condition C3, fixed with option A).** Silver reads at most
`SILVER_MAX_FILES_PER_TRIGGER` bronze files per batch (default 1000, which is Delta's own
default when nothing is set; now explicit). This also holds for the initial snapshot that a
new query (checkpoint reset) reads first: a reset re-reads bronze in many batches, not one.
Without a rule, batch 2 of a re-read would route older history to late. Rule: **no row is late
while silver re-reads bronze's starting snapshot.** Batch N is a snapshot batch if N == 0 or
`offsets/N-1` has `isStartingVersion: true`. It is the batch's START, not its end: measured with
Delta 4.4.1, the batch that reads the last snapshot files ends with `isStartingVersion: false`.
A missing or unreadable offset, or one without a boolean `isStartingVersion`, is never
defaulted: start-up refuses (exit 2), and inside a batch the batch fails and the pipeline exits
2. **Cost, written down:** a truly late row inside a re-read, or a tail row that shares a batch
with the last snapshot files, stays in silver.

**Write order per batch and why a crash between two writes is safe.** Plan (no writes:
snapshot status, wm, counts, dedupe, split, check `late + on-time == keyed_unique`), then late,
then silver, then the control row (txn), then Spark's `commits/N`. On a re-run, batch N gets the
same input (`offsets/N`), the same snapshot status (`offsets/N-1` never changes), the same wm
(control rows k < N never change, W is fixed) and so the same split; both data writes are
insert-if-absent and the control append is skipped by txn. Tested with a crash at each of the 4
boundaries (after the plan, after late, after silver, after the control row): the tables and
the control row equal a crash-free run on every field except `*_this_attempt`, `*_ms` and
`processed_at` (the attempt's wall-clock time), with exactly one control row. The same four
points were later hit live with a SIGKILL of the whole container (D2.6).

**Silver and late stay disjoint.** One class per id inside a batch (dedupe before split). A
late row whose id is already in silver is not inserted (`late_dup_of_silver`); the silver lookup
is pruned to the late rows' own dates +- H, so an old late row never widens silver's MERGE
window. An on-time row whose id is already in late is not inserted (`dup_of_late`; full scan
of late, which is small). This "check the other table, then insert" is safe because the silver
query is the only writer of both tables (Phase 3 replay must keep that rule). Horizon limit as
in D2.3: a silver copy more than H days from its late copy is not seen; verify's R1 reports it.

**Checkpoint reset.** The wm restarts at -infinity and stays there for the whole snapshot.
Rows already in late stay in late (tables are appendOnly; nothing moves); their on-time copies
count as `dup_of_late`. After a reset, S ∩ L = ∅, S ∪ L == all keyed bronze ids and no id
twice still hold, because every write is insert-if-absent and both tables are append-only.
Tested on a small stream (4 ids, one late, one keyless row:
`test_late_rows_replay_restart_recompute_and_checkpoint_reset`): after the reset both tables
are unchanged. Not kept: the same split as before. A clean re-split needs silver, late,
control and checkpoint reset together, which deletes data: never automatic.

**One-time upgrade of the live tables (Q1 (a)).** At start-up: missing control columns are
added with `ALTER TABLE ADD COLUMNS` (works on an appendOnly table, tested) and the late table
is created; both logged as WARNING `upgrade`. Old control rows have no `m_k`, so the wm stays
-infinity until the first upgraded batch records one. Refused (exit 2): the late table is
missing although this query already ran with a watermark ("missing after the upgrade"), or
any control row has `watermark_delay_s` ("the late table existed and is gone").

**Control columns added:** `in_initial_snapshot`, `watermark_delay_s`, `watermark_used`,
`batch_max_clamped_event_time`, `clamped_rows`, `late_rows`, `late_dup_of_silver`,
`late_inserted_this_attempt`, `late_merge_lo/hi`, `late_ms`, `ontime_rows`, `dup_of_late`,
`wm_read_ms`. `merge_lo/hi` now come from on-time rows only; `merge_skipped` means "no on-time
row to insert". Late table columns: silver's plus `watermark_used`, `lag_behind_wm_ms`.

**verify (R1-R3 moved into slice 4, Q3).** For the run's ids: R1 S ∩ L = ∅ (also over the
whole tables), R2 S ∪ L == C, R3 no id twice in silver or in late; whole tables: silver rows +
late rows == distinct keyed bronze ids. `|L|` is reported, not judged. R4-R6 and the
W-oracle check are slice 5. smoke, smoke-dupes and smoke-restart check silver ∪ late, because
back-to-back backfill runs can overlap in event time by more than W (a run spans ~100 s and
puts its last event at "now"); such routing depends on wall-clock gaps, so no test asserts it.

**Measured live (2026-11-19, after the upgrade; 1-13 row batches; 3 x `make smoke`,
`make smoke-dupes`, `make smoke-restart`, `make sim-late`).** Baseline = slice 3 follow-up
(`DELTA_SNAPSHOT_PARTITIONS=4`).

| | Baseline | Slice 4 |
|---|---|---|
| silver whole batch (`addBatch`), 11 batches | 4.3-7.6 s (median 5.3) | 6.1-15.6 s (median 8.9, +68 %); 3 of 11 over the 10 s trigger |
| of which: silver MERGE | 1.9-2.6 s (median 2.4) | 1.3-4.1 s (median 2.5) |
| of which: wm read (control table) | - | 0.8-2.3 s (median 1.4) |
| of which: late write (the one batch with late rows) | - | 1.4 s |
| first silver batch after a restart | 7.6 s | 15.6 s (upgrade start), 11.4 s (smoke-restart) |
| silver caught up after bronze (orchestrator wait) | 13.9, 11.7, 9.6 s | 22.6, 15.0, 16.2 s (smoke); 13.8 / 14.9 s (dupes); 15.2 s (restart) |
| Spark container peak (`docker stats`) | 1.53 GiB (slice 3 max 2.05) | 1.94 GiB (smokes + dupes), 2.03 GiB (sim-late); limit for this slice 2.26 |
| JVM heap peak / GC share | 0.55 GiB / 0.3 % | 0.72-0.74 GiB / 0.25-0.48 % |
| correctness | PASS x3 | every run PASS on both layers; silver + late == distinct keyed bronze ids each time |

Reading: the late path costs little when there are no late rows, but every batch now pays for
the wm read (a full read of the small control table, one file per batch) and an extra
anti-join count against late. The median stays under the 10 s trigger, with little headroom.
The control table grows by one file per batch, so we expected the wm read to grow too; the
timing follow-up below measured no growth up to 500 rows, and the cache fallback (keep the
running max in the driver, re-read once at start-up) was decided against for now.
In smoke-dupes the second send's batch routed 7 rows to late, all already in silver
(`late_dup_of_silver = 7`, nothing inserted): cross-run overlap, as predicted (plan section 1).

*Updated in Phase 2 slice 7 (final gate, 2026-11-20; `docs/evidence/phase2/gate/`):* the
first silver batch after a Spark start took 22.0 s in `make smoke` and 22.7 s in
`make smoke-restart` (`addBatch`), against 11.4 and 15.6 s in the table above. The cause was
not measured. Each gate run had only that one silver batch, so the "Revisit when" line below
has no steady batch to judge; read literally (median `addBatch` over 10 s), it is crossed. The
smoke's wm read was 2,937 ms, under that line's 3 s. The revisit is left to the Phase 10
benchmark, which the line already names.

**Timing follow-up (2026-11-19, measured with `bench/silver_steps.py` and
`bench/wm_probe.py`; method in the probes' docstrings).**
- **3-smoke median `addBatch` 10.7 s, 2 of 3 batches over the 10 s trigger** (9.3, 10.7 and
  12.2 s). Over 9 batches (13-21) the median was 9.1-9.3 s. A batch longer than the trigger
  is a delay, not a loss: the next batch starts when it ends.
- **Delta writes a checkpoint every 10 commits** (Delta's default). The silver commit that
  lands on one adds ~3 s to that batch's MERGE; the control commit that lands on one adds
  ~2 s to that batch's control write. The first batch after a start pays ~2 s of warm-up in
  the control write.
- **wm read**: no growth with the control table up to 500 rows (1.2-1.4 s after an append at
  1-10 rows and at 451-500 rows, in the probe). A read replays the last checkpoint plus at
  most 9 JSON commits (UNVERIFIED as the cause; the numbers agree). Decision: no cache, no
  anti-join change.
- **Revisit when** the median `addBatch` across a run is over 10 s, or `wm_read_ms` is over
  3 s, or at the Phase 10 benchmark (run `wm_probe.py` at 5,000 control rows).
- **Tried one aggregation for the plan step** (counts, m_N and split in one Spark action
  instead of two; 2026-11-20): 8 -> 6 jobs per batch, plan-count median -0.4 s (2,653 ->
  2,259 ms over 3 smokes each side), before-run spread 1.37 s, so within noise. **Reverted**
  by the rule set before measuring (keep only if the median drops by more than the
  before-run spread). Revisit in Phase 10. The test it added (a non-keyed copy with a lower
  offset does not hide the keyed copy) stays.

### D2.5 Oracle rules R4-R6, their gates, and `make smoke-late` (slice 5)

**What it is.** Verify (`streaming/verify_manifest.py`, pure parts in `streaming/oracle.py`)
now judges the late split, not only counts it. C = the manifest's delivered ids, S/L = silver
and late ids from C.
- **R4** `L ⊆ may_drop` (only very-late events may be routed). **R5** every keep id (on time
  or slight) is in S. R5 follows from R2 + R4; it is reported because it is the easiest to read.
- **R6** the job against its own control table, for every batch written since slice 4:
  R6a a late row has `event_time <= watermark_used` and that wm equals its batch's control
  wm; R6b a silver row has `event_time >` its batch's wm; R6c the recorded wm equals
  `max(m_k, k < N) - W` (NULL in the snapshot or with no m_k) and never goes down;
  R6d the recorded `m_k` is >= the clamped time of every row batch k inserted.
- Batches written before slice 4 (`watermark_delay_s` NULL; live: batches 0-7 of the
  first query) are counted and skipped, never judged.
- **Independence.** Verify writes the clamp and the wm formula again (about 40 lines) instead
  of importing the job's code, so a bug in the job cannot hide in its own check. Every rule
  has a tamper test that makes it FAIL (`tests/spark/test_verify_oracle.py`, 21 tests; the
  exact set of failing rules is asserted).
- **Honest limits.** R4/R5 come from the manifest, so they do not depend on the job. R6
  checks the job against what it recorded: R6d catches a recorded max that is too LOW for
  the rows a batch inserted, but not one that is too low for a duplicate copy the batch
  dropped, and nothing catches a max that is too HIGH except R6c's effect on the next wm.

**Two gates for R4/R5. Either one fails -> "not checkable: reason", never PASS, never FAIL.**
- **W check.** The manifest's `assumed_watermark_s` must equal the one W recorded in the
  control rows of every batch that inserted a run row. A v1/v2 manifest, an omitted
  `late_events` list, a pre-slice-4 batch, or two different Ws -> not checkable.
- **Isolation.** wm(N) is a max over ALL earlier batches, from every producer, so another
  run's rows can route this run's events (slice 4: smoke-dupes routed 7 rows of its second
  send). Exact condition, from the tables: `foreign_max < min_t + W`, where `min_t` = the
  earliest event_time of the run's keyed bronze rows and `foreign_max` = the largest clamped
  time of any other keyed bronze row. If it holds, a routed run event can only have been
  routed by the run's own rows, which is what R4/R5 judge. The clamp is EXACTLY the job's
  (`streaming/silver.py` `key_ok` and `clamped_event_time`): keyed rows only, and a row with
  a NULL Kafka time never counts, however far in the future its event_time is. Tamper tests
  pin both (a NULL-Kafka-time future row stays isolated; a future row counts only up to its
  Kafka time; equal to `min_t + W` is not isolated, 1 ms less is).
- **Isolation reads ALL foreign bronze rows, including rows written AFTER the run.** It is
  conservative by construction: if `foreign_max < min_t + W`, no foreign row can lift a wm to
  `min_t` or beyond, so a routed run event was routed by the run's own rows. It can say "not
  checkable" when it did not need to; it does not let another run's routing pass. So
  **re-verifying an old run after newer runs exist can say "not checkable"** (measured:
  smoke-late run 1 re-verified after five later runs, margin -2,773 s). The verdict that
  counts is the one from the run's own verify, and every e2e SUMMARY now records the R4-R6
  verdicts and both gates as of that verify.
- `make verify` exits 0 with "not checkable" printed; `make smoke-late` exits 2 (result
  "NOT CHECKABLE"). Back-to-back backfill smokes are often not checkable (their timelines end
  at "now"); that is printed, not judged.

**`make smoke-late`** (`e2e/smoke_late.py`, `simulator/config.late.yaml`: realtime x10,
4 matches, 30 % late, 20 % of those very late): one real run, then exact assertions from
`late_counts`, no fractions. Asserted (exit 1): `|S| = delivered - |L|`; every late event
found once; on time and slight: none in L, all in S; very: `L ⊆ very`, `|S ∩ very| +
|L ∩ very| = late_counts.very`; every run row inserted after the silver baseline batch.
Reported only: |L| as "x of y very events", per very event `event_time - wm`, G
(`processed_at - kafka_timestamp`), the slight margin, batch times. Warnings: no very event
routed, max G > 30 s, slight margin < 15 s.

**Measured (2026-11-20; 3 runs, Spark restarted only before run 1 by the image rebuild):**

| | run 1 | run 2 | run 3 |
|---|---|---|---|
| events (on time / slight / very) | 245 (176 / 53 / 16) | 260 (177 / 69 / 14) | 251 (195 / 39 / 17) |
| very routed (\|L\|) | 13 of 16 | 14 of 14 | 13 of 17 |
| slight routed | 0 | 0 | 0 |
| slight margin (min event_time - wm) | 34.2 s | 36.0 s | 34.6 s |
| G median / max | 16.2 / **40.6 s** | 14.1 / 25.1 s | 15.1 / 20.1 s |
| isolation margin | 2.4 h | 22 min | 4.9 min |
| silver batches, addBatch median / max | 27, 5.9 / 9.9 s | 25, 8.4 / 15.3 s | 32, 6.1 / 12.4 s |
| batches over the 10 s trigger | 0 | 9 | 2 |
| batches ending mid bronze version | 0 | 0 | 0 |
| total time | 7.6 min | 7.4 min | 9.0 min |
| container peak (limit below) | 2.593 GiB | 2.673 GiB (+0.080) | 2.661 GiB (-0.012) |
| `make timing-check`: events ahead | 0 of 245 | 0 of 260 | 0 of 251 |

- R4, R5, R6a-d PASS in all three; both gates checkable in all three. The late MERGE ran
  live in 10, 9 and 9 batches. Run 1's G max of 40.6 s is probably the first batch after the
  restart waiting for JVM start-up (UNVERIFIED: G is not reported per batch; runs 2-3,
  without a restart, stayed under 26 s). The D2.4 revisit trigger (median addBatch over 10 s across a run) did not fire.
- JVM peaks since the JVM started (Spark REST `/executors` `peakMemoryMetrics`): heap
  1.236 GiB, off-heap 0.447 GiB, GC 0.6-0.9 % of uptime. Inside the container the JVM's own
  memory was 2.37 GiB; page cache 11 MB.

**Memory stop line for smoke-late.** The slice 4 line, 2.26 GiB, was slice 3's peak plus
10 %, measured on short smokes. Run 1 crossed it (2.593 GiB) with the heap at 1.19 GiB and GC
under 1 %: a longer run lets the JVM (driver heap up to 2 GiB, `SPARK_DRIVER_MEMORY`) keep
more committed memory, which a peak from short runs does not predict. It was replaced by a
line tied to the container limit, **set before runs 2-3 and not changed after**: container
peak > 2.8 GiB (200 MB under the 3 GiB limit), or the peak growing > 0.1 GiB from one run to
the next with Spark not restarted (a leak signal), or heap > 1.6 GiB, GC > 10 %, OOM. Runs 2-3
crossed none of them. Lowering the driver memory is left for the Phase 10 benchmarks.

### D2.6 Fault injection: SIGKILL at the write boundaries, `make smoke-faults` (slice 6)

**What is injected.** A real process kill, not a Python exception: an exception would run
`finally` blocks, `plan.release()` and Spark's own error handling, which a crash does not.
`streaming/faults.py` adds four test-only points to silver's `process_batch`: `after_read`
(plan built, nothing written), `after_late` (late MERGE done), `after_silver` (silver MERGE
done), `after_control` (control row committed, `commits/N` not yet written, so it is also the
"before commits/N" point). At its point, the first batch with late AND on-time rows writes a
marker file (`data/faults/<tag>/<point>.reached`, fsync'd), logs `FAULT REACHED` and holds still.
`e2e/smoke_faults.py` sees the marker and runs `docker kill --signal KILL valkyrie-spark`. The
container's PID 1 is tini, so the kernel kills every process at once: no `finally`, no atexit, no
JVM shutdown hook. If no kill arrives within 120 s, the hook logs `FAULT FALLBACK` and SIGKILLs
the JVM and itself; smoke-faults then reports exit 6 (did not fire as claimed).
*Tradeoff:* the kill lands at a chosen boundary, not at a random instant inside a write. Torn
writes inside a single Delta commit are left to Delta's own atomic commit (not tested here).

**How the hooks stay off (F1, K4).** Active only if `VALKYRIE_FAULTS_ENABLED` is exactly `"1"` AND
a valid point and tag are set. Only `docker-compose.faults.yml` sets them, and only
`e2e/smoke_faults.py` passes that file. It is not named `docker-compose.override.yml`, which
Compose would load by itself. smoke-faults refuses to run if `COMPOSE_FILE` is set. The env is
read once at start-up; when inactive, `hit()` is one comparison of a module global (no env read,
no file I/O, no marker folder; unit test with an env that raises on any read). Active: a loud
WARNING plus a stderr banner at start-up. Tests: `tests/unit/test_faults.py` (18),
`tests/unit/test_compose_faults_isolated.py` (5); `make faults-config-check` prints the number of
`VALKYRIE_FAULT` mentions in the default compose config (must be 0). The full Spark suite (111)
passed with the hooks in place and inert, and created no faults folder.

**Data per silver point** (tiny config, ~12-14 events per run):
A = backfill ending now, caught up, then a graceful stop. While Spark is down: B (start 30 min
ago, so every event is late by >= ~26 min), C (ends now, sent >= 125 + 15 - 60 s after A's end so
that it is on time by >= 15 s), A' (A's seed again: re-stamped duplicates). Spark starts with the
fault file; the first silver batch N holds late = |B| and on time = |C| + |A'|.

**Evidence that it fired (F4), read while the job is down, else exit 6:** the marker (point, batch,
both kinds of rows); exactly one `FAULT REACHED point=X batch=N` line and no `FAULT FALLBACK` in the
killed container's logs; exit code 137 with OOMKilled false; `offsets/N` present and `commits/N`
absent; and the tables in the state the point claims (B in late from batch N or not, C in silver
from batch N or not, 0 or 1 control row for N), read from the Delta log on the laptop with pyarrow
(`e2e/faults_check.py`, a reader independent of Spark).

**Judged after the restart (F3), without the fault file (container env checked: no
`VALKYRIE_FAULT*`):** verify A, B and C (every id in silver ∪ late, none missing, none
twice; B all in late, C and A all in silver; A's kept rows are from its first send; R6a-d not
FAIL; R4/R5 are "not checkable" here, see below); the silver guard's exact (mode, T, C, P),
(resume, N-1, N-1, N) and for `after_control` (resume, N, N-1, N); batch N re-run once; exactly
one control row per batch and its fields, including which attempt wrote it
(`late_inserted_this_attempt`, `inserted_this_attempt`); `watermark_used` = max(m_k, k < N) - W;
row deltas bronze 2|A| + |B| + |C|, silver |A| + |C|, late |B|.

**Results (2026-11-20, VERIFIED; SUMMARY lines in `docs/evidence/phase2/slice6/`):**

| point | batch N | (T, C, P) at restart | control row N written by | late / on-time inserts in it | non-empty orphan files |
|---|---|---|---|---|---|
| after_read | 120 | 119, 119, 120 | the re-run | 12 / 13 | 0 |
| after_late | 122 | 121, 121, 122 | the re-run | 0 / 14 | 0 |
| after_silver | 124 | 123, 123, 124 | the re-run | 0 / 0 | 0 |
| after_control | 126 | 126, 125, 126 | the killed attempt (re-run skipped by txn) | 12 / 13 | 0 |

A first `after_read` run (batch 118) fired, but smoke-faults crashed while reading the down state
(see WHAT_BROKE), so its F4 evidence is incomplete; it is counted as "fired, evidence incomplete",
not as a pass. Its after-the-fact checks (guard, control row, verify A/B/C) agreed.

**inside_bronze (best effort).** Bronze uses the native Delta sink, so it has no hook: smoke-faults
starts Spark normally and SIGKILLs it as soon as bronze's `offsets/N` appears (polled every 10 ms),
then classifies: (i) planned, not in Delta (txn = N-1), (ii) in Delta, not committed (txn = N),
(iii) too late (`commits/N` present). Up to 3 attempts. Result: **(i) on attempt 1** (batch 133,
txn 132, no file written, 0 orphans); after the restart the bronze guard read (T, C, P) =
(132, 132, 133), and the Delta log added exactly 14 rows = the run's 14 events. State (ii) was not
reached, so "in Delta but not in the checkpoint" for bronze is covered only by the D2.2 tests.

**Orphan files (K3).** Data files on disk that no `add` in any Delta commit references. None of
the kills left an orphan with rows. Empty (0-row) orphans exist in late and control from before
slice 6 (23 and 117 at the start). Observed: control gained exactly one per committed control
row (129 rows, 129 empty orphans after slice 6's last run); late gained one in each replay
whose late MERGE inserted nothing (after_late, after_silver, after_control) and none when it
inserted rows. Why Delta leaves these 0-row files is UNVERIFIED. They are counted separately
and never deleted.

**R4/R5 are "not checkable" in fault runs, by design.** Isolation needs no foreign bronze row
within W of the run; A, B and C are sent within ~3 minutes of each other, so each is foreign to the
others. The exact per-run assertions above replace R4/R5 here.

**What we can NOT inject on this laptop (F6):** disk full, a slow or failing disk, network
partitions between Spark and Kafka, losing the single Kafka broker's data (only one broker, no
replication), clock jumps, kills at random instants inside one Parquet or Delta write, and memory
pressure from other tenants. Kafka restarts and topic recreation are not covered in Phase 2.

**Memory:** the same M1 stop lines as smoke-late (D2.5), set before the runs. Container peaks
1.50-1.94 GiB, heap peaks 0.57-0.78 GiB; none crossed. (Corrected in slice 7: this line first
said "heap peak <= 0.66 GiB", but after_silver's heap peak was 0.781 GiB.)

---

## Real-data exploration (between Phase 2 and Phase 3)

### DR.1 Real match data as a fenced, counts-only research step
- **Context:** before the Phase 3 quality gates, check the simulator's event schema against
  real data: 50 ranked matches (summary + timeline) from the public match API of a real 5v5
  MOBA game. Plain Python, 104 calls with a dev key, no pipeline change (empty diff on the
  pipeline paths). Code and findings: `research/real_match_data/`.
- **Where it lives (option B):** one folder with both scripts, tests, the findings and the
  API's legal disclaimer; the main README has one disclaimer line and a link. The wording
  audit becomes: 0 studio names outside that folder and that line. *Rejected:* (A) all
  local, which keeps the audit at 0 but is not reproducible, untested and invisible;
  (C) committed with names hidden in config, which is obfuscation and makes the disclaimer
  impossible. *Tradeoff:* the audit has one documented exception; VALKYRIE's game,
  champions and simulator stay fictional.
- **Seed: the challenger ladder of one platform.** Players are taken in the order returned,
  one page of 20 ranked solo ids each, at most 5, then a snowball. The operator's own
  account had no games in this title (exit 4, by design). 3 ladder players gave 60 ids.
  Every unexpected case stops with its own exit code instead of falling back silently.
  *Tradeoff:* a biased sample (top players, one server, overlapping): 338 distinct players
  in 500 slots, and every match shares a player. The findings describe the schema and data
  quality, not representative gameplay.
- **Counts only:** raw responses stay in gitignored `realdata/`. Committed findings hold
  counts, field paths and an allowlist of schema enums. They are written only if the totals
  reconcile and a privacy check finds none of the 1713 ids and names read from the raw files
  (with a self-test that plants one of each kind). *Why:* the repo is public, and the API's
  docs list "aggregate player stats (no specific players)" as an approved use. *Tradeoff:*
  no examples in the findings.
- **Open: the event id for real data.** 1981 events are byte-identical to an earlier one in
  the same match, and 25.1% of events tie on timestamp, so a content hash or a (type, time,
  actor) key collides. Candidate: match id + frame index + event index. That only works if
  the order is stable across fetches: **UNVERIFIED** (one fetch per match). It is to be
  tested in the real-data backfill adapter (timeline step 7); no more calls now.
- **Design note (UNVERIFIED): post-game data needs a backfill path.** A match arrives only
  after it ends, so by event time every event is late by about the match duration (median
  1652 s here), far beyond W = 60 s (D2.4). Through the realtime path it would all go to the
  late table. Step 7 decides the path.
- **Volume:** real matches have about 20x the simulator's events (median 1220 vs 62 per
  match). This is relevant to the Phase 4 memory headroom: the Spark container already peaks
  at 2.59-2.67 GiB of its 3 GiB limit on runs of about 250 events (D2.5). How memory scales
  with events per batch is UNVERIFIED.

---

## Phase 3: quality gates, dead-letter queue, replay, gold

### D3.1 A valid copy after a bad copy: detected in Phase 3, fixed in Phase 4 (planned)
- **Context:** Phase 3 sends rows that fail a hard gate to a dead-letter table (DLQ), and
  the first decision for an `event_id` is final: a later copy of an id that is still open in
  the DLQ is counted (`dup_of_dlq`, `late_dup_of_dlq`) and not inserted. If the first copy
  was corrupt and a later copy is valid, the valid event ends up only in bronze. A replay
  re-checks the DLQ's own corrupt copy, which still fails. **That is silent loss.**
- **Phase 3 (built in slices 1-2): detect and FAIL.**
  - The batch logs a WARNING and its control row keeps both counts.
  - Oracle rule **R14** (verify, its own gate code) FAILs for every open DLQ id that has a
    bronze copy passing every hard rule. It also FAILs if the stream's counts are > 0. It
    says "not checkable", never PASS, when it cannot pin bronze.
  - Tests: the real path (bad copy, then the valid copy in a later batch), a tamper test
    (exactly {R14} fails) and a negative test. Live Phase 3 runs cannot produce the case,
    since there is no duplicate injection before Phase 4 (D1.13).
- **Phase 4 (planned, with duplicate injection): a valid copy resolves the open entry.**
  - In the stream (still the single writer), the accepted copy goes to silver or late,
    classified by its own batch's watermark (R6 unchanged). Then a resolution is appended:
    `resolved_by = 'valid_copy'`, no replay id, the resolving query and batch.
  - Write order per batch: DLQ -> late -> silver -> resolutions -> control -> `commits/N`.
  - Deterministic on a re-run: "every accepted id of batch N that is still open after the
    data writes gets a resolution, insert-if-absent on `dlq_key`". The same final state
    holds whether the crash came before or after the resolution write.
  - Order-independent: valid first (the bad copy is `rejected_dup_of_accepted`), both in one
    batch (a valid copy is preferred), bad first in an earlier batch (resolved by this step).
    Every order ends with the id accepted, which Phase 4's shuffle and duplicate tests
    assert. R14 stays as the detector and must then PASS.
- **Why not in Phase 3:** the fix is only provable with duplicate injection, which D1.13
  places in Phase 4. Building it without a live test would be an unproven claim; detecting
  the gap is provable now.
- **Tradeoff:** until Phase 4, such a case fails verification loudly instead of being
  repaired. It cannot occur with Phase 3's simulator modes.

### D3.2 Slice 0: what Spark 4.1.4 and Delta 4.4.1 do, pinned before building (2026-11-26)
Throwaway probes ran in `spark-tools` (`./data` read-only) and wrote only to the container's
`/tmp`. The answers are pinned in `tests/spark/test_phase3_pins.py` (14 tests). No Kafka
writes; the live late and control tables were byte-identical before and after (sha256 of
every file).

| # | Question | Answer (VERIFIED) | Consequence for Phase 3 |
|---|---|---|---|
| P1 | absent vs null field | both parse to NULL; the difference survives only in `raw_value` | "absent = null" (FINDINGS F1) holds by construction |
| P1 | `''` or blank `event_id` | parse to `''` / `'   '` with `parse_ok` true | the C3 gap is real: today they would become silver keys |
| P1 | one field of the wrong JSON type | only that field becomes NULL, the rest is kept; no string -> int coercion (`"7"` -> NULL) | wrong-typed envelope field -> `parse_ok` false -> keyless; wrong-typed per-type field (e.g. `gold: "100"`) -> NULL -> H7. A `wrong_type` fault kind has an exact expected reason |
| P1 | `event_time` without ms, or a number | NULL | H3 |
| P1 | unknown JSON keys | dropped | S4 only sees schema fields set on the wrong type |
| P1 | "not a JSON object" | `from_json` gives a struct of NULLs, not a NULL struct; `json_object_keys(raw_value) IS NULL` exactly for malformed, array, number, `''`, truncated (false for `{}`) | H1 uses `json_object_keys` |
| P2 | ALTER ADD `array`/`map` on an append-only table | works; old rows NULL; writers without the columns still work; still append-only | `quality_flags`, `reject_counts`, `flag_counts` can be added in place |
| P2 | insert-only MERGE that inserts nothing | no commit | supports the recorded cut (plan addendum B); slice 1 tests it on the DLQ code |
| P3 | P vs C after `query.stop()` | idle: P == C. During a batch: `stop()` waits for the running `foreachBatch` function, then does not write `commits/N`, so P == C + 1 | the rules-version guard is needed; replay's "P == C" check can refuse after an ordinary `make spark-stop` |
| P4 | batch overwrite with `txnAppId`/`txnVersion` already present | skipped: no new version, old rows kept | gold's crash re-run works as planned; no fallback needed |
| P5 | `VACUUM ... RETAIN 168 HOURS DRY RUN` | lists only unreferenced files older than the retention; a referenced file is kept whatever its mtime; a dry run writes nothing | the dry run can be compared with our own orphan list |
| P5 | what VACUUM writes | two commitInfo-only commits, `VACUUM START` and `VACUUM END` (no txn) | `delta_log.reconcile` is unaffected; smoke-restart's "every commit in (V0, V1] has one txn" check would fail if VACUUM ran mid-run, so VACUUM runs only with the stream stopped |
| P5 | retention below the default | refused (`DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT`) | the safety check stays on; it is never disabled |
| P5 | time travel after VACUUM | every version still reads, count == the log's count | on append-only tables VACUUM removes only never-committed files |
| P5 | VACUUM speed | file listing uses `spark.sql.sources.parallelPartitionDiscovery.parallelism` (10,000 tasks by default): 371 s on a 6-file table on `local[1]`, 25 s at 2 | `make vacuum` and the tests set it low |
| P6 | dry run on copies of live late (v33) and control (v132) | 26 and 131 orphans, none older than 7 days on 2026-11-26 07:45 UTC, so the dry run listed 0, matching our list; a real VACUUM on the copies kept every version readable (34 and 133 checked) | the first live orphans become eligible on 2026-11-26 09:25 (control) and 2026-11-27 13:53 (late) |
| P7 | where the 0-row orphans come from (D2.6 left it UNVERIFIED) | reproduced on toy tables: an append of 1 row split over 2 partitions writes one empty file that the commit does not list (`coalesce(1)`: none); an insert-only MERGE that inserts nothing writes one empty file and no commit | matches the D2.6 pattern (one per control row; late in no-op re-runs). That each live file came from exactly these paths is UNVERIFIED. `coalesce(1)` in `write_control_row` is a one-line fix, measured in slice 6; no-op MERGEs still need VACUUM |

**Corrections to the plan from P1:**
- A row's key kind follows `key_ok`, not its rule number. A row with an absent or wrong-typed
  envelope field (`event_type`, `schema_version`, `match_id`, `match_seq`) has `parse_ok`
  false, so it is keyless (keyed by offset) although its reason is H7.
- Each rule fires only on its own condition: H5 on a non-null unknown type (a NULL type is
  H7), and H1 on `json_object_keys(raw_value) IS NULL`.

### D3.3 The memory growth line is judged after 3 warm-up runs (slice 1 baseline, 2026-11-26)
- **What happened:** the baseline for slice 1 (unchanged Phase 2 code, `make smoke` × 5 back
  to back after a Spark start, `bench/smoke_measure.py`) crossed the growth line of D2.5,
  "container peak grows > 0.1 GiB from one run to the next without a Spark restart", on
  runs 1 -> 2 and 2 -> 3. Per the plan that was a STOP.
- **Decision rule, fixed before runs 4-5:** warm-up if both growths 3 -> 4 and 4 -> 5 are
  < 0.1 GiB of container peak and the heap peak does not rise by > 0.1 GiB either;
  otherwise a leak signal in the Phase 2 code (stop, no slice 1 code).
- **Numbers (VERIFIED, `docs/plans/s1/baseline.jsonl`):**

  | Run | Container peak | Growth | Heap peak | Heap growth |
  |---|---|---|---|---|
  | 1 (starts Spark) | 1.483 GiB | | 0.496 GiB | |
  | 2 | 1.698 GiB | +0.215 | 0.568 GiB | +0.072 |
  | 3 | 1.817 GiB | +0.119 | 0.652 GiB | +0.084 |
  | 4 | 1.835 GiB | **+0.018** | 0.667 GiB | +0.015 |
  | 5 | 1.839 GiB | **+0.004** | 0.667 GiB | +0.000 |

- **Result: warm-up.** The JVM takes more memory in its first runs after a start and then
  levels off. Not a leak.
- **The line from now on:** "container peak grows > 0.1 GiB between consecutive runs
  **after 3 warm-up runs since a Spark start**". The absolute lines (peak > 2.8 GiB,
  heap > 1.6 GiB, GC > 10 %, OOM) apply to every run, warm-up included. The "after"
  measurements of each slice use the same sequence (Spark stopped, then the same runs back
  to back), so like is compared with like.

### D3.4 Slice 1: gates and the dead-letter queue in the stream (2026-11-26)
Built as planned (plan sections 1-2, addenda A and B). The choices made while building:
- **Key (C3):** `key_ok` also needs a non-blank `event_id` (after trim). A blank id is
  keyless, so it goes to the DLQ by offset (H2). Verify's two key mirrors changed with it.
  Live bronze had 0 blank ids before the change (read-only count, bronze version 140), so
  no live row moves.
- **Where the gates run:** `plan_batch` adds the gate columns before dedupe, so dedupe can
  prefer a copy that passes every hard rule, then the lowest offset (D2.3 unchanged for
  all-valid batches). `m_k` still counts every keyed row, rejected ones included (plan 1.5).
- **Write order:** DLQ -> late -> silver -> cut -> control row -> `commits/N`. New fault
  point `after_dlq` (same firing condition as the other four: late and on-time rows; slice 5
  decides whether its live run also needs DLQ rows).
- **The DLQ write:** keyless rows by `off:<topic>/<partition>/<offset>`, rejected keyed
  rows by `id:<event_id>`. A rejected copy of an id already in silver (+-H) or late is
  `rejected_dup_of_accepted` and not written. The rows are anti-joined with the DLQ first,
  and the insert-if-absent MERGE must insert exactly that many (`DlqWriteError`
  otherwise: a second writer or a bug). Nothing to reject: no read, no commit.
- **D_open** (keyed DLQ rows without a resolution) is read once per batch, after the DLQ
  write. It then also holds this batch's own rejected ids, which are never among its
  accepted ids (one copy per id), so every attempt of a batch sees the same set.
  An accepted copy whose id is in D_open is counted (`dup_of_dlq`, `late_dup_of_dlq`),
  not inserted, and logged as WARNING `valid copy stranded in DLQ` (D3.1).
- **Explicit column lists in the silver and late MERGEs** instead of `INSERT *`: an
  upgraded table has `quality_flags` at the end, a new one has it elsewhere, and a named
  list does not depend on column order.
- **Defaults for the new paths are sibling folders** of the late and control tables
  (`dead_letter`, `dlq_resolutions`, `replays`). A config that sets only the Phase 2 paths
  (tests, tools) therefore can never touch the live lake.
- **Control row:** the plan's `dlq_inserted` / `dlq_already_present` are split into keyless
  and keyed terms, so both DLQ equations of plan 2.5 can be checked (R8). Plus the cut
  (`silver_version`, `late_version`, `dlq_version`, `resolutions_version`, read from the
  Delta logs after the writes, no Spark job) and `cut_ms`, with each read's ms in the
  `silver batch` log line.
- **Rules-version guard (not overridable):** while batch P is in flight, the version
  recorded for P (its control row, else its DLQ rows) must match. Without such a record,
  batch P - 1's version must match. The first attempt's version is then unknown, and
  finishing P with P - 1's version is the one choice that cannot differ from it once a
  writer has run. A Phase 2 batch (no version) may be followed by any version.
- **A missing DLQ or resolutions table after a Phase 3 batch** is refused (overridable with
  `SILVER_ALLOW_REBUILD`, like the late table, D2.4).
- **The replays table** is created now with a fixed schema; slice 3 writes it.
- **Verify:** R8 (the equations, per Phase 3 control row) and R9 (at batch C's recorded
  cut: S, L and D_open disjoint, no `dlq_key` twice, no keyed id twice, resolved ids in S
  or L). Every smoke FAILs if the run has a DLQ row (R10, D3.5).
- **The balances of plan 2.8 stay in verify's consistency section,** not in R9. That
  section already owned D2.3's balance; it now reads "silver + late + D_open keyed ==
  distinct keyed bronze ids", plus "DLQ keyless rows == sum keyless_rows of Phase 3
  batches". Repeating them under R9 would make one fault fail two rules, and the tamper
  tests assert exact sets of failing rules.
- **Writes after the cut** are reported per table (`versions_after_cut`), not judged. R9
  reads at the cut, so it cannot see them; R1-R3 read the newest versions and still do.
  Turning it into a FAIL (a cheap second-writer detector) is an open question for the user.

### D3.5 R10 "good data is never dead-lettered"; one JVM per Spark test file (2026-11-27)
- **R10 replaces the unnamed "DLQ rows of this run" check** (manifests v1-v3 are good
  data). The old check failed with a note that named no rule, picked keyless rows by the
  run's offset range even where other producers wrote inside it, and gave no verdict.
  - **R10a:** a DLQ row (any key kind, resolved or not) whose `event_id` is one of the
    manifest's ids FAILs. Not checkable when the manifest lists no ids.
  - **R10b:** a DLQ row not caught by R10a at offsets only this run wrote FAILs. A
    partition's `[min, max]` is the run's own range when `count == max - min + 1`. Rows in
    ranges that other producers shared cannot be attributed: they are counted (`shared_rows`,
    `shared_keyless`) and the verdict is "not checkable", never PASS. Without
    `kafka_offsets` (manifest v1): not checkable.
  - What changed in strength: a good event pushed to the DLQ still FAILs, by id or at the
    run's own offsets (tamper tests in `test_dlq_verify.py` and `test_verify_oracle.py`). The one
    case that no longer FAILs is a row in an offset range other producers shared: it was
    a FAIL that could blame the run for another producer's message, and it is now
    "not checkable" with its count shown. `dlq_rows_for_run` (by id + own + shared)
    stays in the report; every smoke must show 0.
  - Every DLQ failure note now starts with its rule name (R9 or R10), as the tamper tests
    require.
- **`make test-spark` runs one pytest process per test file** (`SPARK_TESTS=path` still
  runs one process). It is a workaround for the test-JVM retention in WHAT_BROKE (holder
  not found). Tradeoff: one JVM start per file instead of one in total, against a full
  suite that no longer depends on how many tables earlier files opened. The test
  session's Spark settings, the job and verify are unchanged.
- **Memory stop line for TEST containers** (set 2026-11-27, before the measuring rerun;
  **replaced after measurement** by the gate at the end of D3.5). STOP when anonymous memory (`memory.stat` `anon`) is > 2.5 GiB, or
  on any test failure, OOM kill or non-zero exit. File cache (`file`) does not count.
  - Why not the job's line: D2.5's line (container peak > 2.8 GiB) was set for the
    streaming job. Applied to test containers it tripped on four files, but
    `test_silver.py` peaked at 2.978 GiB with at most 444 MB of live heap and 1,417 MB
    committed. Most of that peak can be file cache from the Delta tables the tests write
    into the container's `/tmp`, and the kernel reclaims file cache before it OOM-kills.
    Anonymous memory (heap, JVM native memory, Python) is what cannot be reclaimed.
    2.5 GiB leaves 0.5 GiB of the 3 GiB limit for the cache and the kernel.
  - Applied to `test_dlq.py` and `test_silver.py` (the files with the highest peaks):
    if both stay under it, no file is split. If either goes over, that file is split by
    theme, and a split file still over the line is a STOP.
  - The streaming job's line (container peak > 2.8 GiB, D2.5) is unchanged. The "after"
    runs also report the job's anon/file breakdown, as information for Phase 4, not as a
    new line.
  - **Measured, and the premise was wrong** (the same day). The file cache was small:
    0.21-0.26 GiB at most, and 25-48 MB at the moment of the anon peak. The peaks were
    anonymous memory: 2.950 GiB (`test_dlq.py`) and 2.927 GiB (`test_silver.py`), almost
    all of it the `java` process; Python held about 50 MB. Both files were split as
    planned (`test_dlq.py` / `test_dlq_recovery.py` / `test_dlq_verify.py`,
    `test_silver.py` / `test_silver_stream.py`, shared builders in `dlq_helpers.py` and
    `silver_helpers.py`). Every split file still had 2.2-2.9 GiB of anon, the 2-test file
    included. **So 2.5 GiB of anon is too tight for a JVM allowed `-Xmx2g`:** the heap may
    grow to 2 GiB and JVM native memory comes on top, however few tests a file has. The
    line was set before the facts were known, and it was the line that was wrong, not the
    tests.
- **Why the JVM is bigger than its heap** (measured on `test_dlq_recovery.py` at `-Xmx2g`,
  JVM memory MXBeans sampled every 3 s next to the container's anon memory). At the anon
  peak (3,003 MB) the java process held 2,899 MB:
  - 2,048 MB of committed heap: during a test the heap grows to the full `-Xmx`, and the
    forced GC at teardown shrinks it again, so per-test numbers under-read it;
  - 366 MB of non-heap (metaspace, code cache) and 0 MB of direct buffers;
  - 485 MB that no JVM counter covers (median 550 MB over the run; up to 576 threads).
- **A smaller test heap was tried and not kept.** With `-Xmx1536m` for tests only, all 147
  tests passed with 0 OOM kills, but three files stayed over 2.5 GiB of anon:
  `test_dlq_recovery.py` 2.525 GiB, `test_verify_oracle.py` 2.649 GiB, and
  `test_late_routing.py` 2.975 GiB. That last peak is two JVMs: one test starts the real
  pipeline, a 512m JVM, as a subprocess. All three filled the whole 1,536 MB heap. A heap
  small enough to pass the line (about 1.25 GB) would be below what they use, so it was
  reverted.
- **Decision (2026-11-27): test containers get `mem_limit` 4 GiB, and the test JVM keeps
  `-Xmx2g`, the same as the job.** This changes the earlier "no limit changes", for the
  test harness only. It failed the 3.5 GiB anon rule below and was reverted at first. It
  was then restored, after measurement, together with the new gate (end of D3.5), as the
  exact Makefile version measured in the m4g run. No new full-suite run was made for it:
  m4g is the evidence for this configuration.
  - The job (`spark`, 3 GiB, `SPARK_DRIVER_MEMORY` 2g) and verify (`spark-tools`, 1.5 GiB)
    are unchanged.
  - How: `make test-spark` sets `SPARK_MEM_LIMIT=4g` (`SPARK_TEST_MEM_LIMIT`) on its own
    `docker compose run` only.
  - Why: tests run with the same heap as the job, so they cover what the job does, and
    the anon measured (heap + about 0.85-1.1 GB native) fits under 4 GiB with room. A test
    heap below the job's would make test failures say little about the job.
  - Cost: the laptop budget. Kafka 1 + job 3 + tests 4 = 8 GiB is over the 6.5 GiB line,
    so `make test-spark` refuses with exit 2 while the job container (`valkyrie-spark`)
    runs.
- **Test-container rule, set before the 4 GiB run (replaced the 2.5 GiB anon line; itself
  replaced after measurement by the gate below): KEEP only if** all tests pass, there are 0 OOM kills, there is 0 swap use (`memory.swap.current`,
  `memory.swap.peak` and `memory.swap.events` sampled), and every file's anon peak is at
  most 3.5 GiB. Otherwise revert and STOP.
  - Swap in the earlier 3 GiB runs: **unknown per run**. Docker lets a container swap as
    much as its memory limit by default, WSL has 4 GiB of swap, and those samplers did not
    record swap. The WSL VM did swap since its boot at 09:05, which was before all of
    those runs: `/proc/vmstat` shows `pswpout` 436,925 pages, about 1.67 GiB written out.
    That counter is VM-wide, so it cannot say which run swapped. It is possible that
    "0 OOM kills" at 3 GiB relied on swap.
- **Result of the 4 GiB run (m4g): failed the 3.5 GiB anon rule; reverted, then restored
  under the new gate below.**
  - Passed: all 147 tests passed, every file exited 0 and there were 0 OOM kills.
  - Swap: 0 in every container (`swap.current`, `swap.peak` and all swap events stayed 0).
    The VM-wide `pswpout` did not move during the run (436,925 before and after).
  - Every container had the 4 GiB limit (4,294,967,296 bytes). The suite took 4,494 s
    (74 min 54 s).
  - **The anon line failed in one file.** `test_late_routing.py` reached **3.681 GiB**,
    with 14 samples over 3.5 GiB.
    - All 14 fall in the two minutes (18:28:57-18:30:54 UTC) when
      `test_upgrade_from_slice3_then_each_new_guard_refuses_with_exit_2` runs the real
      pipeline as a second JVM.
    - At the peak, the test JVM's RSS was 3.04 GB (heap committed 2,048 MB, non-heap
      390 MB, about 600 MB uncounted) and the child JVM's was 0.67 GB.
    - With one JVM, this file's highest anon was 3.143 GiB.
  - Highest anon in the other files:
    - `test_verify_oracle.py` 3.144 GiB (java RSS 3.13 GB: heap 2,048, non-heap 371,
      about 700 MB uncounted, 554 threads);
    - `test_dlq_recovery.py` 2.888 GiB;
    - all others 0.57-2.49 GiB.
    - File cache was at most 0.26 GiB in every file.
  - **Observed, cause UNVERIFIED:** with 1 GiB more room, the same test JVM grew bigger.
    Under the 3 GiB limit, `test_verify_oracle.py` never went above 3.000 GiB of container
    usage (part 4, `docker stats`; anon itself was not sampled then). Here it reached
    3.144 GiB of anon, and the test JVM alone in `test_late_routing.py` reached
    3.143 GiB. So the native part outside the heap seems to grow into the room it is
    given. A higher container limit therefore does not set a fixed peak.
- **We stop iterating on test-container memory lines (2026-11-27).** The 2.5 GiB and
  3.5 GiB anon lines were limit-minus-margin guesses. The m4g run shows that JVM non-heap
  grows into the room it is given, so a fixed margin is not a sound test gate.
- **Test-container gate (replaces both anon lines, set after measurement for the reason
  above):** all tests pass, every file exits 0, 0 OOM kills and 0 swap, all per file.
  - Every slice report gives the anon peak per file, compared with the m4g baseline
    (table below).
  - A file whose anon peak is within 0.2 GiB of the container limit (at or above 3.8 GiB
    with 4 GiB) is **flagged** for the next slice's plan. That is not a STOP.
  - The job's lines (D2.5 container peak, D3.3 growth line) are unchanged.
  - m4g baseline, anon peak per file (GiB; 4 GiB limit, `-Xmx2g`, 0 swap, 0 OOM kills):

    | File | anon peak | File | anon peak |
    |---|---|---|---|
    | test_bronze_table.py | 1.741 | test_phase3_pins.py | 1.591 |
    | test_delta_compat.py | 1.134 | test_raw_parse.py | 0.571 |
    | test_dlq.py | 2.093 | test_silver.py | 2.082 |
    | test_dlq_recovery.py | 2.888 | test_silver_stream.py | 2.490 |
    | test_dlq_verify.py | 2.461 | test_verify_manifest.py | 2.163 |
    | test_jars_lock_image.py | (too short) | test_verify_oracle.py | 3.144 |
    | test_late_routing.py | 3.681 (two JVMs) | | |

  - No file is flagged at this baseline. The highest, `test_late_routing.py`, is
    0.32 GiB under the 4 GiB limit.
  - The uncounted native memory and the thread counts are a Phase 4 item (WHAT_BROKE).

### D3.6 The job's memory step is sizing, not a leak; job heap 2g -> 1536m (2026-11-28)
- **What happened:** in slice 1's after sequence the job crossed two of its lines (D2.5,
  D3.3):
  - growth from smoke 4 to smoke 5: +0.436 GiB container, +0.400 GiB heap peak;
  - smoke-late: container peak 2.951 GiB, and `memory.current` reached the 3 GiB limit.
- **Leak or sizing? Measured on the running job** (jcmd's module,
  `java -m jdk.jcmd/sun.tools.jcmd.JCmd`; the `jcmd` launcher is not in the image). At each
  point: `GC.heap_info`, a full GC (`GC.run`), `GC.heap_info` again.

  | Point | Heap committed | Heap used | Live after full GC |
  |---|---|---|---|
  | start (job idle 7.5 h) | 570.0 MiB | 409.8 MiB | 167.0 MiB |
  | after smoke 1 | 874.0 MiB | 504.2 MiB | 197.9 MiB |
  | after smoke 2 | 884.0 MiB | 393.9 MiB | 198.7 MiB |

  - The live heap grew +30.9 MiB and then +0.8 MiB per smoke. That is under the ~50 MB
    leak line fixed before the measurement, so **not a leak**.
  - What grows is the heap that G1 commits: 570 to 880 MiB with about 200 MiB live.
- **Why it fills the container (arithmetic on measured parts):** after the full GC the
  container held:
  - heap committed: 657 MiB;
  - metaspace: 242 MiB;
  - Python: about 76 MiB;
  - other JVM native memory: about 690 MiB.

  With G1 at its full 2 GiB, that is about 2.98 GiB, the 3 GiB limit. So `-Xmx2g` can
  fill a 3 GiB container with no leak at all, and the peak and growth lines then fire.
- **Decision:** the job's heap default `SPARK_DRIVER_MEMORY` 2g -> **1536m** (compose
  default only; plan section 6, lever 3).
  - Estimated peak: 1.5 + 0.24 + 0.69 + 0.08, about 2.5 GiB, under the 2.8 GiB line.
  - The container limit stays at 3 GiB, so the laptop budget is unchanged
    (Kafka 1 + job 3 + batch 2 = 6 GiB of limits).
  - The Python fallback of "2g" in `streaming/pipeline.py` and `bronze.py` applies only
    when the variable is unset. Compose always sets it.
- **Known mismatch:** test JVMs keep `-Xmx2g`. `make test-spark` and `make test-spark-fast`
  pass `SPARK_DRIVER_MEMORY=2g` (`SPARK_TEST_DRIVER_MEMORY`). So the tests run with more
  heap than the job, and a test can pass with heap use the job would not survive. Every
  after sequence measures the job's own heap.
- **Rule for the one after sequence, fixed before it:** Spark restarted, then 3 warm-up
  smokes, smokes 4-7, then smoke-late. PASS needs all of:
  - median silver addBatch ≤ 14,832 ms, over smokes 4-7, the runs after the warm-up
    (runs 2-7 are reported next to it; smoke 1 starts Spark and is excluded, as in D3.3);
  - container peak ≤ 2.8 GiB in every run;
  - growth ≤ 0.1 GiB between consecutive smokes after the warm-up;
  - every R check PASS;
  - 0 missing and 0 duplicate ids;
  - 0 OOM kills and 0 swap.

  FAIL means no more tuning: report, a WHAT_BROKE known limit, a commit only if
  correctness passed, STOP.
- **Result of that sequence (2026-11-28, `-Xmx1536m` VERIFIED from the job's command
  line): FAIL on speed, PASS on everything else.**
  - **Speed: FAIL.** Median silver addBatch over smokes 4-7 = median(16,809, 13,552,
    16,702, 19,560) = **16,755.5 ms > 14,832 ms**. Runs 2-7 give the same median.
  - **Memory: PASS.**
    - Container peaks were 1.510-1.986 GiB in the smokes and **2.458 GiB in smoke-late**
      (≤ 2.8; it was 2.951 at 2g).
    - Growth after the warm-up: +0.063, +0.006, +0.069 and +0.073 GiB (each ≤ 0.1). The
      heap peak stayed flat at 0.744 GiB from smoke 3.
    - 0 swap, 0 OOM kills, GC 0.27-0.58 %.
  - **Correctness: PASS.** All 8 runs PASS: 351 events, 0 missing, 0 duplicates, all R
    checks PASS, 0 DLQ rows.
  - By the rule: no more tuning. Known limit in WHAT_BROKE. The heap change is committed,
    because correctness passed.

### D3.7 Slice 2: bad-data mode, manifest v4, R10c/R10d/R11/R14 (2026-11-28)
Plan `docs/plans/phase3-slice2-plan.md`, approved with these decisions:
- **Q1 A:** R11 and R14 use verify's own gate code, `streaming/oracle_gates.py`. It is
  standard-library Python that re-parses `raw_value`. It imports nothing from the job
  (`gates.py`, `silver.py`, `schemas.py`), so a bug in the job's parser or rules shows up
  as an R11 FAIL.
- **Q2:** piece 3 of the smoke-bad evidence (control `reject_counts` / `flag_counts`) is
  "not checkable" (exit 2), never PASS, when other producers' rows share the run's batches.
- **Q3:** `wrong_type` is one kind with two variants: `envelope` (`match_seq: "abc"`,
  keyless) and `field` (`gold: "100"`, keyed). The coverage search needs both.
- **Q4:** `new_region` corrupts exactly one whole match per run (the smallest
  `sha256(f"{seed}:region:{match_id}")`).
- **Q5:** the live runs are 3 × `make smoke-bad` and then 1 × `make smoke` (about 735 events).
- **Q6:** the tamper sets derived in plan section 4 are the expectation. Any difference is
  reported, never bent to fit.

**Three conditions added at approval (fixed before any code):**
1. **The watermark must ignore bad rows.** A Spark test feeds the same batches twice:
   once with good rows only, once with the good rows plus DLQ'd (`epoch_time`) and
   flagged (`future_time`) rows. `m_k` (`batch_max_clamped_event_time`) and the watermark
   must be equal in both. If the test fails, that is stream code: STOP and report. No
   stream change in this slice.
2. **No verdict on the heap line.** The heap line (≤ 1.6 GiB) cannot fire while the job
   runs with `-Xmx1536m` (D3.6), so the heap peak is reported with no verdict.
3. **Growth is compared within one target only.** Report the container-peak growth for
   smoke-bad runs 1 → 2 → 3, with no verdict. Run 4 is a different target (`make smoke`),
   so 3 → 4 cannot be judged. This replaces plan section 9.3's "growth for run 3 → 4".

**Condition 1 failed, then decided (2026-11-28): m_k skips rows past the S1 tolerance.**
- **What failed:** the condition 1 test, in backfill. A `future_time` row (+24 h) is keyed,
  so it counted in m_k, clamped to its Kafka time. In backfill that is the send time,
  after every event of the run. m_k moved by up to 2 min and the wm by 2 min (slice 2
  report, section 1). Realtime passed only because of its data.
- **Decision (project owner, option 1 narrowed):** a keyed row with
  `event_time > ingest_time + 5 min` (the S1 condition, D1.13) **never counts in m_k,
  whatever its class**: accepted, flagged or rejected. Every other D2.4/D3.4 m_k rule
  stays: keyed rows only, rejected keyed rows still count, NULL Kafka time never counts,
  and the clamp to the Kafka time stays.
- **Why it is safe:**
  - D3.4 counted rejected rows only to keep D2.4/D2.5 unchanged ("the clamp bounds the
    effect of a bad row", plan 1.5). In backfill the clamp does not bound it.
  - D2.4's "same wm on replay" holds: the condition reads only stored bronze columns
    (`event_time`, `ingest_time`) and a constant (`FUTURE_TOLERANCE_S` = 300). It does
    not depend on the rules version.
  - Live bronze had 0 such rows (read-only count, 9,549 keyed rows). Recorded control
    rows and live routing do not change.
- **Where:**
  - the job: `streaming/silver.py` `beyond_future_tolerance` and `clamped_event_time`;
  - verify, written again: `_clamped_us`, used by R6d and the isolation gate. R6d reads
    `bronze_ingest_time` from silver/late.
- **Tests:**
  - condition 1, now also "the same good rows are routed to late";
  - a boundary test: +5 min counts, +5 min 1 ms never, for accepted, flagged and
    rejected rows;
  - a pin: the exclusion equals the S1 flag on every three-way case;
  - an isolation case in `test_verify_oracle.py`.
