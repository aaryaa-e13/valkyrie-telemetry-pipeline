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
run -> find the one manifest for that seed (must be v2) -> **contiguity check** -> **pre-run
guard** -> wait for catch-up -> verify in `spark-tools` with `--fail-on-duplicates`.
The pure logic lives in `e2e/catchup.py` and is unit-tested (34 tests). Docker calls are thin
wrappers in `e2e/docker_ops.py`, each with a timeout.

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
- **UNVERIFIED claim, to re-check against Spark 4.1.4 at the start of Phase 2:** "Structured
  Streaming has no side output for too-late rows; stateful operators drop them silently." This
  comes from general knowledge of Spark, not from a test in this repo. If it is true, Phase 2
  must route too-late events to a table itself, because silent drops would break the oracle.
- **Oracle (Phase 4) with lateness.** Late events are Kafka-confirmed, so they stay in the
  manifest's delivered ids, totals and checksum; v3 only annotates them. The raw layer must
  contain every one of them (`make verify`, unchanged). For a watermark-gated layer the rule
  becomes: `accepted + late table == confirmed`, no overlap, and anything missing from
  `accepted` must be a `may_drop` event.

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
  3, e.g. the query plan or scan metrics. UNVERIFIED which.
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
