# What Broke and How I Fixed It

A log of real problems hit while building VALKYRIE. Each entry covers the symptom, the root
cause, the fix, and the lesson.

Template:

```
### <short title> (Phase N)
- Symptom:
- Root cause:
- Fix:
- Lesson:
```

---

## Phase 1

### Kafka advertised listeners: "connects, then Connection refused" (Phase 1b)
- **Status:** Prevented by design in slice (a). Reproduced on purpose in slice (b) to prove the
  design works.
- **Symptom (reproduced):** A container that bootstrapped via `host.docker.internal:9092` passed
  the metadata check (it found 6 partitions), then every send failed:
  `Connect to ipv4#127.0.0.1:9092 failed: Connection refused`. After the 10 s delivery timeout,
  the delivery callback logged `delivery failed ... _MSG_TIMED_OUT` and the event was counted as
  failed.
- **Root cause:** A Kafka client uses the bootstrap address only for its *first* connection. The
  broker then replies with its **advertised** address, and the client uses that from then on. The
  EXTERNAL listener advertises `localhost:9092`, which is correct on the laptop, but inside a
  container `localhost` is the container itself.
- **Fix (in place since slice a):** Two listeners, each advertising an address that is valid for
  its audience:
  - `INTERNAL://kafka:29092` for containers on the Docker network (the simulator container, and
    Spark in slice c),
  - `EXTERNAL://localhost:9092` for tools running on the laptop (the `.venv` simulator, tests).
  The Compose simulator service sets `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`.
  Both paths are verified: the laptop run delivered 14/14 and the container run 10/10.
- **Lesson:** "Can connect" is not "can produce". Always test from where the client actually
  runs. Also, the bootstrap address and the advertised address are different things.

### Console consumer `--property` no longer works in Kafka 4.x (Phase 1b)
- **Symptom:** Most tutorials use `kafka-console-consumer.sh --property print.key=true`. In
  Kafka 4.3.2 the help output only lists `--formatter-property`.
- **Root cause:** Kafka 4.x renamed the console tools' options (`--property` became
  `--formatter-property` for the consumer and `--reader-property` for the producer).
- **Fix:** Use `--formatter-property print.key=true --formatter-property print.partition=true`.
  This is in `make consume`.
- **Lesson:** Check `--help` for your exact version instead of copying commands from old
  blog posts.

### Console consumer ends with `ERROR ... TimeoutException` (Phase 1b, not a bug)
- **Symptom:** After printing all messages, the consumer prints
  `ERROR Error processing message, terminating consumer process ... TimeoutException`.
- **Root cause:** `--timeout-ms 8000` means "exit if no message arrives for 8 s". The tool
  reports that normal exit as an error.
- **Fix:** None needed. Check the final line, `Processed a total of N messages`.

### Spark job crashed at start: `mkdir of file:/data/lake/raw/game_events/_spark_metadata failed` (Phase 1c)
- **Symptom:** `make spark-up` started the container, which exited with code 1 a few seconds
  later. The only clue was a Java stack trace (`java.io.IOException: mkdir ... failed`). My
  "wait for the first batch" check also hung, because it only looked for JSON log lines.
- **Root cause:** `./data` was owned by **root** (`drwxr-xr-x 0 0`), while the container runs as
  uid 1000. An earlier `docker compose run spark ...` (the connector check) mounted `./data`
  before it existed, and **Docker creates a missing bind-mount folder as root**. The later
  `mkdir -p data` in the Makefile did nothing, because the folder already existed.
- **Fix:**
  1. Bind mounts use the long syntax with `bind.create_host_path: false`, so a missing folder
     now fails loudly instead of silently becoming root-owned.
  2. A `make dirs` target creates `data/` and `manifests/` as your user, and every Spark and
     simulator target depends on it.
  3. Repaired the existing empty folder with `chown 1000:1000` from a root container (no sudo,
     nothing deleted).
  4. The job now catches start-up failures and logs one structured `failed to start query`
     line, so the cause is visible in `make spark-logs` without reading a stack trace.
- **Lesson:** With bind mounts, file ownership is decided by the uid inside the container plus
  whoever created the folder first. Create host folders yourself and run containers as a
  non-root uid that matches yours. When waiting for a log line, also watch for failure.

### `date +%s%3N` printed nanoseconds, not milliseconds (Phase 1d)
- **Symptom:** A seed meant to be epoch milliseconds (13 digits) came out as
  `1794679946598044635` (19 digits).
- **Root cause:** This Ubuntu ships **uutils coreutils 0.8.0** (the Rust rewrite) as `date`. It
  ignores the `%3N` width modifier and always prints all 9 nanosecond digits. GNU `date`
  would print 3.
- **Fix:** The orchestrator (slice d2) generates seeds in Python
  (`time.time_ns() // 1_000_000`) instead of shelling out to `date`.
- **Lesson:** Don't assume GNU coreutils behaviour in scripts. Generate values in the language
  you control, and check the output's shape.

### Status offsets parser crashed on dict input (Phase 1d, caught by unit test)
- **Symptom:** `TypeError: unhashable type: 'dict'` in `parse_offsets`.
- **Root cause:** `raw in {"None", "null", ""}` checks membership in a set, which hashes `raw`.
  A dict can't be hashed.
- **Fix:** Only do that check when `raw` is a string.
- **Lesson:** This is why the status logic is pure and unit-tested on the laptop. The bug would
  otherwise have appeared only as a warning inside the running Spark job.

### "Crashed" label for a clean stop (Phase 1d2, caught in a live failure test)
- **Symptom:** After `make spark-stop`, the timeout report said `CRASHED` although the container
  had exited with code 0.
- **Root cause:** The first version of the hint treated "not running" as one case.
- **Fix:** New label `stopped` for exit code 0 without OOM. `crashed` is now for non-zero exit,
  OOMKilled, or a missing container. Unit-tested.
- **Lesson:** Diagnostics must separate "someone stopped it" from "it died". They lead to
  different actions.

### My own wait loop never ended: `pgrep -f` matched itself (Phase 1d2, test tooling)
- **Symptom:** A shell loop `while pgrep -f "seed 777001"; do sleep; done` ran forever.
- **Root cause:** `pgrep -f` matches the full command line of every process, including the
  shell running the loop, whose command line contains the same text.
- **Fix:** Stopped the loop and checked the simulator's own log for `run complete` instead.
  Nothing else was affected: the `chmod` after the loop had not run yet.
- **Lesson:** Wait on a specific PID or an explicit completion signal (log line, file), not on
  a text pattern that your own command contains.

### "Container missing" crashed the orchestrator: Docker's error text changed case (Phase 1d3)
- **Symptom:** The first `make smoke-restart` after a WSL restart (Spark container gone) ended
  with `FAIL (exit 2): docker inspect valkyrie-spark failed: error: no such object` instead of
  starting Spark.
- **Root cause:** `container_state` looked for `"No such"`, but this Docker version prints
  `error: no such object:` in lowercase. d2 never hit it live, because the container always existed.
- **Fix:** `is_missing_container_error()` matches case-insensitively ("no such object" / "no such
  container"). Unit-tested with all three spellings.
- **Lesson:** Don't match exact error text from another tool. And every branch, even the "missing"
  one, needs to be run live once: the unit tests passed because they never called docker.

### A unit test hung forever: my fake clock could not reach the due time (Phase 1e2, test tooling)
- **Symptom:** `pytest tests/unit` printed dots, then stopped. No failure, no error; the command
  hit its 120 s timeout. Before the new sender tests the whole suite took under 3 s.
- **Root cause:** The realtime sender waits in a loop: "while time remains, wait". Kafka's
  `poll(timeout)` may return early, so the test used a fake clock whose `wait(s)` advanced time
  by only `0.3 * s`. The remaining time then shrank 1 s, 0.7 s, 0.49 s, ... and never reached
  zero. Once the step was smaller than what a float near 1000.0 can represent, adding it changed
  nothing at all and the loop spun forever. The bug was in the test helper, not in the sender:
  a real early return still lets real time pass.
- **How it was found:** The hang started exactly when `tests/unit/test_sender.py` was added, and
  only one test used `early_return=0.3`. Re-running with `timeout 100 pytest -x` kept the run
  bounded while narrowing it down.
- **Fix:** The fake clock now advances by at least 0.5 ms per wait:
  `min(seconds, max(seconds * early_return, 0.0005))`.
- **Lesson:** A fake clock must not be able to stand still, or any "wait until" loop becomes an
  infinite loop. And run new tests under a timeout, so a hang shows up as a failure.
- **Also learned:** I tried to stop the hung run with `pkill -f "pytest tests/unit"`. The pattern
  matched the shell that was running `pkill` itself and killed that too. Same trap as the
  `pgrep -f` entry above.

### `make verify` rejected a correct v3 manifest: stale Spark image (Phase 1e3)
- **Symptom:** Right after the reader learned manifest v3 (and its unit tests passed),
  `make verify MANIFEST=manifests/run_20261115T105353Z_seed721.json` failed with
  `manifest_version 3 not supported (supported: [1, 2])` and `make: *** [verify] Error 2`.
- **Root cause:** `common/manifest_reader.py` is copied into the Spark image at build time
  (`COPY common/ common/`). `make verify` runs inside that image but did not rebuild it. So the
  laptop had the new reader and the container still had the old one. `make smoke` hid this,
  because it rebuilds both images first.
- **How it was found:** The message listed `[1, 2]` although the file on disk said `{1, 2, 3}`:
  two different copies of the code. `docker-compose.yml` showed that `spark-tools` uses the
  image and mounts only `data/` and `manifests/`, not the source code.
- **Fix:** `make verify` now depends on a new `spark-image` target (a quiet, cached build that
  takes a few seconds when nothing changed).
- **Lesson:** Code baked into an image is a snapshot. Every target that runs code from an image
  must build it first, or it will sooner or later test old code. The useful part: the stale
  reader failed loudly instead of misreading a format it did not know. That is why unknown
  manifest versions are rejected.

### Seed 733 sent twice: every event twice in raw Parquet (Phase 1e3, operator error the tool allowed)
- **Symptom:** `make verify` for the seed 733 run reported `"found_unique_ids": 14`,
  `"rows_for_these_ids": 28`, `"duplicate_rows": 14`: every event was in Parquet exactly twice.
  (The result was still PASS, because the raw layer only reports duplicates unless
  `--fail-on-duplicates` is given.)
- **Root cause:** The same simulator command was run twice with `--seed 733`. `event_id` is
  derived from the seed (D1.6), so the second run produced the same 14 ids and Kafka stored
  them as 14 new messages. Nothing was wrong in Kafka or Spark; the raw layer keeps everything
  (D1.14). The simulator gave no hint that the seed had been used.
- **How it was found:** `manifests/` held two files for the seed,
  `run_20261115T111409Z_seed733.json` and `run_20261115T111755Z_seed733.json`. They have the
  same `delivered_checksum` (the same events) but different `kafka_offsets` (partition 0:
  1043-1049 and 1050-1056; partition 4: 1230-1236 and 1237-1243). Same ids at two different
  offset ranges means two sends, not a pipeline that wrote rows twice.
- **Fix:** (1) The simulator now logs a WARNING when a manifest for the same seed already
  exists. It does not fail, because resending a seed on purpose is a legitimate duplicate test.
  (2) `make sim-realtime` and `make sim-late` generate a fresh seed unless `SEED=` is given.
- **Second trap, the `$(ls ...)` gotcha:** `make verify MANIFEST=$(ls manifests/run_*_seed733.json)`
  expands to *two* paths once two files match. `MANIFEST` gets the first one, and make treats
  the second as a target to build and only prints `make: Nothing to be done for
  'manifests/run_20261115T111755Z_seed733.json'`. So one run is verified and the other is
  silently skipped. Paste the exact file name from the simulator's `run complete` line, or pick
  the newest explicitly: `ls -t manifests/run_*_seed733.json | head -1`.
- **Lesson:** Deterministic ids make re-sending a seed look like duplicates, which is exactly
  what Phase 2 dedupe needs for testing, but the tool must say so. To tell "sent twice" from
  "written twice", compare the Kafka offsets of the runs. And a shell glob in a command is only
  safe while exactly one file matches.

## Phase 2

### Delta brought two jars that duplicate Spark's classes (Phase 2 slice 1)
- **Symptom:** The first Delta build passed, and the planned C2 check (no artifact in two
  versions, by file name) would have passed too. But the build log added
  `commons-logging-1.3.0.jar` and `shims-0.9.25.jar`. A class-level scan of all jars showed
  that `commons-logging` shares 5 classes (`org/apache/commons/logging/Log`, `LogFactory`, ...)
  with Spark's `jcl-over-slf4j-2.0.17.jar`, and `shims` shares `org/roaringbitmap/ArraysShim`
  with Spark's `RoaringBitmap-1.3.0.jar`.
- **Root cause:** `fetch_jars.sh` skips a jar whose artifact Spark already ships, but it still
  adds that jar's own dependencies. Spark leaves those out on purpose: it replaces JCL with
  `jcl-over-slf4j`, and RoaringBitmap 1.x no longer needs `shims`. Skipping
  `hadoop-client-runtime` and `RoaringBitmap-0.9.25` was right; adding their dependencies was
  not.
- **Why it matters:** With the same class in two jars, the JVM loads whichever jar comes first
  on the classpath, and the order of `jars/*` is not specified. Today Spark's copies won. A
  different build or filesystem could flip that silently.
- **Fix:** `--exclude-packages commons-logging:commons-logging,org.roaringbitmap:shims` for the
  Delta fetch (after proving that nothing references the 22 JCL-only classes). C2 now fails if
  any class in a jar we add also exists in another jar. The only ignored class is Spark's
  `UnusedStubClass`.
- **Lesson:** "Skip what is already there" is a rule about jar names, but conflicts happen
  between classes. Check classes, and check what a skipped jar drags in.

### My first strict C2 would have failed on stock Spark (Phase 2 slice 1, my own mistake)
- **Symptom:** I proposed "fail on any class that is in two jars" and later ran the planned
  rule "no artifact in two versions". Both fail on Spark 4.1.4 itself, before Delta: 1,364
  duplicate classes in 7 groups of Spark's own jars, and two same-name pairs
  (`hive-shims-0.23` / `hive-shims`, `jline` 2.x / 3.x).
- **Root cause:** I wrote the rule before measuring the stock baseline.
- **Fix:** C2 fails only on problems involving jars we add. Stock findings are printed and
  compared with a recorded baseline, and only WARN on drift (D2.1).
- **Lesson:** Before writing a "must be zero" check, measure what the unchanged system gives.
  Otherwise the check is either wrong on day one or gets weakened in a hurry.

### Jackson fetch failed: "expected 1 cache entry ... found 0" (Phase 2 slice 1)
- **Symptom:** The third `fetch_jars.sh` run (`jackson-datatype-jdk8:2.21.2`) stopped the build:
  `ERROR: expected 1 cache entry for com.fasterxml.jackson.datatype_jackson-datatype-jdk8-2.21.2.jar,
  found 0`.
- **Root cause:** To record the exact coordinate, the script looks the jar up in Ivy's cache
  under `<group>/<module>/jars/`. Jackson publishes with Maven packaging `bundle` (OSGi), and
  Ivy stores those under `<module>/bundles/`.
- **Fix:** Look in both `jars/` and `bundles/`. The guard that failed (exactly one cache entry,
  else stop) stays. It turned a wrong assumption into a clear build error instead of a
  missing coordinate.

### The jar-name rule misreads names like `spark-sql-kafka-0-10_2.13` (Phase 2 slice 1, latent since Phase 1)
- **Symptom:** A new C2 audit compares the artifact name parsed from each file name with the
  real coordinate. It found 2 of 45 mismatches: `spark-sql-kafka-0-10_2.13-4.1.4.jar` parses as
  `spark-sql-kafka-0` (and the token provider the same way).
- **Root cause:** The rule "artifact = file name before the first `-<digit>`" treats `-0-10`
  as a version. `hive-shims-0.23-2.3.10.jar` has the same problem.
- **Impact:** None in this build. With the real name, both jars would still be added, because
  Spark ships no connector. But a wrongly parsed name can wrongly skip a jar in a future fetch.
- **Fix:** No parser change. C2 checks every add/skip decision against the real coordinate and
  fails if a misparse would change one. The limitation is recorded in D2.1.

### My check script reported the wrong MERGE metrics for a no-op MERGE (Phase 2 slice 1, test tooling)
- **Symptom:** C4 printed `numTargetRowsInserted: 2` for the *second* (replayed) MERGE, which
  inserts nothing.
- **Root cause:** An insert-only MERGE that inserts nothing writes no Delta commit. So
  `history(1)` still showed the first MERGE.
- **Fix:** C4 and C11 compare versions before and after, and report "no commit" when nothing
  was written.
- **Lesson:** "The latest history entry" means "the latest commit", not "my operation". This
  matters for slice 3, which will read per-batch MERGE metrics.
- *Updated in slice 7:* slice 3 takes the inserted-row count from the MERGE's own result row
  (`num_inserted_rows`, `streaming/silver.py:574` and `:616`), not from `history()`, so this
  trap does not apply there.

### Deleting `commits/N` without its `.crc` made Spark report "concurrent queries" (Phase 2 slice 3, my test)
- **Symptom:** `test_silver_guard_refusals_and_crash_state` failed with `[STREAM_FAILED] ...
  Multiple streaming queries are concurrently using .../cp_silver/commits`. Only one query was
  running.
- **Root cause:** The test simulated "crash after the control row, before `commits/N`" by
  deleting `commits/N`, but left the checksum file `.N.crc` behind. Hadoop's local file system
  writes `commits/N` by rename and also creates `.N.crc`. The leftover checksum made the
  rename fail, and Spark reports any failure to create a commit file as a concurrent writer.
  The bronze version of this test already removed both files; I did not copy that line.
- **Fix:** delete `.N.crc` too. The guard decision itself was right (`resume`) before the
  failure.
- **Lesson:** a "simulated crash" must leave exactly what a real crash leaves. A real crash
  before `commits/N` leaves neither file.

### The Spark test suite was slow because Delta rebuilds snapshots with 50 tasks (Phase 2 slice 3)
- **Symptom:** the first full `make test-spark` of slice 3 took 25 minutes. One test with 11
  small MERGEs took 239 s.
- **Root cause:** every Delta operation rebuilds the table snapshot with
  `spark.databricks.delta.snapshotPartitions` = 50 tasks. The test session runs `local[1]`, so
  that is about 5 s per operation, even on tiny tables (seen in the REST API: "Compute
  snapshot" executions of ~5 s; GC only 1.6 % of uptime).
- **Fix:** the test session sets it to 2 (`tests/spark/conftest.py`). Full suite 62 tests in
  6 min 45 s (was 14 min for 46 tests); the MERGE test 239 s -> 52 s. The job still uses
  the default; see D2.3 for why that matters for silver's batch time.
- *Updated in slice 7:* since the slice 3 follow-up the job uses 4 too
  (`DELTA_SNAPSHOT_PARTITIONS`, D2.3).

### "A reset's first batch reads all of bronze" was false: Delta caps a batch at 1000 files (Phase 2 slice 4, my plan)
- **Symptom:** none in production yet. Found while answering the mentor's condition C3 before
  any slice 4 code: a Spark test put 1005 one-row files into bronze and ran the real silver
  query from an empty checkpoint. It printed `batches=[(0, 1000), (1, 5)]`.
- **Root cause:** silver read bronze with no `maxFilesPerTrigger` / `maxBytesPerTrigger`, and
  my plan assumed that meant "no limit". Delta then uses a default of 1000 files per batch,
  also for the initial snapshot. With the watermark, batch 1 of a re-read would have routed
  older history to late.
- **Fix (mentor's choice A):** the cap is explicit (`SILVER_MAX_FILES_PER_TRIGGER`, 1000), and
  `wm = -infinity` for every batch that starts inside the initial snapshot (D2.4). Tests use a
  cap of 2 and run in seconds.
- **Lesson:** an unset option is not "unlimited"; check the library's default.

### The last snapshot batch ends with `isStartingVersion: false` (Phase 2 slice 4, found by a probe)
- **Symptom:** none (found before writing the rule, condition A1). With 5 snapshot files and a
  cap of 2, `offsets/2` (the END of the batch that read the last snapshot file) was
  `{"reservoirVersion":2,"index":-1,"isStartingVersion":false}`.
- **Root cause:** Delta moves the offset past the snapshot as soon as the snapshot is used up.
  A rule based on the batch's own `offsets/N` would have given that snapshot batch a normal
  watermark.
- **Fix:** the rule reads where the batch STARTS, `offsets/N-1` (batch 0 always starts in the
  snapshot). Tests cover 5 files / cap 2, the snapshot ending mid-batch with a tail file, and
  ending exactly at a batch boundary.

### Crash-point tests failed on `bronze_ingest_time` (Phase 2 slice 4, my test)
- **Symptom:** all four `test_any_crash_point_then_replay_equals_a_crash_free_run` cases failed:
  the tables differed only in `bronze_ingest_time`.
- **Root cause:** the test built each batch with `to_raw_rows(...)`, which stamps
  `current_timestamp()` every time the DataFrame is evaluated. The reference run and the
  crashed run therefore had different "bronze" rows. A real replay re-reads the same bronze
  rows.
- **Fix:** the test writes each batch to a small Delta table once and reads it back for every
  attempt. Then all four crash points matched the crash-free run.
- **Lesson:** same as slice 3's `.crc` mistake: a simulated replay must replay the same input.

### A new test broke an old one through the shared Spark session (Phase 2 slice 4, my test)
- **Symptom:** in the full `make test-spark`, `test_stream_dedupes_and_writes_a_control_row_for_every_batch`
  failed with `SnapshotStatusError: offsets/4 is gone`, an error that only my new test raises.
  It passed when the silver tests ran alone.
- **Root cause:** `test_snapshot_status_error_makes_the_pipeline_exit_2` leaves a failed streaming
  query in the session-wide `spark.streams`. The old test calls `awaitAnyTermination(1)`, which
  re-raises the exception of any query that terminated with an error since the last
  `resetTerminated()`.
- **Fix:** the new test calls `spark.streams.resetTerminated()` in a `finally`.
- **Lesson:** the Spark test session is shared state; a test that makes a query fail must clean
  up after itself.

### smoke-faults lost its own evidence on the first run (Phase 2 slice 6, my script)
- **Symptom:** the first `after_read` fault run fired (marker for batch 118, container SIGKILLed),
  then smoke-faults crashed with `pyarrow ArrowInvalid: No match for FieldRef.Name(late_rows)`
  while reading the control table with the job down. Its `finally` restarted Spark, so the
  down-state tables and the killed container's logs were gone.
- **Root cause:** the control table's schema grew in slice 4; Delta reads older files with the
  new columns as NULL, but my pyarrow reader asked every file for every column.
- **Fix:** read each file's own schema and fill missing columns with None (unit test); write
  the killed container's logs and the down state to files right after the kill; catch any
  crash so the SUMMARY is still written. One extra, approved Kafka write re-ran `after_read`.
- **Lesson:** evidence that only exists while the system is down must be saved first, before
  any code that can fail runs.

### A failing test was committed because a pipe hid pytest's exit code (Phase 2 slice 6)
- **Symptom:** commit `3329f8b` contained a failing isolation test; the command chain had
  looked green.
- **Root cause:** `pytest ... | tail` returns `tail`'s exit code, so `&& git commit` ran anyway.
- **Fix:** `96c4adf` fixed the test; command chains now start with `set -o pipefail`.
- **Lesson:** in a shell pipeline the exit code is the last command's unless `pipefail` is
  set. Read the test result line itself before committing.

### A fresh clone failed `make test-unit`: pyarrow imported but not pinned (Phase 2 slice 7, my mistake in slice 6)
- **Symptom:** the slice 7 fresh-clone check (`git clone` into an empty folder, `make venv`,
  `make lint`, `make test-unit`) stopped at collection: `ModuleNotFoundError: No module named
  'pyarrow'` in `tests/unit/test_faults_check.py`. Lint and the other 503 unit tests passed.
  In my working copy all 519 passed.
- **Root cause:** slice 2 dropped `pyarrow==26.0.1` from `requirements/dev.txt`, because
  nothing imported it then (D2.2). Slice 6 added `e2e/faults_check.py`, which reads Parquet
  files with pyarrow, and its unit test, but did not add the pin back. My `.venv` still had
  pyarrow from before: removing a line from a requirements file does not uninstall anything.
- **Fix:** `pyarrow==26.0.1` is pinned again (it has no dependencies of its own). A scan of
  every third-party import in tracked `.py` files, per environment (laptop venv, simulator
  image, Spark image), found no other missing pin. Its other hits are `pyspark`/`delta`
  imports inside functions that only run in the Spark image. The Spark image's pip gate still
  passes (`requirements/spark.txt` unchanged). A new clone then passed lint and all 519 unit
  tests, and the tests left `git status` clean.
- **Lesson:** a long-lived venv hides a missing pin; only a fresh environment shows it.
  **For Phase 10:** CI starts from a fresh checkout every time, so running `make venv lint
  test-unit` there would have caught this on the commit that introduced it.


## Real-data exploration (between Phase 2 and Phase 3)

Code in `research/real_match_data/` (its README names the API and carries the disclaimer).
None of this touches the pipeline.

### A network error on the first call was reported as "account not found" (fetch script, my code)
- **Symptom:** in a unit test, two network errors on the account lookup ended the run with
  exit 2, "account lookup failed (not found or unreadable)", as a setup error.
- **Root cause:** the client returns "skip this item" both for a 404 and for a second
  network/5xx error, and the caller read every skip as "not found".
- **Fix:** the client records the last status; only a real 404 means "not found", anything
  else stops with exit 1. Caught by the tests before the first commit (`76ffc7e`). The
  account lookup was later replaced by the ladder seed, which kept the same rule.
- **Lesson:** "no result" has more than one cause; keep the cause next to the result.

### A test fixture field name broke the wording audit (fetch script tests, my mistake)
- **Symptom:** the slice 7 wording audit (0 studio names outside the research folder) found
  one hit in `tests/unit/test_realdata_fetch.py`.
- **Root cause:** a fake ladder entry used the API's real field name for a player's display
  name, which contains the studio name.
- **Fix:** renamed to a neutral `playerName` before the commit (`3e227b0`); test files take
  such names from the module's constants.
- **Lesson:** run the audit before each commit, not only at the end of a phase.

### The network edge blocked urllib's default User-Agent (live fetch)
- **Symptom:** every live run got HTTP 403 in 0.1 s, body `error code: 1010`. The same key
  worked in the browser API explorer.
- **Root cause:** found by the operator with curl, one change at a time: curl's default
  User-Agent got 200; `-A "Python-urllib/3.11"` got 403. The edge in front of the API
  blocks urllib's default User-Agent.
- **Fix:** every call sends an explicit, honest User-Agent
  (`valkyrie-research/0.1.0 (...)`, no browser string) (`4a88b14`). A test checks the header
  on every call, also over real HTTP to 127.0.0.1. The next run: 104 calls, all 200.
- **Lesson:** change one variable at a time. The block came from the edge, not from the API.

### A 403 from the edge was reported as "key rejected" (fetch script, my code)
- **Symptom:** the script told the operator to check the API key, and the key was fine.
- **Root cause:** every 401/403 was treated as a key problem, and the error body was not
  logged, so the `1010` in it was not seen.
- **Fix:** only a 401/403 whose body is the API's JSON status object means "key rejected"
  (exit 3). Any other body stops as `edge_blocked` (exit 6, "blocked by the network edge
  (User-Agent?)"). Every error logs the first 100 characters of its body, with the key
  masked in the whole body before the cut (`4a88b14`).
- **Lesson:** log the error body (safely). It had the answer from the first run.

### Operator incident: the API key was pasted at the shell prompt
- **What happened:** the key was once pasted at the shell prompt instead of the hidden
  `read -rsp` prompt. It landed in a local terminal log and in shell history.
- **Fix:** the key was rotated (a new dev key). No key value is in the repo: the key scan
  after each fetch (grep for the key in the repo and the logs folder) printed no files.
- **Lesson:** type the key only at the hidden prompt; run the key scan after every fetch.
  The terminal log and shell history are outside the repo and were left to the operator.

## Phase 3

### My VACUUM probe "showed" VACUUM deleting a referenced file (Phase 3 slice 0, my probe)
- **Symptom:** the first VACUUM probe on a toy table reported `referenced_old_file_kept:
  false`, so VACUUM seemed to have deleted a file the table still used.
- **Root cause:** the probe picked "a referenced file" by its name prefix (`part-0`). Delta
  gives the 0-row orphan files the same kind of name, and the one it picked was an orphan.
- **Fix:** take the path from the Delta log's live `add` actions. The re-run kept the file,
  and the pin test does it this way (`tests/spark/test_phase3_pins.py`).
- **Lesson:** identify Delta files through the log, never by their names. A surprising
  result is checked again before it is believed.

### VACUUM took 6 minutes on a 6-file test table (Phase 3 slice 0)
- **Symptom:** the VACUUM pin test took 371 s; the Spark progress bar showed a stage of
  10,000 tasks.
- **Root cause:** VACUUM lists files with `spark.sql.sources.parallelPartitionDiscovery.
  parallelism` tasks (10,000 by default). On `local[1]` each tiny task costs time.
- **Fix:** a test fixture sets it to 2 and restores it afterwards: 25 s. `make vacuum`
  (slice 6) sets it too.
- **Lesson:** when something small is slow, look at the stage's task count first.

### Delta's own output broke my probe's JSON on stdout (Phase 3 slice 0, my probe)
- **Symptom:** reading the probe's JSON failed with "Expecting value: line 1 column 1".
- **Root cause:** VACUUM prints `Deleted N files and directories ...` to stdout from the JVM
  side, before the probe printed its JSON.
- **Fix:** the probe output was read from its first `{` line. `make vacuum` (slice 6) will
  write its report to a file, not rely on stdout.
- **Lesson:** stdout is not only yours when a library runs in the same process.

### The DLQ write failed with INCOMPATIBLE_COLUMN_TYPE (Phase 3 slice 1)
- **Symptom:** the first Spark test of the DLQ write failed: "UNION can only be performed
  on tables with compatible column types. The 4th column ... STRING ... ARRAY<STRING>".
- **Root cause:** the rejected keyed rows were anti-joined with silver and late on
  `event_id` (`join(other, "event_id", "left_anti")`). A join with a USING column puts that
  column first, so the rejected rows no longer had the DLQ column order. `union` matches
  columns by position, not by name.
- **Fix:** `unionByName` in the DLQ write. Verify's run-row count unions only `dlq_key`.
  The test that caught it (`tests/spark/test_dlq.py`, one batch with every rule) stays.
- **Lesson:** after a join, never `union` by position. Phase 2 code was safe only because
  it never unioned joined frames.

### The Spark test JVM ran out of heap after ~120 tests (Phase 3 slice 1) — KNOWN LIMIT
- **Symptom:** the full `make test-spark` (one pytest process, 147 tests) failed every
  test from test 124 on (`test_verify_oracle.py`) with `SparkOutOfMemoryError:
  [UNABLE_TO_ACQUIRE_MEMORY] Unable to acquire 262144 bytes of memory, got 0`. 7 tests
  failed in one run, 24 in the next (3 more tests). The same tests pass alone
  (`exp-isolated`: 2 tests, 6 min 40 s). No assertion failed: it was memory only.
- **Numbers** (test container: 3 GiB limit = 3,221,225,472 bytes, `-Xmx2g`, `local[1]`;
  live heap measured after a forced GC by a local pytest plugin):
  - Full suite: live heap 127 MB after test 10, 823 MB after 40, 1,121 MB after 70,
    1,259 MB after 100. The container reached its 3 GiB limit in every full run.
  - What stays alive (JVM class histogram, after test 18 of `test_dlq.py`): 163 `DeltaLog`
    objects (one per table path any test opened), 163 `Snapshot`, 153 `ShuffleDependency`
    (153 shuffles still registered, never cleaned), 287 `QueryExecution`. The heap is
    mostly Catalyst plan objects (`TreeNode` lambdas, `BestEffortLazyVal`: 225 MB at test
    135 of the full run). About 5 MB of live heap per table path.
- **Standard fixes tried, one at a time** (`test_dlq.py`, 19 tests; live heap after test
  1 -> 13 -> 18, MB; reference run 75 -> 553 -> 819). None stopped the growth:
  - Spark UI keeping 50 executions instead of 1000: 67 -> 503 -> 790.
  - Spark UI off in the test session: 70 -> 544 -> 821.
  - `DeltaLog.clearCache()` after every test: 67 -> 447 -> 540 (after test 16). Delta's
    cached state RDDs went to 0, but the heap kept growing (about 28 MB per test instead
    of about 44).
  - Stop active streaming queries and `catalog.clearCache()` after each test: 74 -> 547
    -> 818. There were already 0 active queries and 0 cache-manager entries after every
    test, so there was nothing to release.
  - Python `gc.collect()` after each test (Py4J proxies held by cycles): 64 -> 535 -> 805.
    Live Py4J proxies stayed between 50 and 260 the whole time, so Python does not pin
    the plans.
- **Root cause: NOT FOUND (time-boxed).** Something inside the JVM keeps each opened
  table path's DeltaLog, snapshot and their query plans (with their shuffles and
  broadcasts) alive, and it is not Delta's `deltaLogCache`, the Spark UI, the cache
  manager, active streams or Python. A heap-dump walk to the GC root was started and
  stopped at the time box. Phase 3 crossed the 2 GB heap because each test lake now has 3
  more tables (DLQ, resolutions, replays) and the suite has about 40 more tests (belief,
  not measured).
- **Fix (a workaround, not a root-cause fix):** `make test-spark` runs one pytest process,
  so one JVM, per test file. Nothing piles up across files. `SPARK_TESTS=path` still runs
  one process. The test session's Spark config is unchanged, and so are the job's and
  verify's.
- **Known limit, Phase 4 item:** the live job opens about 7 fixed paths, so the same
  retention would be bounded there. That is a belief: its growth line was flat in slice 1's
  baseline (+0.004 GiB between runs), but long runs were never measured. **Phase 4: watch
  the live job's container and heap growth line on the long chaos runs** (D3.3's rule), and
  reopen this if it grows.
- **Test containers, after the split (D3.5):**
  - Test containers get 4 GiB, and `make test-spark` refuses while the job runs.
  - A test JVM with `-Xmx2g` holds more than its heap. In the m4g run, `test_verify_oracle.py`
    held heap 2,048 MB, non-heap about 370 MB and about 700 MB that no JVM counter covers,
    with 550-630 threads.
  - That uncounted part grows into the room the container gives it, so a fixed
    limit-minus-margin anon line was dropped as a test gate. The gate is now: all pass,
    exit 0, 0 OOM kills and 0 swap per file.
- **Phase 4 item (not now):** find the uncounted native memory with JVM Native Memory
  Tracking (`-XX:NativeMemoryTracking=summary` on a test JVM), and watch thread counts per
  file and in the live job.
- **Lesson:** one long-lived test JVM is shared state too. Measure live heap per test
  (after a GC) before reading an out-of-memory error as a code bug.
