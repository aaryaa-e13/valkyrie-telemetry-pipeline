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
