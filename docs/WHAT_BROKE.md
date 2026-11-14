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
