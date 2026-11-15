"""Graceful-restart check: Spark resumes exactly at its committed offsets.

Steps:
    1. preconditions (Kafka up; Spark started if it is not running)
    2. baseline: wait until Spark has committed everything Kafka holds
    3. graceful stop (assert exit code 0); record E0/B0/Q0/R0 and Parquet rows
    4. send a new-seed run while Spark is down (contiguity, run.min == E0)
    5. restart Spark            6. wait for catch-up (d2 rules + diagnostics)
    7. verify --fail-on-duplicates, and count Parquet rows again
    8. restart checks (e2e/restart.py): no re-read, no skip, Parquet delta

Spark is ALWAYS left running: a `finally` block restarts it if needed, even
after a failure or Ctrl-C. If that fails, the last lines say how to start it.

Assumes nobody else writes to the topic during the run; if someone does, the
run.min == E0 check stops with exit 4. Hard kills are Phase 4.

Exit codes: 0 pass, 1 verify failed, 2 setup, 3 not caught up, 4 another
writer / contiguity, 5 a restart rule failed (stop exit code, re-read, skip,
Parquet delta, or cannot prove).

Usage:
    python -m e2e.smoke_restart
    python -m e2e.smoke_restart --timeout 2           # force a catch-up timeout
    python -m e2e.smoke_restart --check-evidence data/e2e/smoke_restart_X.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from e2e import catchup, restart
from e2e.docker_ops import (
    CommandError,
    compose_stop_spark,
    compose_up_spark,
    container_logs,
    container_state,
    topic_end_offsets_raw,
)
from e2e.orchestrator import (
    EXIT_CONTIGUITY,
    EXIT_SETUP,
    EXIT_TIMEOUT,
    KAFKA_CONTAINER,
    MODES,
    SPARK_CONTAINER,
    Abort,
    Run,
    add_common_args,
    say,
    send_run,
    verify_report,
    wait_for_catch_up,
)
from e2e.parquet_count import count_rows
from streaming.status import read_status

EXIT_RESTART = 5


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="VALKYRIE graceful-restart check")
    add_common_args(p)
    p.add_argument("--topic", default=os.environ.get("TOPIC_NAME", "game-events"))
    p.add_argument("--lake-path", type=Path, default=Path("data/lake/raw/game_events"))
    p.add_argument("--evidence-dir", type=Path, default=Path("data/e2e"))
    p.add_argument("--baseline-timeout", type=float, default=60.0)
    p.add_argument("--start-timeout", type=float, default=90.0,
                   help="seconds to wait for 'query started' when (re)starting Spark")
    p.add_argument("--check-evidence", type=Path,
                   help="only re-run the checks on a saved evidence file")
    return p.parse_args(argv)


def wait_query_started(since: float, timeout: float) -> tuple[bool, str]:
    """Wait for a 'query started' log line written after `since`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        facts = catchup.parse_log_lines(container_logs(SPARK_CONTAINER, since=since))
        if facts.started_run_ids:
            return True, f"query started (run_id {facts.started_run_ids[-1]})"
        state = container_state(SPARK_CONTAINER)
        if not state.running and state.status != "created":
            return False, f"container {state.describe(datetime.now(UTC))}"
        time.sleep(1)
    return False, f"no 'query started' line within {timeout:.0f}s"


def start_spark(timeout: float) -> tuple[bool, str]:
    since = time.time()
    res = compose_up_spark()
    if res.returncode != 0:
        return False, f"docker compose up failed: {res.stderr.strip()[-300:]}"
    return wait_query_started(since, timeout)


def wait_baseline(args: argparse.Namespace) -> dict[int, int]:
    """Wait until committed end >= Kafka end everywhere. Returns Kafka end offsets."""
    kafka_end = restart.parse_kafka_end_offsets(
        topic_end_offsets_raw(KAFKA_CONTAINER, args.topic), args.topic)
    deadline = time.monotonic() + args.baseline_timeout
    while True:
        snap = catchup.snapshot_from_status(read_status(args.status_path))
        committed = snap.end_offsets.get(args.topic, {}) if snap else {}
        pending = restart.baseline_pending(kafka_end, committed)
        if not pending:
            return kafka_end
        if time.monotonic() > deadline:
            state = container_state(SPARK_CONTAINER)
            raise Abort(EXIT_TIMEOUT, f"baseline not reached in {args.baseline_timeout:.0f}s "
                        f"(Spark {state.describe(datetime.now(UTC))}): "
                        + "; ".join(pending))
        time.sleep(args.poll)


def scenario(args: argparse.Namespace, run: Run, ev: dict[str, Any]) -> None:
    """Steps 1-7; fills `ev` (evidence) as it goes. Raises Abort on early failure."""
    timeout = args.timeout if args.timeout is not None else MODES["restart"]["timeout"]

    say("[1/8] preconditions")
    if not container_state(KAFKA_CONTAINER).running:
        raise Abort(EXIT_SETUP, "Kafka is not running; run `make up`")
    if not container_state(SPARK_CONTAINER).running:
        ok, why = start_spark(args.start_timeout)
        if not ok:
            raise Abort(EXIT_SETUP, f"could not start Spark: {why}")
        say(f"      started Spark: {why}")

    say("[2/8] baseline: waiting until Spark has committed everything in the topic")
    ev["kafka_end_at_baseline"] = wait_baseline(args)
    say(f"      kafka end offsets {ev['kafka_end_at_baseline']}")

    say("[3/8] graceful stop (SIGTERM, 60 s grace)")
    ev["spark_stopped_by_us"] = True
    t = time.monotonic()
    res = compose_stop_spark()
    state = container_state(SPARK_CONTAINER)
    if state.running:
        raise Abort(EXIT_RESTART, f"Spark still running after `compose stop`: {res.stderr}")
    ev["stop_exit_code"] = state.exit_code
    ev["stop_s"] = round(time.monotonic() - t, 1)
    status = read_status(args.status_path)
    snap = catchup.snapshot_from_status(status)
    if not status or snap is None or args.topic not in snap.end_offsets:
        raise Abort(EXIT_SETUP, "no usable status file after the stop: cannot record E0")
    ev.update(e0=snap.end_offsets[args.topic], b0=snap.batch_id, q0=snap.query_id,
              r0=status["last_batch"].get("run_id"))
    ev["parquet_before"] = count_rows(args.lake_path).as_dict()
    say(f"      exit code {state.exit_code} after {ev['stop_s']}s; E0 {ev['e0']}; B0 {ev['b0']}; "
        f"Parquet rows {ev['parquet_before']['rows']}")

    say("[4/8] sending a new-seed run while Spark is down")
    manifest = send_run(run, args, stream=False)
    assert manifest.kafka_offsets is not None
    if manifest.topic != args.topic:
        raise Abort(EXIT_SETUP, f"manifest topic {manifest.topic!r} != {args.topic!r}")
    ev.update(run_id=manifest.run_id, manifest=str(manifest.path),
              run_count=manifest.events_delivered,
              run_offsets={str(p): [r.min, r.max, r.count]
                           for p, r in sorted(manifest.kafka_offsets.items())})
    say(f"      {manifest.events_delivered} events, offsets "
        + ", ".join(f"p{p} {r.min}..{r.max}" for p, r in sorted(manifest.kafka_offsets.items())))
    problems = catchup.check_contiguity(manifest.kafka_offsets)
    e0 = snap.end_offsets[args.topic]
    writers, unprovable = restart.run_start_problems(manifest.kafka_offsets, e0)
    if problems or writers:
        raise Abort(EXIT_CONTIGUITY, "another writer was active while Spark was down:\n  "
                    + "\n  ".join(problems + writers), short="another writer / contiguity: "
                    + "; ".join(p.split(":")[0] for p in problems + writers))
    if unprovable:
        raise Abort(EXIT_RESTART, "cannot prove the restart:\n  " + "\n  ".join(unprovable))

    say("[5/8] restarting Spark")
    restart_t = time.time()
    res = compose_up_spark()
    if res.returncode != 0:
        raise Abort(EXIT_SETUP, f"docker compose up failed: {res.stderr.strip()}")

    say(f"[6/8] waiting for catch-up (timeout {timeout:.0f}s, includes JVM start-up)")
    ev["catch_up"] = wait_for_catch_up(manifest, snap, args, timeout, verbose=False)
    ev["status_after"] = read_status(args.status_path)
    ev["new_run_ids"] = list(catchup.parse_log_lines(
        container_logs(SPARK_CONTAINER, since=restart_t)).started_run_ids)
    ev["parquet_after"] = count_rows(args.lake_path).as_dict()
    say(f"      caught up {ev['catch_up']['wait_s']}s after restart "
        f"(batch {ev['catch_up']['batch_id']}); Parquet rows {ev['parquet_after']['rows']}")

    say("[7/8] verify --fail-on-duplicates (spark-tools, read-only data)")
    _, report = verify_report(manifest)
    ev["committed_rows_after"] = report.get("parquet_total_rows")
    ev["verify"] = {k: report.get(k) for k in ("result", "expected_unique_ids",
                                                "found_unique_ids", "missing_ids",
                                                "duplicate_rows", "notes")}


def check_and_report(ev: dict[str, Any]) -> tuple[int, list[restart.Check]]:
    checks = restart.evaluate(ev)
    say(restart.format_checks(checks))
    return restart.exit_code_for(checks), checks


def ensure_spark_left_running(args: argparse.Namespace) -> bool:
    """Restart Spark if it is not running. Returns True if it is running at the end."""
    if container_state(SPARK_CONTAINER).running:
        return True
    say("      Spark is not running; starting it again (finally block)")
    ok, why = start_spark(args.start_timeout)
    if ok:
        say(f"      Spark restarted: {why}")
        return True
    say(f"      Spark is NOT running ({why}). Start it with: make spark-up")
    return False


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.check_evidence:
        ev = json.loads(args.check_evidence.read_text(encoding="utf-8"))
        say(f"re-checking evidence {args.check_evidence}")
        code, _ = check_and_report(ev)
        say(f"RESULT {'PASS' if code == 0 else 'FAIL'} (exit {code})")
        return code

    t0 = time.monotonic()
    seed = args.seed if args.seed is not None else time.time_ns() // 1_000_000
    run = Run(mode="restart", seed=seed)
    ev: dict[str, Any] = {"evidence_version": restart.EVIDENCE_VERSION, "topic": args.topic,
                          "seed": seed}
    summary: dict[str, Any] = {"scenario": "smoke-restart", "seed": seed}
    code = 0
    try:
        scenario(args, run, ev)
        say("[8/8] restart checks")
        code, checks = check_and_report(ev)
        summary["checks"] = {c.name: c.ok for c in checks}
    except Abort as exc:
        code = exc.code
        say(f"\nFAIL (exit {code}): {exc}")
        summary["error"] = exc.short
    except (CommandError, OSError, ValueError) as exc:
        code = EXIT_SETUP
        say(f"\nFAIL (exit {code}): {exc}")
        summary["error"] = str(exc)
    except KeyboardInterrupt:
        code = 130
        summary["error"] = "interrupted"
    finally:
        try:
            summary["spark_left_running"] = ensure_spark_left_running(args)
        except CommandError as exc:
            summary["spark_left_running"] = False
            say(f"      could not check Spark ({exc}). Start it with: make spark-up")
        if ev.get("run_id"):
            args.evidence_dir.mkdir(parents=True, exist_ok=True)
            path = args.evidence_dir / f"smoke_restart_{ev['run_id']}.json"
            path.write_text(json.dumps(ev, indent=2) + "\n", encoding="utf-8")
            summary["evidence"] = str(path)
        summary.update(run_id=ev.get("run_id"), result="PASS" if code == 0 else "FAIL",
                       exit_code=code, total_s=round(time.monotonic() - t0, 1))
        say(f"SUMMARY {json.dumps(summary, separators=(',', ':'))}")
    return code


if __name__ == "__main__":
    sys.exit(main())
