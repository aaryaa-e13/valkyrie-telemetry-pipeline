"""End-to-end run: simulator -> Kafka -> Spark -> bronze + silver Delta -> verify.

Steps:
    1. seed (Python, epoch ms)      5. contiguity check                    (exit 4)
    2. start Spark (idempotent)     6. wait for bronze, then silver        (exit 3)
    3. baseline status snapshot     7. verify bronze + silver (spark-tools) (exit 0/1)
    4. simulator run + manifest     8. silver evidence: MERGE timing + pruning (report only)

Silver catch-up (DECISIONS D2.3): once bronze has the run, the bronze table's
current version V is read from its Delta log; silver has the run when its
committed Delta source offset is past V (e2e/catchup.check_silver_caught_up).

Exit codes:
    0 PASS
    1 verify failed (missing ids or duplicates)
    2 setup problem (Kafka down, simulator failed, unusable manifest,
      status file inconsistent with the topic)
    3 Spark did not catch up (timeout, or the container stopped)
    4 contiguity violation (another producer wrote into the run's offsets)

Usage:
    python -m e2e.orchestrator --mode smoke
    python -m e2e.orchestrator --mode demo --matches 20
    python -m e2e.orchestrator --mode smoke --timeout 2        # force a timeout
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.delta_log import current_version, table_id
from common.manifest_reader import Manifest, ManifestError, load_manifest
from e2e import catchup, silver_checks
from e2e.docker_ops import (
    CommandError,
    compose_up_spark,
    container_logs,
    container_state,
    run_simulator,
    run_verify,
)
from streaming.status import read_status

EXIT_PASS, EXIT_VERIFY, EXIT_SETUP, EXIT_TIMEOUT, EXIT_CONTIGUITY = 0, 1, 2, 3, 4
SPARK_CONTAINER = "valkyrie-spark"
KAFKA_CONTAINER = "valkyrie-kafka"
LOG_TAIL = 40

MODES: dict[str, dict[str, Any]] = {
    "smoke": {"config": "simulator/config.tiny.yaml", "timeout": 90.0, "matches": None},
    "demo": {"config": "simulator/config.yaml", "timeout": 180.0, "matches": 20},
    # Used by e2e.smoke_restart; the timeout includes JVM start-up after the restart.
    "restart": {"config": "simulator/config.tiny.yaml", "timeout": 120.0, "matches": None},
    # Used by e2e.smoke_late (realtime, ~5-7 min of sending); the timeout counts from
    # the end of the send, so 180 s is generous.
    "late": {"config": "simulator/config.late.yaml", "timeout": 180.0, "matches": None},
}


class Abort(Exception):
    """Stop the run with an exit code and a message."""

    def __init__(self, code: int, message: str, short: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.short = short or message.splitlines()[0]


@dataclass
class Run:
    """Mutable state of one orchestrated run, also used for the JSON summary."""

    mode: str
    seed: int
    summary: dict[str, Any] = field(default_factory=dict)


def say(msg: str = "") -> None:
    """Human-readable progress line (stdout, flushed for live output)."""
    print(msg, flush=True)


def add_common_args(p: argparse.ArgumentParser) -> None:
    """Flags shared by e2e.orchestrator and e2e.smoke_restart."""
    p.add_argument("--config", help="simulator config (default depends on mode)")
    p.add_argument("--matches", type=int, help="override number of matches")
    p.add_argument("--sim-rate", type=float, default=0.0, help="simulator events/s (0 = max)")
    p.add_argument("--seed", type=int, help="default: epoch milliseconds")
    p.add_argument("--timeout", type=float, help="catch-up timeout in seconds")
    p.add_argument("--poll", type=float, default=1.0, help="status poll interval (s)")
    p.add_argument("--trigger-interval-s", type=float,
                   default=float(os.environ.get("E2E_TRIGGER_INTERVAL_S", "10")),
                   help="Spark trigger interval, used only for the timeout hint")
    p.add_argument("--no-start-spark", action="store_true",
                   help="do not start the Spark container (to test failure handling)")
    p.add_argument("--status-path", type=Path, default=Path(
        os.environ.get("E2E_STATUS_PATH", "data/status/bronze.json")))
    p.add_argument("--silver-status-path", type=Path, default=Path(
        os.environ.get("E2E_SILVER_STATUS_PATH", "data/status/silver.json")))
    p.add_argument("--bronze-path", type=Path, default=Path("data/lake/bronze/game_events"))
    p.add_argument("--silver-path", type=Path, default=Path("data/lake/silver/game_events"))
    p.add_argument("--late-path", type=Path, default=Path("data/lake/silver/late_events"))
    p.add_argument("--silver-timeout", type=float, default=60.0,
                   help="seconds to wait for silver after bronze has the run")
    p.add_argument("--spark-ui", default=os.environ.get("E2E_SPARK_UI", "http://localhost:4040"))
    p.add_argument("--manifest-dir", type=Path, default=Path("manifests"))
    p.add_argument("--json-summary", type=Path, help="also write the summary JSON to this file")


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="VALKYRIE end-to-end run")
    p.add_argument("--mode", choices=["demo", "smoke"], default="smoke")
    add_common_args(p)
    return p.parse_args(argv)


# --- steps ---------------------------------------------------------------------


def ensure_services(start_spark: bool) -> None:
    kafka = container_state(KAFKA_CONTAINER)
    if not kafka.running:
        raise Abort(EXIT_SETUP, f"Kafka is {kafka.describe(datetime.now(UTC))}; run `make up`")
    if start_spark:
        res = compose_up_spark()
        if res.returncode != 0:
            raise Abort(EXIT_SETUP, f"could not start Spark:\n{res.stderr.strip()}")
    spark = container_state(SPARK_CONTAINER)
    say(f"      kafka: {kafka.describe(datetime.now(UTC))}; "
        f"spark: {spark.describe(datetime.now(UTC))}")


def send_run(run: Run, args: argparse.Namespace, stream: bool) -> Manifest:
    """Run the simulator and load the manifest it wrote for our seed."""
    config = args.config or MODES[run.mode]["config"]
    extra: list[str] = []
    matches = args.matches if args.matches is not None else MODES[run.mode]["matches"]
    if matches is not None:
        extra += ["--matches", str(matches)]
    if args.sim_rate:
        extra += ["--rate", str(args.sim_rate)]
    before = set(args.manifest_dir.glob(f"run_*_seed{run.seed}.json"))  # smoke-dupes
    res = run_simulator(config, run.seed, extra, stream=stream)
    if res.returncode != 0:
        tail = "\n".join((res.stdout + res.stderr).splitlines()[-15:])
        raise Abort(EXIT_SETUP, f"simulator exited with {res.returncode}:\n{tail}")

    found = sorted(set(args.manifest_dir.glob(f"run_*_seed{run.seed}.json")) - before)
    if len(found) != 1:
        raise Abort(EXIT_SETUP, f"expected exactly one NEW manifest for seed {run.seed}, "
                                f"found {[str(f) for f in found]}")
    try:
        manifest = load_manifest(found[0])
    except (ManifestError, OSError) as exc:
        raise Abort(EXIT_SETUP, f"unusable manifest: {exc}") from exc
    if manifest.kafka_offsets is None or not manifest.topic:
        raise Abort(EXIT_SETUP, f"{found[0]} is manifest v{manifest.version} without "
                                "kafka_offsets/topic; cannot wait for catch-up")
    return manifest


def snapshot(path: Path) -> tuple[catchup.StatusSnapshot | None, bool]:
    """(snapshot, file_exists) of the status file."""
    return catchup.snapshot_from_status(read_status(path)), path.exists()


def wait_for_catch_up(
    manifest: Manifest, baseline: catchup.StatusSnapshot | None, args: argparse.Namespace,
    timeout: float, verbose: bool,
) -> dict[str, Any]:
    """Poll the status file until the run is committed. Raises Abort(3) otherwise."""
    assert manifest.kafka_offsets is not None and manifest.topic is not None
    offsets, topic = manifest.kafka_offsets, manifest.topic
    wait_start_wall = time.time()
    start = time.monotonic()
    first = catchup.check_caught_up(baseline, snapshot(args.status_path)[0], offsets, topic)
    remaining_at_start = {row.partition: row.remaining for row in first.lag}
    last_report = start
    while True:
        current, _ = snapshot(args.status_path)
        result = catchup.check_caught_up(baseline, current, offsets, topic)
        waited = time.monotonic() - start
        if result.caught_up:
            if verbose:
                say(catchup.format_lag_table(result.lag, remaining_at_start))
            return {"wait_s": round(waited, 1), "batch_id": current.batch_id if current else None,
                    "notes": list(result.reasons)}
        spark = container_state(SPARK_CONTAINER)
        if not spark.running or waited >= timeout:
            raise Abort(EXIT_TIMEOUT, timeout_report(
                manifest, result, remaining_at_start, spark, waited, timeout,
                wait_start_wall, args))
        if verbose and time.monotonic() - last_report >= 5:
            pending = sum(r.remaining for r in result.lag)
            say(f"      ... {waited:4.0f}s  {pending} offsets pending "
                f"({'; '.join(result.reasons)})")
            last_report = time.monotonic()
        time.sleep(args.poll)


def wait_for_silver(args: argparse.Namespace, verbose: bool) -> dict[str, Any]:
    """After bronze has the run: wait until silver has consumed bronze's current version."""
    bronze_v = current_version(args.bronze_path)
    if bronze_v is None:
        raise Abort(EXIT_SETUP, f"no bronze Delta log at {args.bronze_path}")
    bronze_id = table_id(args.bronze_path)
    start = time.monotonic()
    wait_start_wall = time.time()
    last_report = start
    while True:
        snap = catchup.delta_snapshot_from_status(read_status(args.silver_status_path))
        result = catchup.check_silver_caught_up(snap, bronze_id, bronze_v)
        waited = time.monotonic() - start
        if result.caught_up:
            return {"wait_s": round(waited, 1), "bronze_version": bronze_v,
                    "batch_id": snap.batch_id if snap else None, "notes": list(result.reasons)}
        spark = container_state(SPARK_CONTAINER)
        if not spark.running or waited >= args.silver_timeout:
            logs = container_logs(SPARK_CONTAINER, since=wait_start_wall)
            facts = catchup.parse_log_lines(logs, query="silver")
            reason = ("Spark container stopped while waiting for silver" if not spark.running
                      else f"silver timed out after {waited:.0f}s "
                           f"(limit {args.silver_timeout:.0f}s)")
            raise Abort(EXIT_TIMEOUT, "\n".join([
                f"silver did not catch up with bronze version {bronze_v}: {reason}",
                f"  not caught up: {'; '.join(result.reasons)}",
                f"  container    : {SPARK_CONTAINER} {spark.describe(datetime.now(UTC))}",
                f"  status file  : {args.silver_status_path}",
                f"  during wait  : {len(facts.batch_ids)} silver 'batch progress' lines, "
                f"{len(facts.errors)} ERROR lines {list(facts.errors)[-3:]}",
                f"--- last {LOG_TAIL} lines of {SPARK_CONTAINER} logs ---",
                *(container_logs(SPARK_CONTAINER, tail=LOG_TAIL) or ["(no logs)"]),
                "--- end of logs ---"]), short=f"silver did not catch up: {reason}")
        if verbose and time.monotonic() - last_report >= 5:
            say(f"      ... {waited:4.0f}s  silver: {'; '.join(result.reasons)}")
            last_report = time.monotonic()
        time.sleep(args.poll)


def silver_evidence(args: argparse.Namespace, since_wall: float) -> dict[str, Any]:
    """MERGE timing (all silver batches since the run started) and pruning evidence."""
    records = silver_checks.silver_batch_records(container_logs(SPARK_CONTAINER,
                                                                since=since_wall))
    return {"merge_timing": silver_checks.merge_timing(records, args.trigger_interval_s),
            "late": silver_checks.late_summary(records),
            "merge_scan": silver_checks.merge_scan_evidence(args.spark_ui, args.silver_path,
                                                            records)}


def timeout_report(
    manifest: Manifest, result: catchup.CatchUp, remaining_at_start: dict[int, int],
    spark: catchup.ContainerState, waited: float, timeout: float, wait_start_wall: float,
    args: argparse.Namespace,
) -> str:
    """Everything needed to tell crashed / stuck / slow / status-not-updating apart."""
    now = datetime.now(UTC)
    current, exists = snapshot(args.status_path)
    kafka = container_state(KAFKA_CONTAINER)
    wait_logs = catchup.parse_log_lines(container_logs(SPARK_CONTAINER, since=wait_start_wall))
    label, why = catchup.diagnose(
        container=spark,
        remaining_at_start=sum(remaining_at_start.values()),
        remaining_now=sum(r.remaining for r in result.lag),
        status=current, wait_logs=wait_logs, waited_s=waited,
        trigger_interval_s=args.trigger_interval_s,
    )
    reason = ("Spark container stopped while waiting" if not spark.running
              else f"timed out after {waited:.0f}s (limit {timeout:.0f}s)")
    tail = container_logs(SPARK_CONTAINER, tail=LOG_TAIL)
    parts = [
        f"Spark did not catch up on run {manifest.run_id}: {reason}",
        "",
        f"  likely cause : {label.upper()} - {why}",
        f"  container    : {SPARK_CONTAINER} {spark.describe(now)}",
        f"  kafka        : {KAFKA_CONTAINER} {kafka.describe(now)}",
        f"  status file  : {args.status_path}: "
        f"{catchup.describe_status_file(current, exists, now)}",
        f"  not caught up: {'; '.join(result.reasons) or '-'}",
        f"  during wait  : {len(wait_logs.batch_ids)} 'batch progress' log lines "
        f"(batch ids {list(wait_logs.batch_ids)[-5:]}), "
        f"{wait_logs.status_write_warnings} status write warnings, "
        f"{len(wait_logs.errors)} ERROR lines",
        "",
        catchup.format_lag_table(result.lag, remaining_at_start),
        "",
        f"--- last {LOG_TAIL} lines of {SPARK_CONTAINER} logs ---",
        *(tail or ["(no logs: container missing)"]),
        "--- end of logs ---",
    ]
    return "\n".join(parts)


def verify_report(manifest: Manifest, versions: list[int] | None = None,
                  fail_on_duplicates: bool = True) -> tuple[int, dict[str, Any]]:
    """Run verify_manifest in spark-tools; (exit code, JSON report), even if it FAILs.

    `versions`: also report bronze row counts at these Delta versions.
    `fail_on_duplicates`: bronze duplicates fail (silver duplicates always fail).

    Raises:
        Abort: verify produced no JSON report (crash or unusable manifest).
    """
    extra = ["--versions", ",".join(str(v) for v in versions)] if versions else []
    res = run_verify(manifest.path.as_posix(), extra, fail_on_duplicates=fail_on_duplicates)
    try:
        return res.returncode, json.loads(res.stdout)
    except ValueError:
        tail = "\n".join((res.stdout + res.stderr).splitlines()[-20:])
        raise Abort(EXIT_SETUP if res.returncode == 2 else EXIT_VERIFY,
                    f"verify exited {res.returncode} without a JSON report:\n{tail}") from None


def summarize_verify(report: dict[str, Any]) -> dict[str, Any]:
    """The verify keys that go into the SUMMARY line (bronze top level + silver)."""
    out = {k: report.get(k) for k in (
        "result", "expected_unique_ids", "found_unique_ids", "missing_ids", "duplicate_rows")}
    sv = report.get("silver") or {}
    out["silver"] = {k: sv.get(k) for k in (
        "result", "found_unique_ids", "silver_found_ids", "late_found_ids", "in_both_ids",
        "duplicate_rows", "duplicate_event_ids", "late_duplicate_event_ids",
        "ids_in_both_tables", "total_rows", "late_total_rows")}
    o = sv.get("oracle") or {}
    out["silver"]["oracle"] = {   # R4-R6 verdicts and both gates, as of this verify (D2.5)
        "verdicts": o.get("verdicts"),
        "w_check": (o.get("w_check") or {}).get("reason") or (o.get("w_check") or {}).get("w_s"),
        "isolation_margin_ms": (o.get("isolation") or {}).get("margin_ms"),
        "isolation": ((o.get("isolation") or {}).get("reason") or "isolated") if o else None}
    cons = sv.get("consistency") or {}
    out["silver"]["consistency"] = {k: cons.get(k) for k in (
        "checkable", "silver_rows_upto_batch", "late_rows_upto_batch",
        "dlq_open_keyed_rows_upto_batch", "bronze_keyed_distinct_ids",
        "control_excluded_total", "bronze_keyless_rows")}
    dl = sv.get("dlq") or {}
    out["silver"]["dlq"] = {   # Phase 3: good data must give 0 DLQ rows for the run
        "verdicts": dl.get("verdicts"), "dlq_rows_for_run": dl.get("dlq_rows_for_run"),
        "dlq_total_rows": dl.get("dlq_total_rows"), "phase3_batches": dl.get("phase3_batches"),
        "cut": dl.get("cut")}
    return out


def union_line(sv: dict[str, Any]) -> str:
    """C7 numbers in one line: |S|, |L| of the run; whole tables vs bronze."""
    cons = sv.get("consistency") or {}
    tables = (f"tables: silver {cons.get('silver_rows_upto_batch')} + late "
              f"{cons.get('late_rows_upto_batch')} vs bronze distinct keyed ids "
              f"{cons.get('bronze_keyed_distinct_ids')}; keyless {cons.get('bronze_keyless_rows')}"
              f" (control excluded {cons.get('control_excluded_total')})"
              if cons.get("checkable") else f"tables: not checkable ({cons.get('reason')})")
    return (f"run: |S| {sv.get('silver_found_ids')}, |L| {sv.get('late_found_ids')}, "
            f"S ∩ L {sv.get('in_both_ids')}; {tables}")


def verify(manifest: Manifest, verbose: bool, fail_on_duplicates: bool = True
           ) -> dict[str, Any]:
    """Run verify_manifest in spark-tools; returns its JSON report, Abort on FAIL."""
    res_code, report = verify_report(manifest, fail_on_duplicates=fail_on_duplicates)
    if verbose or res_code != 0:
        say(json.dumps(report, indent=2))
    if res_code != 0:
        raise Abort(EXIT_VERIFY if res_code == 1 else EXIT_SETUP,
                    f"verify {report.get('result')}: {report.get('notes')}")
    return report


# --- main ----------------------------------------------------------------------

def send_and_wait(run: Run, args: argparse.Namespace, timeout: float, verbose: bool,
                  key: str = "") -> Manifest:
    """Steps 3-6: baseline, send, contiguity/freshness, bronze catch-up, silver catch-up.

    `key` prefixes the summary keys (smoke-dupes sends twice: "send1_", "send2_").
    """
    baseline, _ = snapshot(args.status_path)
    run.summary[f"{key}baseline_batch_id"] = baseline.batch_id if baseline else None
    say(f"[3/7] baseline status: "
        f"{'none' if not baseline else f'batch {baseline.batch_id}, query {baseline.query_id}'}")

    say("[4/7] sending run with the simulator")
    manifest = send_run(run, args, stream=verbose)
    assert manifest.kafka_offsets is not None and manifest.topic is not None
    run.summary.update({f"{key}run_id": manifest.run_id, f"{key}manifest": str(manifest.path),
                        f"{key}events": manifest.events_delivered,
                        f"{key}partitions": {str(p): [r.min, r.max] for p, r in
                                             sorted(manifest.kafka_offsets.items())}})
    say(f"      {manifest.events_delivered} events, offsets "
        + ", ".join(f"p{p} {r.min}..{r.max}" for p, r in sorted(manifest.kafka_offsets.items())))

    say("[5/7] contiguity + freshness checks, then waiting for bronze "
        f"(timeout {timeout:.0f}s)")
    problems = catchup.check_contiguity(manifest.kafka_offsets)
    if problems:
        raise Abort(EXIT_CONTIGUITY, "run offsets are not contiguous; refusing to continue:\n  "
                    + "\n  ".join(problems),
                    short="contiguity violation: " + "; ".join(p.split(" ->")[0] for p in problems))
    conflicts = catchup.pre_run_conflicts(baseline, manifest.kafka_offsets, manifest.topic)
    if conflicts:
        raise Abort(EXIT_SETUP, "status file is inconsistent with the topic:\n  "
                    + "\n  ".join(conflicts), short="status file inconsistent with topic "
                    "(topic recreated?)")
    cu = run.summary[f"{key}catch_up"] = wait_for_catch_up(manifest, baseline, args, timeout,
                                                          verbose)
    say(f"      bronze caught up after {cu['wait_s']}s (batch {cu['batch_id']})")
    say(f"[6/7] waiting for silver (timeout {args.silver_timeout:.0f}s)")
    sc = run.summary[f"{key}silver_catch_up"] = wait_for_silver(args, verbose)
    say(f"      silver caught up after {sc['wait_s']}s (silver batch {sc['batch_id']}, "
        f"past bronze version {sc['bronze_version']})")
    return manifest


def orchestrate(run: Run, args: argparse.Namespace) -> None:
    demo = run.mode == "demo"
    timeout = args.timeout if args.timeout is not None else MODES[run.mode]["timeout"]
    t0 = time.monotonic()
    t0_wall = time.time()

    say(f"[1/7] seed {run.seed} (epoch ms, generated in Python)")
    say("[2/7] checking services" + ("" if args.no_start_spark else " and starting Spark"))
    ensure_services(start_spark=not args.no_start_spark)

    manifest = send_and_wait(run, args, timeout, demo)

    say("[7/7] verifying bronze + silver against the manifest (spark-tools, read-only data)")
    report = verify(manifest, verbose=demo)
    run.summary["verify"] = summarize_verify(report)
    run.summary["silver_evidence"] = silver_evidence(args, t0_wall)
    run.summary["total_s"] = round(time.monotonic() - t0, 1)
    sv = report.get("silver") or {}
    ms = run.summary["silver_evidence"]["merge_scan"]
    say(f"PASS: bronze {report['found_unique_ids']}/{report['expected_unique_ids']} ids, "
        f"{report['duplicate_rows']} duplicates; silver ∪ late {sv.get('found_unique_ids')}/"
        f"{sv.get('expected_unique_ids')} ids, {sv.get('duplicate_rows')} duplicates; "
        f"{run.summary['total_s']}s total")
    say("      " + union_line(sv))
    say("      merge scan: " + (f"read {ms['files_read']} files, {ms['files_in_window']} in "
                                f"window, {ms['live_files']} live" if ms.get("measured")
                                else f"not measured ({ms.get('reason')})"))
    if demo:
        say("\nWhere to look:\n"
            "  Spark UI      http://localhost:4040\n"
            "  bronze Delta  data/lake/bronze/game_events/\n"
            "  silver Delta  data/lake/silver/game_events/\n"
            "  late Delta    data/lake/silver/late_events/\n"
            f"  status file   {args.status_path}\n"
            f"  manifest      {manifest.path}\n"
            "The stack is still running; `make down` stops it.")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    seed = args.seed if args.seed is not None else time.time_ns() // 1_000_000
    run = Run(mode=args.mode, seed=seed, summary={"mode": args.mode, "seed": seed})
    code = EXIT_PASS
    try:
        orchestrate(run, args)
    except Abort as exc:
        code = exc.code
        say(f"\nFAIL (exit {code}): {exc}")
        run.summary["error"] = exc.short
    except CommandError as exc:
        code = EXIT_SETUP
        say(f"\nFAIL (exit {code}): {exc}")
        run.summary["error"] = str(exc)
    except KeyboardInterrupt:
        code = 130
        run.summary["error"] = "interrupted"
    run.summary.update(result="PASS" if code == 0 else "FAIL", exit_code=code)
    line = json.dumps(run.summary, separators=(",", ":"))
    if args.json_summary:
        args.json_summary.parent.mkdir(parents=True, exist_ok=True)
        args.json_summary.write_text(line + "\n", encoding="utf-8")
    say(f"SUMMARY {line}")
    return code


if __name__ == "__main__":
    sys.exit(main())
