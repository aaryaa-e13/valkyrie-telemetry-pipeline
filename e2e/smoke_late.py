"""smoke-late: one REAL realtime run with deliberate lateness, judged by R1-R6 (slice 5, D2.5).

Steps:
    1. fresh seed; services up; the silver baseline (status file) is noted
    2. memory sampler starts (docker stats + Spark REST, threshold 2.26 GiB)
    3. realtime send of simulator/config.late.yaml (~5-7 min), contiguity check,
       bronze then silver catch-up (as e2e.orchestrator)
    4. verify (spark-tools): R0-R3, R7, the W check, isolation, R4, R5, R6a-d
    5. the D7 judge below, on exact counts from late_counts (no fractions)

Exit codes: as e2e.orchestrator (0 PASS, 1 checks failed, 2 setup, 3 catch-up,
4 contiguity), and 2 also when the W check or isolation is "not checkable"
(result "NOT CHECKABLE", the reason printed): smoke-late never passes, and never
says "fail", for what it could not prove.

Asserted (exit 1), only when both gates are checkable (plan S3):
    |S| == delivered - |L|; every late event found once;
    on time: none in L, all in S;   slight: none in L, all in S (R5);
    very: L ⊆ very ids (R4), |S ∩ very| + |L ∩ very| == late_counts.very;
    every run row inserted by a silver batch after the baseline batch.
Reported only: |L| ("x of y very events"), per very event its wm margin, G, the
slight margin, batch times, memory. |L| == 0 with very events is a WARNING.

Usage:
    python -m e2e.smoke_late
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from e2e import catchup, silver_checks
from e2e.docker_ops import CommandError, container_logs
from e2e.orchestrator import (
    EXIT_PASS,
    EXIT_SETUP,
    EXIT_VERIFY,
    SPARK_CONTAINER,
    Abort,
    Run,
    add_common_args,
    ensure_services,
    say,
    send_and_wait,
    summarize_verify,
    union_line,
    verify_report,
)
from streaming.status import read_status

EXIT_NOT_CHECKABLE = 2
# Stop lines (condition M1, set before runs 2-3): container peak > 2.8 GiB (200 MB under
# the 3 GiB container limit), peak growth > 0.1 GiB from the previous run (leak signal),
# heap > 1.6 GiB, GC > 10 %, OOM. Run 1 used 2.26 GiB (slice 4: 2.05 + 10 %) and crossed it.
CONTAINER_PEAK_GIB = 2.8
PEAK_GROWTH_GIB = 0.1
G_WARN_MS = 30_000               # W / 2: very routing loses its margin (S6)
SLIGHT_WARN_MS = 15_000          # 0.25 W: half the expected slight margin (S6)


def judge(report: dict[str, Any], baseline: tuple[str, int] | None
          ) -> tuple[int, list[str], list[str]]:
    """(exit code, problems, warnings) from verify's report. Pure, unit-tested.

    `baseline`: (silver query id, batch id) committed before the send, or None.
    """
    problems: list[str] = []
    warnings: list[str] = []
    sv = report.get("silver") or {}
    o = sv.get("oracle")
    if report.get("result") != "PASS":
        problems.append(f"verify {report.get('result')}: {report.get('notes')}")
    if not o:
        return EXIT_VERIFY, problems + ["verify report has no oracle section"], warnings
    v = o["verdicts"]
    gates = sorted({v[r] for r in ("R4", "R5") if v[r].startswith("not checkable")})
    if problems:
        return EXIT_VERIFY, problems, warnings
    if gates:
        return EXIT_NOT_CHECKABLE, [f"R4/R5 {g}" for g in gates], warnings
    bands, lc = o.get("bands"), o.get("late_counts")
    if not bands or not lc:
        return EXIT_NOT_CHECKABLE, ["manifest has no late_events / late_counts"], warnings

    delivered = sv.get("expected_unique_ids")
    n_s, n_l = sv.get("silver_found_ids"), sv.get("late_found_ids")
    on, sl, ve = bands["ontime"], bands["slight"], bands["very"]

    def need(ok: bool, text: str) -> None:
        if not ok:
            problems.append(text)

    need(n_s == delivered - n_l, f"|S| {n_s} != delivered {delivered} - |L| {n_l}")
    found = sl["silver"] + sl["late"] + ve["silver"] + ve["late"]
    need(found == lc["slight"] + lc["very"],
         f"late events found {found} != late_counts {lc['slight']} + {lc['very']}")
    need(on["late"] == 0, f"{on['late']} on-time events in late")
    need(on["silver"] == delivered - lc["slight"] - lc["very"] == on["expected"],
         f"on-time events in silver {on['silver']} != {delivered} - {lc['slight']} - "
         f"{lc['very']} (manifest on-time {on['expected']})")
    need(sl["late"] == 0, f"{sl['late']} slight events in late")
    need(sl["silver"] == lc["slight"], f"slight in silver {sl['silver']} != {lc['slight']}")
    need(ve["late"] <= lc["very"], f"very in late {ve['late']} > {lc['very']}")
    need(ve["silver"] + ve["late"] == lc["very"],
         f"very in silver {ve['silver']} + late {ve['late']} != {lc['very']}")
    need(n_l == ve["late"], f"|L| {n_l} != very events in late {ve['late']} (R4)")
    for rule in ("R4", "R5"):
        need(v[rule] == "PASS", f"{rule} {v[rule]}")
    batches = o["timing"]["run_batches"]
    if baseline is not None:
        early = [b["batch_id"] for b in batches
                 if b["query_id"] == baseline[0] and b["batch_id"] <= baseline[1]]
        need(not early, f"run rows inserted by batches {early} <= baseline batch "
                        f"{baseline[1]}: some ids existed before the send")

    if lc["very"] > 0 and n_l == 0:
        warnings.append(f"no very event was routed (0 of {lc['very']}): the live run did "
                        "not exercise the late path; the Spark tests carry the proof (D7)")
    g = o["timing"]["g_ms"]
    if g["max"] is not None and g["max"] > G_WARN_MS:
        warnings.append(f"max G {g['max']} ms > {G_WARN_MS} ms: very routing lost its margin")
    m = o["timing"]["slight_margin_ms"]
    if m is not None and m < SLIGHT_WARN_MS:
        warnings.append(f"slight margin {m} ms < {SLIGHT_WARN_MS} ms (R5/R6b still decide)")
    return (EXIT_VERIFY if problems else EXIT_PASS), problems, warnings


def memory_stops(memory: dict[str, Any], prev_peak_gib: float | None) -> list[str]:
    """M1 stop lines crossed in this run (pure, unit-tested)."""
    stops = list(memory.get("over_thresholds") or [])
    peak = memory.get("container_peak_gib")
    if not memory.get("samples"):
        stops.append("container memory not measured")
    elif prev_peak_gib is not None and peak is not None \
            and peak - prev_peak_gib > PEAK_GROWTH_GIB:
        stops.append(f"container peak grew {peak - prev_peak_gib:.3f} GiB from the previous "
                     f"run ({prev_peak_gib} -> {peak}) > {PEAK_GROWTH_GIB}")
    return stops


def spark_started_at() -> str | None:
    """The Spark container's start time (to show no restart between runs)."""
    res = subprocess.run(["docker", "inspect", "--format", "{{.State.StartedAt}}",
                          SPARK_CONTAINER], capture_output=True, text=True)
    return res.stdout.strip() or None


def _sec(us: int | None) -> str:
    return "-inf" if us is None else time.strftime("%H:%M:%S", time.gmtime(us / 1e6))


def tables(o: dict[str, Any]) -> list[str]:
    """Human-readable tables for the log (bands, batches, very events, gates)."""
    out = ["      band     expected  silver  late"]
    for name, b in (o.get("bands") or {}).items():
        out.append(f"      {name:<8} {b['expected']:>8} {b['silver']:>7} {b['late']:>5}")
    out.append("      batch  run_S  run_L  late_rows  late_inserted  inserted  wm (UTC)")
    for b in o["timing"]["run_batches"]:
        out.append(f"      {b['batch_id']:>5} {b['run_silver']:>6} {b['run_late']:>6} "
                   f"{b.get('late_rows')!s:>10} {b.get('late_inserted_this_attempt')!s:>14} "
                   f"{b.get('inserted_this_attempt')!s:>9}  {_sec(b.get('wm_us'))}")
    out.append("      very event      delay_s  batch  event_time-wm_s  routed")
    for e in o["timing"]["very_events"]:
        d = e["event_time_minus_wm_ms"]
        out.append(f"      {e['event_id'][:14]:<14} {e['delay_ms'] / 1000:>8.1f} "
                   f"{e['batch_id']:>6} {'-inf' if d is None else f'{d / 1000:.1f}':>16}  "
                   f"{'yes' if e['routed'] else 'no'}")
    w, iso, pre = o["w_check"], o["isolation"], o["timing"]["control_before_run"] or {}
    out.append(f"      W check: {'OK, W = ' + str(w.get('w_s')) if w['checkable'] else w['reason']}"
               f"; isolation: {'OK' if iso['checkable'] else iso['reason']}, margin "
               f"{iso.get('margin_ms')} ms; control max before the run -> min_t gap "
               f"{pre.get('gap_to_min_t_ms')} ms")
    return out


class MemorySampler:
    """e2e.memory_sampler in a child process; stopped by a file (no Kafka, no writes)."""

    def __init__(self, label: str) -> None:
        self._stop = Path(tempfile.mkdtemp(prefix="smoke_late_")) / "stop"
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "e2e.memory_sampler", "--until-file", str(self._stop),
             "--seconds", "1500", "--label", label,
             "--container-peak-gib", str(CONTAINER_PEAK_GIB)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def stop(self) -> dict[str, Any]:
        self._stop.touch()
        try:
            out, _ = self._proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            return {"measured": False, "reason": "memory sampler did not stop"}
        for line in out.splitlines():
            if line.startswith("MEMORY "):
                return json.loads(line[len("MEMORY "):])
        return {"measured": False, "reason": out[-500:]}


def run_late(run: Run, args: argparse.Namespace) -> int:
    t0, t0_wall = time.monotonic(), time.time()
    say(f"[1/8] seed {run.seed} (fresh), config simulator/config.late.yaml (realtime)")
    say("[2/8] checking services and starting Spark")
    started_before = spark_started_at()
    ensure_services(start_spark=not args.no_start_spark)
    run.summary["spark_started_at"] = started = spark_started_at()
    run.summary["spark_restarted_by_this_run"] = started != started_before
    say(f"      spark container started at {started}"
        f"{' (restarted by this run)' if started != started_before else ' (not restarted)'}")
    snap = catchup.delta_snapshot_from_status(read_status(args.silver_status_path))
    baseline = (snap.query_id, snap.batch_id) if snap else None
    run.summary["silver_baseline"] = list(baseline) if baseline else None
    say(f"      silver baseline: {baseline or 'none'}; memory sampler started "
        f"(limit {CONTAINER_PEAK_GIB} GiB)")
    sampler = MemorySampler(f"smoke-late {run.seed}")
    try:
        manifest = send_and_wait(run, args, args.timeout or 180.0, verbose=True)
        say("[7/8] verifying R0-R7 + gates against the manifest (spark-tools, read-only)")
        code, report = verify_report(manifest, fail_on_duplicates=True)
    finally:
        memory = sampler.stop()
    run.summary["memory"] = memory
    jvm = memory.get("jvm") or {}
    gib = 1024 ** 3
    say("      MEMORY container peak "
        f"{memory.get('container_peak_gib')} GiB (end {memory.get('container_end_gib')}, "
        f"previous run {args.prev_container_peak_gib}); JVM since its start (REST /executors "
        "peakMemoryMetrics): " + (
            f"heap {(jvm.get('heap_peak_bytes') or 0) / gib:.3f} GiB, off-heap "
            f"{(jvm.get('offheap_peak_bytes') or 0) / gib:.3f} GiB, GC {memory.get('gc_share')}"
            if jvm.get("measured") else f"not available ({jvm.get('reason')})"))
    run.summary["memory_stops"] = stops = memory_stops(memory, args.prev_container_peak_gib)
    logs = container_logs(SPARK_CONTAINER, since=t0_wall)
    run.summary["silver_evidence"] = {
        "batch_timing": silver_checks.batch_timing(logs, args.trigger_interval_s),
        "late": silver_checks.late_summary(silver_checks.silver_batch_records(logs)),
        "delta_checkpoints": {
            name: silver_checks.delta_checkpoints_since(path, t0_wall)
            for name, path in (("silver", args.silver_path), ("late", args.late_path),
                               ("control", Path("data/lake/_control/silver_batches")))}}
    run.summary["verify"] = summarize_verify(report)
    sv = report.get("silver") or {}
    o = sv.get("oracle") or {}
    run.summary["oracle"] = {k: o.get(k) for k in ("verdicts", "late_counts", "bands")}
    run.summary["oracle"]["gates"] = {
        "w_check": o.get("w_check"), "isolation": o.get("isolation")}
    run.summary["oracle"]["timing"] = {k: (o.get("timing") or {}).get(k) for k in (
        "g_ms", "slight_margin_ms", "slight_with_wm_null", "control_before_run",
        "very_events", "run_batches")}
    say("[8/8] judging (exact counts from late_counts)")
    if o:
        for line in tables(o):
            say(line)
    jcode, problems, warnings = judge(report, baseline)
    if code not in (0, 1) and jcode == EXIT_PASS:
        jcode, problems = EXIT_VERIFY, [f"verify exited {code}"]
    for w in warnings:
        say(f"WARNING: {w}")
    for x in stops:
        say(f"STOP (memory, M1): {x}")
    run.summary["warnings"] = warnings
    run.summary["total_s"] = round(time.monotonic() - t0, 1)
    if jcode != EXIT_PASS:
        run.summary["not_checkable"] = jcode == EXIT_NOT_CHECKABLE
        say(json.dumps(report, indent=2))
        raise Abort(jcode, "smoke-late " + ("NOT CHECKABLE" if jcode == EXIT_NOT_CHECKABLE
                                            else "failed") + ":\n  " + "\n  ".join(problems),
                    short="; ".join(problems))
    lc = o["late_counts"]
    say(f"PASS: {sv['expected_unique_ids']} ids, |S| {sv['silver_found_ids']}, |L| "
        f"{sv['late_found_ids']} = {sv['late_found_ids']} of {lc['very']} very events routed; "
        f"slight {lc['slight']} all in silver; {run.summary['total_s']}s total")
    say("      " + union_line(sv))
    return EXIT_PASS


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="VALKYRIE smoke-late (realtime, deliberate lateness)")
    add_common_args(p)
    p.add_argument("--prev-container-peak-gib", type=float,
                   help="previous smoke-late's container peak (M1 growth stop line)")
    args = p.parse_args(argv)
    seed = args.seed if args.seed is not None else time.time_ns() // 1_000_000
    run = Run(mode="late", seed=seed, summary={"mode": "smoke-late", "seed": seed})
    code = EXIT_PASS
    try:
        code = run_late(run, args)
    except Abort as exc:
        code = exc.code
        say(f"\n{'NOT CHECKABLE' if run.summary.get('not_checkable') else 'FAIL'} "
            f"(exit {code}): {exc}")
        run.summary["error"] = exc.short
    except CommandError as exc:
        code = EXIT_SETUP
        say(f"\nFAIL (exit {code}): {exc}")
        run.summary["error"] = str(exc)
    except KeyboardInterrupt:
        code = 130
        run.summary["error"] = "interrupted"
    run.summary.update(result="PASS" if code == 0 else
                       "NOT CHECKABLE" if run.summary.get("not_checkable") else "FAIL",
                       exit_code=code)
    line = json.dumps(run.summary, separators=(",", ":"))
    if args.json_summary:
        args.json_summary.parent.mkdir(parents=True, exist_ok=True)
        args.json_summary.write_text(line + "\n", encoding="utf-8")
    say(f"SUMMARY {line}")
    return code


if __name__ == "__main__":
    sys.exit(main())
