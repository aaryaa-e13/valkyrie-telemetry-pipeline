"""smoke-faults: SIGKILL the job at each silver write boundary, restart, judge (slice 6, D2.6).

Silver points (after_read, after_late, after_silver, after_control), each:
    1. run A (tiny backfill, ends now) -> bronze + silver catch-up
    2. graceful stop (exit 0 asserted); send, while Spark is down:
         B  new seed, --start-time now - 30 min   -> all late   (by >= ~26 min)
         C  new seed, ends now, sent >= 80 s after A -> on time (by >= 15 s)
         A' A's seed again (re-stamped duplicates of silver rows)
    3. start Spark WITH docker-compose.faults.yml; the hook freezes the first silver
       batch with late + on-time rows at the point and writes a marker; we then
       `docker kill --signal KILL` the container (no finally, no shutdown hooks)
    4. F4, while the job is down: marker, FAULT REACHED line, exit 137 without OOM,
       no fallback, table + checkpoint state as the point claims -> else exit 6
    5. restart WITHOUT the fault file (env checked: no VALKYRIE_FAULT*), catch-up
    6. F3: verify A, B, C; the guard's exact (T, C, P); batch N re-run; its control
       row (which attempt wrote it); counts and orphans before/after (K2, K3)
inside_bronze (best effort, K1): stop, send D, start, SIGKILL as soon as bronze plans
its batch N, classify (i) planned not in Delta / (ii) in Delta not committed /
(iii) too late; up to 3 attempts; then restart, catch-up, verify D.

Exit codes: 0 pass, 1 a check failed, 2 setup, 3 catch-up, 4 contiguity,
6 the fault did not fire where claimed (or inside_bronze never hit (i)/(ii)).
Spark is always left running WITHOUT the fault env (finally).

Usage:
    python -m e2e.smoke_faults                       # all points, stops at the first failure
    python -m e2e.smoke_faults --point after_late
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from common.delta_log import appended_between, current_version, latest_txn_in_commits
from common.manifest_reader import Manifest, ManifestError, load_manifest
from e2e import catchup
from e2e import faults_check as fc
from e2e.docker_ops import (
    CommandError,
    compose_stop_spark,
    compose_up_spark,
    container_logs,
    container_state,
    run_simulator,
)
from e2e.orchestrator import (
    EXIT_CONTIGUITY,
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
    snapshot,
    verify_report,
    wait_for_silver,
)
from e2e.smoke_late import MemorySampler, memory_stops
from streaming.start_guard import read_checkpoint_state

EXIT_NOT_FIRED = 6
FAULTS_FILE = "docker-compose.faults.yml"
TINY = "simulator/config.tiny.yaml"
POINTS = (*fc.SILVER_POINTS, "inside_bronze")
PATHS = {"bronze": Path("data/lake/bronze/game_events"),
         "silver": Path("data/lake/silver/game_events"),
         "late": Path("data/lake/silver/late_events"),
         "control": Path("data/lake/_control/silver_batches")}
SILVER_CP, BRONZE_CP = Path("data/checkpoints/silver"), Path("data/checkpoints/bronze")
FAULT_ROOT = Path("data/faults")
W_S = 60
MAX_TIMELINE_S = 125          # tiny config: 2 matches x <= 120 s + 5 s (dry runs: <= 123.8 s)
C_MARGIN_S = 15
B_AGO = timedelta(minutes=30)
MARKER_TIMEOUT_S = 240.0
BRONZE_ATTEMPTS = 3


# --- docker helpers ------------------------------------------------------------------

def preflight() -> None:
    env_file = Path(".env")
    if os.environ.get("COMPOSE_FILE") or (
            env_file.exists() and "COMPOSE_FILE" in env_file.read_text(encoding="utf-8")):
        raise Abort(EXIT_SETUP, "COMPOSE_FILE is set (shell or .env): refusing, it could load "
                                "the fault file into normal runs")


def fault_env_in_container() -> list[str]:
    res = subprocess.run(["docker", "inspect", "--format", "{{json .Config.Env}}",
                          SPARK_CONTAINER], capture_output=True, text=True, timeout=20)
    try:
        env = json.loads(res.stdout) or []
    except ValueError:
        return []
    return [e for e in env if e.startswith("VALKYRIE_FAULT")]


def up_with_faults(point: str, tag: str) -> None:
    env = {**os.environ, "VALKYRIE_FAULT_POINT": point, "VALKYRIE_FAULT_TAG": tag}
    res = subprocess.run(["docker", "compose", "-f", "docker-compose.yml", "-f", FAULTS_FILE,
                          "--profile", "spark", "up", "-d", "spark"], env=env,
                         capture_output=True, text=True, timeout=300)
    if res.returncode != 0:
        raise Abort(EXIT_SETUP, f"could not start Spark with the fault file:\n{res.stderr}")


def up_normal() -> list[str]:
    """Start Spark from the default compose file; returns its fault env (must be [])."""
    res = compose_up_spark()
    if res.returncode != 0:
        raise Abort(EXIT_SETUP, f"could not start Spark:\n{res.stderr.strip()}")
    return fault_env_in_container()


def kill_spark() -> float:
    t = time.time()
    subprocess.run(["docker", "kill", "--signal", "KILL", SPARK_CONTAINER],
                   capture_output=True, text=True, timeout=30)
    return t


def stop_gracefully() -> None:
    compose_stop_spark()
    st = container_state(SPARK_CONTAINER)
    if st.running or st.exit_code != 0:
        raise Abort(EXIT_SETUP, f"graceful stop failed: {st.describe(datetime.now(UTC))}")


def wait_for_bronze_rows(v_before: int | None, n: int, timeout: float = 180.0) -> None:
    """Until bronze's Delta log has added n rows since v_before (no status file)."""
    deadline = time.monotonic() + timeout
    while True:
        v = current_version(PATHS["bronze"])
        if v is not None and v_before is not None and v > v_before and \
                appended_between(PATHS["bronze"], v_before, v).rows >= n:
            return
        if time.monotonic() > deadline or not container_state(SPARK_CONTAINER).running:
            raise Abort(3, f"bronze did not add {n} rows after the restart (version {v})")
        time.sleep(1.0)


def save_evidence(args: argparse.Namespace, name: str, text: str) -> None:
    """Write evidence next to --json-summary at once (a later crash cannot lose it)."""
    if args.json_summary:
        args.json_summary.parent.mkdir(parents=True, exist_ok=True)
        (args.json_summary.parent / name).write_text(text + "\n", encoding="utf-8")


def wait_stopped(timeout: float = 60.0) -> catchup.ContainerState:
    deadline = time.monotonic() + timeout
    while True:
        st = container_state(SPARK_CONTAINER)
        if not st.running or time.monotonic() > deadline:
            return st
        time.sleep(0.2)


# --- sending while Spark is down ----------------------------------------------------------

def send(seed: int, extra: list[str], label: str, manifest_dir: Path) -> Manifest:
    before = set(manifest_dir.glob(f"run_*_seed{seed}.json"))
    res = run_simulator(TINY, seed, extra, stream=False)
    if res.returncode != 0:
        tail = "\n".join((res.stdout + res.stderr).splitlines()[-15:])
        raise Abort(EXIT_SETUP, f"simulator ({label}) exited {res.returncode}:\n{tail}")
    found = sorted(set(manifest_dir.glob(f"run_*_seed{seed}.json")) - before)
    if len(found) != 1:
        raise Abort(EXIT_SETUP, f"{label}: expected one new manifest, found {found}")
    try:
        m = load_manifest(found[0])
    except (ManifestError, OSError) as exc:
        raise Abort(EXIT_SETUP, f"{label}: unusable manifest: {exc}") from exc
    assert m.kafka_offsets is not None
    problems = catchup.check_contiguity(m.kafka_offsets)
    if problems:
        raise Abort(EXIT_CONTIGUITY, f"{label}: offsets not contiguous: {problems}")
    say(f"      {label}: {m.events_delivered} events, seed {seed}, {m.path.name}")
    return m


def ids(m: Manifest) -> set[str]:
    return set(m.event_ids)


# --- one silver point -------------------------------------------------------------------------

def control_rows() -> list[dict[str, Any]]:
    return fc.live_rows(PATHS["control"], [
        "silver_query_id", "batch_id", "late_rows", "ontime_rows", "in_initial_snapshot",
        "late_dup_of_silver", "dup_of_late", "late_inserted_this_attempt",
        "inserted_this_attempt", "watermark_used", "watermark_delay_s",
        "batch_max_clamped_event_time"])


def expected_wm(ctl: list[dict[str, Any]], q: str, n: int) -> Any:
    maxima = [r["batch_max_clamped_event_time"] for r in ctl
              if r["silver_query_id"] == q and r["batch_id"] < n
              and r["batch_max_clamped_event_time"] is not None]
    return max(maxima) - timedelta(seconds=W_S) if maxima else None


def run_silver_point(point: str, args: argparse.Namespace, out: dict[str, Any]) -> None:
    t0 = time.monotonic()
    base = int(time.time() * 1000)
    seeds = {"A": base, "B": base + 1, "C": base + 2}
    tag = f"{point}-{base}"
    say(f"\n===== {point} (tag {tag}) =====")
    ensure_services(start_spark=True)
    if fault_env_in_container():
        raise Abort(EXIT_SETUP, "the running Spark container has VALKYRIE_FAULT* set")
    before = fc.lake_state(PATHS)
    out["lake_before"] = before

    say("[1] run A (backfill, ends now), wait for bronze + silver")
    run_a = Run(mode="smoke", seed=seeds["A"], summary={})
    a = send_and_wait(run_a, args, 90.0, False, key="A_")
    t_a_end = time.time()
    say("[2] graceful stop; send B (30 min ago), C (on time), A' (A again) while down")
    stop_gracefully()
    baseline, _ = snapshot(args.status_path)
    start_b = (datetime.now(UTC) - B_AGO).replace(microsecond=0)
    b = send(seeds["B"], ["--start-time", start_b.isoformat().replace("+00:00", "Z")], "B",
             args.manifest_dir)
    wait_s = t_a_end + MAX_TIMELINE_S - W_S + C_MARGIN_S - time.time()
    if wait_s > 0:
        say(f"      waiting {wait_s:.0f} s so that C is on time by >= {C_MARGIN_S} s")
        time.sleep(wait_s)
    c = send(seeds["C"], [], "C", args.manifest_dir)
    a2 = send(seeds["A"], [], "A'", args.manifest_dir)
    out["runs"] = {k: {"manifest": str(m.path), "events": m.events_delivered}
                   for k, m in (("A", a), ("B", b), ("C", c), ("A2", a2))}
    if ids(a2) != ids(a):
        raise Abort(EXIT_SETUP, "A' has different event ids than A")

    say(f"[3] start Spark WITH {FAULTS_FILE} (point {point}); wait for the marker")
    v_bronze_before = current_version(PATHS["bronze"])
    sampler = MemorySampler(f"smoke-faults {point}")
    marker_path = FAULT_ROOT / tag / f"{point}.reached"
    try:
        up_with_faults(point, tag)
        deadline = time.monotonic() + MARKER_TIMEOUT_S
        while not marker_path.exists():
            st = container_state(SPARK_CONTAINER)
            if not st.running or time.monotonic() > deadline:
                logs = container_logs(SPARK_CONTAINER, tail=40)
                raise Abort(EXIT_NOT_FIRED, f"{point}: no marker after "
                            f"{time.monotonic() - deadline + MARKER_TIMEOUT_S:.0f} s; container "
                            f"{st.describe(datetime.now(UTC))}\n" + "\n".join(logs))
            time.sleep(0.05)
        t_kill = kill_spark()
        st = wait_stopped()
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        killed_logs = container_logs(SPARK_CONTAINER)          # before the container is replaced
        save_evidence(args, f"{tag}.killed-container.log", "\n".join(killed_logs))
        save_evidence(args, f"{tag}.state.json", json.dumps(
            {"exit_code": st.exit_code, "oom_killed": st.oom_killed, "marker": marker,
             "lake": fc.lake_state(PATHS), "files": fc.checkpoint_batch(
                 SILVER_CP, int(marker["batch_id"]))}, default=str))
        n = int(marker["batch_id"])
        q = read_checkpoint_state(SILVER_CP).query_id or ""
        say(f"      marker: batch {n}, late {marker['late_rows']}, on time "
            f"{marker['ontime_rows']}; SIGKILL sent; container {st.describe(datetime.now(UTC))}")

        say("[4] F4 while the job is down: did it fire where claimed?")
        down = fc.lake_state(PATHS)
        files = fc.checkpoint_batch(SILVER_CP, n)
        late = fc.live_rows(PATHS["late"], ["event_id", "silver_query_id", "silver_batch_id"])
        silver = fc.live_rows(PATHS["silver"], ["event_id", "silver_query_id",
                                                "silver_batch_id"])
        not_fired = fc.fired_problems(point, marker, killed_logs, st.exit_code, st.oom_killed)
        not_fired += fc.down_state_problems(point, n, q, ids(b), ids(c), late, silver,
                                            control_rows(), files)
        out.update(batch=n, query_id=q, marker=marker, kill_at=t_kill,
                   killed_container={"exit_code": st.exit_code, "oom_killed": st.oom_killed},
                   lake_down=down, delta_down=fc.deltas(before, down), silver_batch_files=files)
        say(f"      exit {st.exit_code}, OOMKilled {st.oom_killed}; offsets/{n} "
            f"{files['offsets']}, commits/{n} {files['commits']}")
        say(f"      down vs before (K2/K3): {fc.deltas(before, down)}")
        if not_fired:
            raise Abort(EXIT_NOT_FIRED, f"{point} did not fire where claimed:\n  "
                        + "\n  ".join(not_fired), short="; ".join(not_fired))

        say("[5] restart WITHOUT the fault file; catch-up")
        t_restart = time.time()
        env_left = up_normal()
        out["fault_env_after_restart"] = env_left
        if env_left:
            raise Abort(EXIT_VERIFY, f"restarted container still has {env_left}")
        # Not the bronze status file: its listener may not have run before the SIGKILL.
        # First bronze's Delta log must hold all of B, C, A'; only then is bronze's
        # current version the one silver must pass (wait_for_silver reads it once).
        wait_for_bronze_rows(v_bronze_before, len(ids(b)) + len(ids(c)) + len(ids(a2)))
        wait_for_silver(args, False)
    finally:
        memory = sampler.stop()
    out["memory"] = memory
    out["memory_stops"] = memory_stops(memory, None)

    say("[6] F3: verify A, B, C; guard; replayed batch; control row")
    new_logs = container_logs(SPARK_CONTAINER, since=t_restart)
    problems: list[str] = []
    if fc.log_records(new_logs, "FAULT INJECTION ACTIVE"):
        problems.append("the restarted process logged FAULT INJECTION ACTIVE")
    guards = fc.log_records(new_logs, "start-up state")
    sg = next((g for g in guards if g.get("logger") == "streaming.silver"), None)
    bg = next((g for g in guards if g.get("logger") == "streaming.bronze"), None)
    out["guards"] = {"silver": sg, "bronze": bg}
    problems += fc.guard_problems(point, n, sg, bg)
    rerun = [r for r in fc.log_records(new_logs, "silver batch") if r.get("batch_id") == n]
    if len(rerun) != 1:
        problems.append(f"{len(rerun)} 'silver batch' lines for batch {n} after the restart "
                        "(want 1: Spark re-runs it)")
    ctl = control_rows()
    mine = sorted(r["batch_id"] for r in ctl if r["silver_query_id"] == q)
    if mine != list(range(mine[0], mine[-1] + 1)) or len(mine) != len(set(mine)):
        problems.append(f"control rows of {q} are not exactly one per batch")
    row_n = [r for r in ctl if r["silver_query_id"] == q and r["batch_id"] == n]
    if len(row_n) != 1:
        problems.append(f"{len(row_n)} control rows for batch {n} (want exactly 1)")
    problems += fc.control_row_problems(point, row_n[0] if row_n else None, len(ids(b)),
                                        len(ids(c)), len(ids(a)), expected_wm(ctl, q, n))
    out["control_row_n"] = {k: (str(v) if isinstance(v, datetime) else v)
                            for k, v in (row_n[0] if row_n else {}).items()}
    verdicts = {}
    for name, m, expect, dup_ok in (("A", a, "silver", True), ("B", b, "late", False),
                                    ("C", c, "silver", False)):
        _, rep = verify_report(m, fail_on_duplicates=not dup_ok)
        problems += fc.run_problems(name, rep, expect, len(ids(m)))
        verdicts[name] = ((rep.get("silver") or {}).get("oracle") or {}).get("verdicts")
        if name == "A" and (rep.get("max_copies_of_one_id"), rep.get("missing_ids")) != (2, 0):
            problems.append(f"A: bronze should hold every A id twice: {rep.get('missing_ids')} "
                            f"missing, max copies {rep.get('max_copies_of_one_id')}")
        if rep.get("duplicate_kafka_offsets") or rep.get("null_kafka_offset_rows"):
            problems.append(f"{name}: R0 failed in bronze")
    out["verdicts"] = verdicts
    after = fc.lake_state(PATHS)
    out["lake_after"] = after
    out["delta"] = fc.deltas(before, after)
    out["expected_rows_delta"] = {"bronze": 2 * len(ids(a)) + len(ids(b)) + len(ids(c)),
                                  "silver": len(ids(a)) + len(ids(c)), "late": len(ids(b))}
    for t, d in out["expected_rows_delta"].items():
        if out["delta"][t]["rows"] != d:
            problems.append(f"{t} rows grew {out['delta'][t]['rows']}, expected {d}")
    out["seconds"] = round(time.monotonic() - t0, 1)
    say(f"      before -> after (K2/K3): {out['delta']}; control rows "
        f"+{out['delta']['control']['rows']}")
    for x in out["memory_stops"]:
        say(f"STOP (memory): {x}")
    if problems:
        raise Abort(EXIT_VERIFY, f"{point} failed:\n  " + "\n  ".join(problems),
                    short="; ".join(problems))
    say(f"PASS {point}: batch {n}; B {len(ids(b))} -> late, C {len(ids(c))} -> silver, A' +0; "
        f"one control row per batch; {out['seconds']} s")


# --- inside_bronze (best effort) ----------------------------------------------------------------

def run_bronze_point(args: argparse.Namespace, out: dict[str, Any]) -> None:
    t0 = time.monotonic()
    say("\n===== inside_bronze (best effort, K1) =====")
    attempts: list[dict[str, Any]] = []
    out["attempts"] = attempts
    for k in range(1, BRONZE_ATTEMPTS + 1):
        ensure_services(start_spark=True)
        before = fc.lake_state(PATHS)
        stop_gracefully()
        baseline, _ = snapshot(args.status_path)
        p0 = fc.newest_batch(BRONZE_CP / "offsets")
        v_before = current_version(PATHS["bronze"])
        d = send(int(time.time() * 1000), [], f"D{k}", args.manifest_dir)
        sampler = MemorySampler(f"smoke-faults inside_bronze {k}")
        try:
            res = compose_up_spark()
            if res.returncode != 0:
                raise Abort(EXIT_SETUP, f"could not start Spark:\n{res.stderr}")
            deadline = time.monotonic() + MARKER_TIMEOUT_S
            while (fc.newest_batch(BRONZE_CP / "offsets") or -1) <= (p0 or -1):
                if time.monotonic() > deadline or not container_state(SPARK_CONTAINER).running:
                    raise Abort(EXIT_SETUP, "bronze never planned a batch after the start")
                time.sleep(0.01)
            kill_spark()
            st = wait_stopped()
            killed_logs = container_logs(SPARK_CONTAINER)
            n = fc.newest_batch(BRONZE_CP / "offsets")
            assert n is not None
            files = fc.checkpoint_batch(BRONZE_CP, n)
            qid = read_checkpoint_state(BRONZE_CP).query_id or ""
            txn, _ = latest_txn_in_commits(PATHS["bronze"], qid)
            state = fc.classify_bronze(n, files, txn)
            down = fc.lake_state(PATHS)
            att = {"attempt": k, "manifest": str(d.path), "events": d.events_delivered,
                   "batch": n, "files": files, "bronze_txn": txn, "state": state,
                   "exit_code": st.exit_code, "oom_killed": st.oom_killed,
                   "delta_down": fc.deltas(before, down),
                   "fallback": bool(fc.log_records(killed_logs, "FAULT FALLBACK"))}
            attempts.append(att)
            say(f"      attempt {k}: batch {n}, offsets {files['offsets']}, commits "
                f"{files['commits']}, txn {txn} -> state ({state}); exit {st.exit_code}")
            t_restart = time.time()
            env_left = up_normal()
            if env_left:
                raise Abort(EXIT_VERIFY, f"restarted container has {env_left}")
            wait_for_bronze_rows(v_before, d.events_delivered)
            wait_for_silver(args, False)
        finally:
            att_mem = sampler.stop()
        problems: list[str] = []
        att["memory"] = att_mem
        att["memory_stops"] = memory_stops(att_mem, None)
        if st.exit_code != 137 or st.oom_killed:
            problems.append(f"killed container exit {st.exit_code}, OOMKilled {st.oom_killed}")
        bg = next((g for g in fc.log_records(container_logs(SPARK_CONTAINER, since=t_restart),
                                              "start-up state")
                   if g.get("logger") == "streaming.bronze"), None)
        att["bronze_guard"] = bg
        want = {"i": (n - 1, n - 1), "ii": (n, n - 1), "iii": (n, n)}.get(state)
        got = (bg or {}).get("table_txn"), (bg or {}).get("checkpoint_committed")
        if (bg or {}).get("mode") != "resume" or (want is not None and got != want):
            problems.append(f"bronze guard {got} mode {(bg or {}).get('mode')}, expected "
                            f"resume with (T, C) = {want}")
        _, rep = verify_report(d, fail_on_duplicates=True)
        if rep.get("result") != "PASS" or rep.get("missing_ids") or rep.get("duplicate_rows"):
            problems.append(f"D{k}: verify {rep.get('result')}: {rep.get('notes')}")
        v_after = current_version(PATHS["bronze"])
        assert v_before is not None and v_after is not None
        added = appended_between(PATHS["bronze"], v_before, v_after).rows
        att["bronze_rows_added_log"] = added
        if added != d.events_delivered:
            problems.append(f"bronze Delta log added {added} rows, the run has "
                            f"{d.events_delivered}")
        att["delta_after"] = fc.deltas(before, fc.lake_state(PATHS))
        if problems:
            raise Abort(EXIT_VERIFY, "inside_bronze failed:\n  " + "\n  ".join(problems),
                        short="; ".join(problems))
        if state in ("i", "ii"):
            out["hit"] = state
            out["seconds"] = round(time.monotonic() - t0, 1)
            say(f"PASS inside_bronze: state ({state}) on attempt {k}; replay correct; "
                f"{out['seconds']} s")
            return
    out["seconds"] = round(time.monotonic() - t0, 1)
    raise Abort(EXIT_NOT_FIRED, f"inside_bronze: neither (i) nor (ii) hit in {BRONZE_ATTEMPTS} "
                "attempts (every kill landed after commits/N); each replay was still checked",
                short="inside_bronze not hit (K1: report and ask)")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="VALKYRIE smoke-faults (SIGKILL at write boundaries)")
    add_common_args(p)
    p.add_argument("--point", choices=[*POINTS, "all"], default="all")
    p.set_defaults(silver_timeout=180.0)      # includes a JVM restart
    args = p.parse_args(argv)
    points = list(POINTS) if args.point == "all" else [args.point]
    code = EXIT_PASS
    summary: dict[str, Any] = {"mode": "smoke-faults", "points": {}}
    try:
        preflight()
        ensure_services(start_spark=True)
        summary["lake_start"] = fc.lake_state(PATHS)
        say(f"lake before the first fault run (K2/K3): {json.dumps(summary['lake_start'])}")
        for point in points:
            out: dict[str, Any] = {}
            summary["points"][point] = out
            if point == "inside_bronze":
                run_bronze_point(args, out)
            else:
                run_silver_point(point, args, out)
            out["result"] = "PASS"
            say(f"POINT {json.dumps({point: out}, default=str, separators=(',', ':'))}")
            if out.get("memory_stops") or any(a.get("memory_stops")
                                              for a in out.get("attempts", [])):
                raise Abort(EXIT_VERIFY, f"memory stop line crossed at {point}")
    except Abort as exc:
        code = exc.code
        say(f"\nFAIL (exit {code}): {exc}")
        summary["error"] = exc.short
    except CommandError as exc:
        code = EXIT_SETUP
        say(f"\nFAIL (exit {code}): {exc}")
        summary["error"] = str(exc)
    except KeyboardInterrupt:
        code = 130
        summary["error"] = "interrupted"
    except Exception as exc:  # noqa: BLE001 - a bug in this script: report, restart Spark
        import traceback

        code = EXIT_VERIFY
        say(f"\nFAIL (exit {code}): smoke-faults crashed:\n{traceback.format_exc()}")
        summary["error"] = f"crash: {exc!r}"
    finally:
        if not container_state(SPARK_CONTAINER).running or fault_env_in_container():
            try:
                left = up_normal()
                summary["spark_left_running"] = not left
            except (Abort, CommandError) as exc:
                summary["spark_left_running"] = False
                say(f"could not restart Spark ({exc}). Start it with: make spark-up")
        else:
            summary["spark_left_running"] = True
    summary.update(result="PASS" if code == 0 else "FAIL", exit_code=code)
    line = json.dumps(summary, default=str, separators=(",", ":"))
    if args.json_summary:
        args.json_summary.parent.mkdir(parents=True, exist_ok=True)
        args.json_summary.write_text(line + "\n", encoding="utf-8")
    say(f"SUMMARY {line}")
    return code


if __name__ == "__main__":
    sys.exit(main())
