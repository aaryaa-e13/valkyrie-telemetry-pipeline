"""Per-step time of one silver micro-batch, from the Spark REST API and the job's logs.

Read only: it GETs the live job's monitoring API and reads `docker logs`. It never
writes to the lake, the checkpoints or Kafka.

Method (docs/plans/phase2-slice4-timing.md, rebuilt for the slice 4 T1 follow-up):

1. Take the SQL executions whose description names the silver query and `batch = N`.
   The earliest one is the foreachBatch write itself ("outer"); its duration should
   equal the logged `add_batch_ms` (cross-check 1).
2. An execution that runs inside another non-outer execution's time window is a
   child (Delta's own snapshot and scan executions inside a MERGE or a save). Its
   time and jobs count toward that parent.
3. Each remaining execution goes into one step, in `process_batch`'s code order,
   by the table paths its plan reads (see classify()). An execution that only reads
   a table's `_delta_log` (a snapshot load, planning for the next action) goes to the
   step of the next classified execution.
4. A step runs from the end of the previous step (or the outer start) to the end of
   its own last execution, so driver time before a step's first Spark job counts
   toward that step. driver = wall - Spark execution time.

Cross-checks printed with every batch: outer duration vs `add_batch_ms`; silver MERGE
from the end of its source view's CreateViewCommand to the end of `.first()` vs the
logged `merge_ms` (the job's timer covers exactly that); wm read vs `wm_read_ms`.

Executions are matched by description: the streaming batch's own ("silver ... batch =
N") and Delta's nested ones ("Delta: silver ... batch = N: ...", "... batch = N - MERGE
operation").

Usage (on the host, next to the running job):
    python3 bench/silver_steps.py 19 20 21 --out docs/plans/t1-probe/before.json
    python3 bench/silver_steps.py --list          # silver batches the UI still has
Env: SPARK_UI_URL (default http://localhost:4040), SPARK_CONTAINER (valkyrie-spark),
SILVER_QUERY_NAME (silver). Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from statistics import median
from typing import Any

STEPS = ("wm read", "plan counts", "late write", "late anti-join + count", "silver MERGE",
         "control write")

# Table paths inside the job container (docker-compose.yml, service spark).
PATHS = {
    "bronze": "/data/lake/bronze/game_events",
    "silver": "/data/lake/silver/game_events",
    "late": "/data/lake/silver/late_events",
    "control": "/data/lake/_control/silver_batches",
}
LOCATION = re.compile(r"(?:file:)?(/data/lake/[^\s,\]\)'\"]+)")


class ProbeError(RuntimeError):
    """The API or the logs did not give what the method needs."""


# --- pure part (unit-tested) ------------------------------------------------------------


def parse_time(s: str) -> float:
    """Spark REST time '2026-11-19T10:00:00.123GMT' -> epoch seconds."""
    base = s.removesuffix("GMT")
    return datetime.strptime(base, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=UTC).timestamp()


def tables_read(plan: str) -> tuple[set[str], set[str]]:
    """(tables whose data the plan reads, tables whose _delta_log only it reads)."""
    data, log_only = set(), set()
    for path in LOCATION.findall(plan):
        for name, root in PATHS.items():
            if path == root or path.startswith(root + "/"):
                (log_only if "/_delta_log" in path else data).add(name)
    return data, log_only - data


def is_merge(execution: dict[str, Any]) -> bool:
    text = str(execution.get("planDescription", "")) + str(execution.get("description", ""))
    nodes = " ".join(str(n.get("nodeName", "")) for n in execution.get("nodes", []))
    return "MergeIntoCommand" in text or "MergeIntoCommand" in nodes or "MERGE INTO" in text


def is_snapshot(plan: str) -> bool:
    """A Delta log replay: reads a `_delta_log`, or scans the log's add/remove actions."""
    return "_delta_log" in plan or ("partitionValues" in plan and "deletionVector" in plan)


def classify(execution: dict[str, Any], current: str | None) -> str | None:
    """Step of one top-level execution, given the step of the previous one.

    None = planning for the next action (a snapshot load, or the CreateViewCommand of
    a MERGE source): the caller gives it the next classified execution's step. An
    action over the persisted micro-batch reads no table path (its plan shows an
    in-memory scan), so "reads nothing" means plan counts. The late MERGE and the
    silver MERGE both read silver and late (each anti-joins the other table), so a MERGE
    takes its step from the count just before it: the silver lookup (late write) or the
    late lookup (silver MERGE).
    """
    plan = str(execution.get("planDescription", ""))
    data, log_only = tables_read(plan)
    if is_merge(execution):
        return "late write" if current == "late write" else "silver MERGE"
    if "control" in data or ("control" in log_only and not data):
        before_counts = current in (None, "wm read")
        return "wm read" if before_counts else "control write"
    if not data:
        return None if is_snapshot(plan) or "CreateViewCommand" in plan else "plan counts"
    if "late" in data:
        return "late anti-join + count"
    if "silver" in data:
        return "late write"
    return "plan counts"


@dataclass
class Exec:
    """One SQL execution, reduced to what the method uses."""

    id: int
    start: float
    end: float
    jobs: list[int]
    raw: dict[str, Any]
    children: list[Exec] = field(default_factory=list)

    @property
    def ms(self) -> float:
        return (self.end - self.start) * 1000

    def all_jobs(self) -> list[int]:
        return self.jobs + [j for c in self.children for j in c.all_jobs()]


def to_exec(e: dict[str, Any]) -> Exec:
    start = parse_time(str(e["submissionTime"]))
    jobs = [int(j) for key in ("successJobIds", "failedJobIds", "runningJobIds")
            for j in e.get(key, [])]
    return Exec(int(e["id"]), start, start + int(e["duration"]) / 1000, jobs, e)


def nest(execs: list[Exec]) -> tuple[Exec, list[Exec]]:
    """(outer, top-level executions with their children attached), in time order."""
    if not execs:
        raise ProbeError("no executions")
    ordered = sorted(execs, key=lambda x: (x.start, -x.end, x.id))
    outer, rest = ordered[0], ordered[1:]
    top: list[Exec] = []
    for x in rest:
        parent = next((p for p in reversed(top) if p.start <= x.start and x.end <= p.end), None)
        if parent is None:
            top.append(x)
        else:
            parent.children.append(x)
    return outer, top


def steps_of(outer: Exec, top: list[Exec], jobs: dict[int, dict[str, Any]]) -> dict[str, Any]:
    """Wall / Spark / driver ms, jobs and tasks per step."""
    labels: list[str | None] = []
    current: str | None = None
    for x in top:
        label = classify(x.raw, current)
        labels.append(label)
        if label is not None:
            current = label
    # Snapshot loads take the next classified step (else the previous one).
    for i, label in enumerate(labels):
        if label is None:
            nxt = next((lab for lab in labels[i + 1:] if lab is not None), None)
            prev = next((lab for lab in reversed(labels[:i]) if lab is not None), None)
            labels[i] = nxt or prev or "plan counts"
    order = [s for s in STEPS if s in labels]
    out: dict[str, Any] = {}
    prev_end = outer.start
    for step in order:
        members = [x for x, lab in zip(top, labels, strict=True) if lab == step]
        end = max(x.end for x in members)
        spark_ms = sum(x.ms for x in members)
        wall_ms = (end - prev_end) * 1000
        job_ids = [j for x in members for j in x.all_jobs()]
        tasks_all = sum(int(jobs[j].get("numTasks", 0)) for j in job_ids if j in jobs)
        tasks = tasks_all - sum(int(jobs[j].get("numSkippedTasks", 0))
                                for j in job_ids if j in jobs)
        out[step] = {"wall_ms": round(wall_ms), "spark_ms": round(spark_ms),
                     "driver_ms": round(wall_ms - spark_ms), "jobs": len(job_ids),
                     "tasks": tasks, "tasks_incl_skipped": tasks_all,
                     "executions": [x.id for x in members]}
        views = [x for x in members if "CreateViewCommand" in str(x.raw.get("planDescription"))]
        if views:   # what the job's merge_ms / late_ms timer covers: after the view
            out[step]["after_view_ms"] = round((end - views[-1].end) * 1000)
        prev_end = max(prev_end, end)
    out["other"] = {"wall_ms": round((outer.end - prev_end) * 1000)}
    return out


def batch_of(description: str, query: str) -> int | None:
    """Batch id of a silver execution, or None if it belongs to another query."""
    lines = [ln.strip() for ln in description.splitlines() if ln.strip()]
    if not lines or query not in lines[0].split():
        return None
    m = re.search(r"(?m)^\s*batch = (\d+)\b", description)
    return int(m.group(1)) if m else None


# --- I/O ----------------------------------------------------------------------------


def get_json(url: str) -> Any:
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 (local UI)
            return json.load(resp)
    except (OSError, ValueError) as exc:
        raise ProbeError(f"{url}: {exc}") from exc


def job_logs(container: str, query: str) -> dict[int, dict[str, Any]]:
    """batch id -> merged fields of the 'silver batch' and 'batch progress' log lines."""
    try:
        res = subprocess.run(["docker", "logs", container], capture_output=True, text=True,
                             check=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProbeError(f"docker logs {container}: {exc}") from exc
    found: dict[int, dict[str, Any]] = {}
    for line in (res.stdout + res.stderr).splitlines():
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("query") != query or rec.get("msg") not in ("silver batch", "batch progress"):
            continue
        found.setdefault(int(rec["batch_id"]), {}).update(rec)
    return found


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("batches", nargs="*", type=int, help="silver batch ids")
    ap.add_argument("--list", action="store_true", help="list silver batches in the UI")
    ap.add_argument("--out", help="write the JSON result (and the raw REST data) here")
    args = ap.parse_args(argv)
    ui = os.environ.get("SPARK_UI_URL", "http://localhost:4040").rstrip("/")
    container = os.environ.get("SPARK_CONTAINER", "valkyrie-spark")
    query = os.environ.get("SILVER_QUERY_NAME", "silver")

    app = str(get_json(f"{ui}/api/v1/applications")[0]["id"])
    base = f"{ui}/api/v1/applications/{app}"
    sql = get_json(f"{base}/sql?details=true&planDescription=true&length=1000000")
    by_batch: dict[int, list[dict[str, Any]]] = {}
    for e in sql:
        b = batch_of(str(e.get("description", "")), query)
        if b is not None:
            by_batch.setdefault(b, []).append(e)
    if args.list or not args.batches:
        print(json.dumps({"app": app, "silver_batches": sorted(by_batch)}))
        return 0
    jobs = {int(j["jobId"]): j for j in get_json(f"{base}/jobs")}
    logs = job_logs(container, query)

    result: dict[str, Any] = {"app": app, "ui": ui, "taken_at": datetime.now(UTC).isoformat(),
                              "batches": {}}
    raw: dict[str, Any] = {}
    for b in args.batches:
        if b not in by_batch:
            raise ProbeError(f"batch {b}: no SQL executions in the UI (have {sorted(by_batch)})")
        outer, top = nest([to_exec(e) for e in by_batch[b]])
        steps = steps_of(outer, top, jobs)
        lg = logs.get(b, {})
        merge = steps.get("silver MERGE", {})
        result["batches"][str(b)] = {
            "rows_in": lg.get("rows_in"), "late_rows": lg.get("late_rows"),
            "add_batch_ms_logged": lg.get("add_batch_ms"), "outer_ms": round(outer.ms),
            "steps": steps,
            "check": {
                "outer_minus_add_batch_ms": (round(outer.ms) - lg["add_batch_ms"]
                                             if lg.get("add_batch_ms") is not None else None),
                "merge_after_view_minus_logged_ms": (
                    merge["after_view_ms"] - lg["merge_ms"]
                    if merge.get("after_view_ms") is not None and lg.get("merge_ms") else None),
                "wm_wall_minus_logged_ms": (steps["wm read"]["wall_ms"] - lg["wm_read_ms"]
                                            if "wm read" in steps and lg.get("wm_read_ms")
                                            is not None else None),
            },
        }
        raw[str(b)] = {"executions": by_batch[b],
                       "jobs": {str(j): jobs[j] for x in [outer, *top] for j in x.all_jobs()
                                if j in jobs}}
    per_step = {s: median(v["steps"][s]["wall_ms"] for v in result["batches"].values())
                for s in STEPS
                if all(s in v["steps"] for v in result["batches"].values())}
    result["median_wall_ms"] = {
        "add_batch": median(v["outer_ms"] for v in result["batches"].values()), **per_step}
    print(json.dumps(result, indent=1))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=1)
        with open(args.out.removesuffix(".json") + ".raw.json", "w", encoding="utf-8") as f:
            json.dump(raw, f)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ProbeError as exc:
        print(f"silver_steps: {exc}", file=sys.stderr)
        sys.exit(2)
