"""Silver evidence for the e2e runs: MERGE timing, pruning, late path (D2.3, D2.4).

Pure functions over the job's JSON log lines, plus one function that reads the
Spark UI's REST API and the silver Delta log. Nothing here can fail a run: the
evidence is reported, and "not measured" is said plainly when it is missing.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from common import spark_rest
from common.delta_log import DeltaLogError, live_files_in_range, read_commits


def silver_batch_records(lines: Iterable[str]) -> list[dict[str, Any]]:
    """The "silver batch" log records (one per processed silver batch), in log order."""
    out = []
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("msg") == "silver batch" and isinstance(rec.get("batch_id"), int):
            out.append(rec)
    return out


def merge_timing(records: list[dict[str, Any]], trigger_s: float) -> dict[str, Any]:
    """MERGE duration per batch vs the trigger interval (condition 3 of the slice)."""
    merged = [r for r in records if not r.get("merge_skipped")]
    ms = sorted(int(r.get("merge_ms") or 0) for r in merged)
    return {
        "batches": len(records),
        "batches_with_merge": len(merged),
        "merge_ms": [{"batch_id": r["batch_id"], "rows_in": r.get("rows_in"),
                      "inserted": r.get("inserted_this_attempt"), "merge_ms": r.get("merge_ms")}
                     for r in merged][-20:],
        "max_merge_ms": ms[-1] if ms else None,
        "median_merge_ms": ms[len(ms) // 2] if ms else None,
        "trigger_ms": int(trigger_s * 1000),
        "over_trigger": sum(1 for m in ms if m > trigger_s * 1000),
    }


def _median(values: list[int]) -> int | None:
    v = sorted(values)
    return v[len(v) // 2] if v else None


def late_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Late-path evidence from the "silver batch" log records of one run (D2.4).

    Sums are over log records, i.e. over batch ATTEMPTS: a replayed batch is
    logged again. They are evidence, never a check; verify reads the tables.
    """
    def total(key: str) -> int:
        return sum(int(r.get(key) or 0) for r in records)

    late = [r for r in records if (r.get("late_rows") or 0) > 0]
    return {
        "batches": len(records),
        "snapshot_batches": sum(1 for r in records if r.get("in_initial_snapshot")),
        "batches_with_late_rows": len(late),
        "late_rows": total("late_rows"), "late_dup_of_silver": total("late_dup_of_silver"),
        "late_inserted": total("late_inserted_this_attempt"),
        "dup_of_late": total("dup_of_late"), "clamped_rows": total("clamped_rows"),
        "watermarks": sorted({r["watermark_used"] for r in records
                              if r.get("watermark_used")})[-3:],
        "median_wm_read_ms": _median([int(r.get("wm_read_ms") or 0) for r in records]),
        "median_late_ms": _median([int(r.get("late_ms") or 0) for r in late]),
    }


def batch_timing(lines: Iterable[str], trigger_s: float, query: str = "silver"
                 ) -> dict[str, Any]:
    """addBatch time per batch with input rows vs the trigger, from "batch progress" lines.

    Also lists batches whose end offset is in the MIDDLE of a bronze version
    (index != -1): only then can part of one bronze version be an "earlier" batch
    for the rest of it (slice 5 plan S6 1a). Report only.
    """
    recs = []
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("msg") == "batch progress" and rec.get("query") == query \
                and isinstance(rec.get("batch_id"), int):
            recs.append(rec)
    add = [int(r["add_batch_ms"]) for r in recs if r.get("add_batch_ms") is not None]
    trigger_ms = int(trigger_s * 1000)
    return {
        "batches": len(recs),
        "median_add_batch_ms": _median(add), "max_add_batch_ms": max(add) if add else None,
        "trigger_ms": trigger_ms, "over_trigger": sum(1 for a in add if a > trigger_ms),
        "ended_mid_version": [r["batch_id"] for r in recs
                              if int((r.get("end_offset") or {}).get("index", -1)) != -1],
    }


def delta_checkpoints_since(table: Path, since_wall: float) -> list[int]:
    """Versions of `table` that got a Delta checkpoint file at or after `since_wall`."""
    log_dir = table / "_delta_log"
    if not log_dir.is_dir():
        return []
    return sorted(int(f.name.split(".")[0]) for f in log_dir.glob("*.checkpoint.parquet")
                  if f.stat().st_mtime >= since_wall)


def merge_scan_evidence(ui_url: str, silver_path: Path,
                        records: list[dict[str, Any]]) -> dict[str, Any]:
    """Files read by the newest MERGE (Spark scan metrics) vs files in its window (Delta log).

    The newest MERGE execution is matched with the newest "silver batch" record
    that ran a MERGE. The expectation is computed at the silver version BEFORE
    that MERGE: if it inserted rows, its own commit is the newest MERGE commit
    and is excluded. Small race: a batch running while we read can mismatch;
    the result then says so instead of passing.
    """
    merged = [r for r in records if not r.get("merge_skipped")]
    if not merged:
        return {"measured": False, "reason": "no silver batch ran a MERGE in this run"}
    last = merged[-1]
    try:
        scan = spark_rest.newest_merge_scan(ui_url)
    except spark_rest.SparkRestError as exc:
        return {"measured": False, "reason": f"Spark UI not reachable: {exc}"}
    if scan is None:
        return {"measured": False, "reason": "no MERGE execution with scan metrics in the UI"}
    commits = read_commits(silver_path)
    if (last.get("inserted_this_attempt") or 0) > 0:
        merges = [v for v, acts in commits
                  if any((a.get("commitInfo") or {}).get("operation") == "MERGE" for a in acts)]
        if merges:
            commits = [c for c in commits if c[0] < merges[-1]]
    try:
        inside, total = live_files_in_range(commits, "event_date", last["merge_lo"],
                                            last["merge_hi"])
    except DeltaLogError as exc:
        return {"measured": False, "reason": str(exc)}
    return {"measured": True, "batch_id": last["batch_id"], "window": [last["merge_lo"],
                                                                      last["merge_hi"]],
            **scan.as_dict(), "files_in_window": inside, "live_files": total,
            "matches_log": scan.files_read == inside}
