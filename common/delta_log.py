"""Read a Delta table's transaction log with the standard library only (no Spark).

The Delta log (`<table>/_delta_log/NNNNNNNNNNNNNNNNNNNN.json`) is a public,
documented protocol: one JSON file per commit, one JSON action per line
(`add`, `remove`, `txn`, `commitInfo`, ...). Unlike Spark's checkpoint files,
it is safe to read from outside Spark.

Used by:
    * streaming.delta_compat_check (slice 1 gate): `read_commits`, `reconcile`,
      `txn_actions` (moved here unchanged in slice 2, DECISIONS D2.2).
    * e2e.smoke_restart: `current_version` and `appended_between`, an
      independent row count for the bronze table (replaces Parquet footers).

Only JSON commit files are read. Delta's `.checkpoint.parquet` files are a
compacted copy of earlier commits and are not needed for a version range,
because Delta keeps every commit JSON for its log retention (30 days by
default; a missing file raises `DeltaLogError`, it is never skipped).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

COMMIT_RE = re.compile(r"^\d{20}\.json$")


class DeltaLogError(Exception):
    """The log cannot prove a count (gap, missing stats, non-append action)."""


def _log_dir(table: Path | str) -> Path:
    return Path(table) / "_delta_log"


def read_commit(table: Path | str, version: int) -> list[dict[str, Any]]:
    """Actions of one commit. Raises DeltaLogError if the commit file is missing."""
    f = _log_dir(table) / f"{version:020d}.json"
    try:
        text = f.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise DeltaLogError(f"commit file missing: {f}") from exc
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def read_commits(table: Path) -> list[tuple[int, list[dict[str, Any]]]]:
    """All JSON commits of a Delta table as (version, actions), oldest first."""
    out = []
    for f in sorted(_log_dir(table).iterdir()):
        if COMMIT_RE.match(f.name):
            actions = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
            out.append((int(f.name[:20]), actions))
    return out


def current_version(table: Path | str) -> int | None:
    """Highest committed version (newest commit JSON), or None if there is no table."""
    log = _log_dir(table)
    if not log.is_dir():
        return None
    versions = [int(f.name[:20]) for f in log.iterdir() if COMMIT_RE.match(f.name)]
    return max(versions) if versions else None


def _dv_key(action: dict[str, Any]) -> str:
    dv = action.get("deletionVector") or {}
    return f"{dv.get('storageType', '')}{dv.get('pathOrInlineDv', '')}{dv.get('offset', '')}"


def reconcile(commits: list[tuple[int, list[dict[str, Any]]]]) -> dict[str, Any]:
    """Replay add/remove actions; sum live numRecords minus deletion vectors.

    A file is identified by (path, deletion vector), because Delta re-adds the
    same path with a new deletion vector when rows are deleted from it.
    """
    live: dict[tuple[str, str], dict[str, Any]] = {}
    removes = 0
    for _, actions in commits:
        for action in actions:
            if "add" in action:
                add = action["add"]
                live[(add["path"], _dv_key(add))] = add
            elif "remove" in action:
                rem = action["remove"]
                removes += 1
                live.pop((rem["path"], _dv_key(rem)), None)
    records = 0
    missing_stats = 0
    dv_files = 0
    for add in live.values():
        stats = json.loads(add["stats"]) if add.get("stats") else {}
        if "numRecords" not in stats:
            missing_stats += 1
            continue
        dv = add.get("deletionVector")
        dv_files += 1 if dv else 0
        records += int(stats["numRecords"]) - int((dv or {}).get("cardinality", 0))
    return {"live_files": len(live), "removes": removes, "records": records,
            "files_without_numRecords": missing_stats, "files_with_deletion_vector": dv_files}


def live_adds(commits: list[tuple[int, list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    """The live `add` actions after replaying the commits (needs commits from 0)."""
    live: dict[tuple[str, str], dict[str, Any]] = {}
    for _, actions in commits:
        for action in actions:
            if "add" in action:
                live[(action["add"]["path"], _dv_key(action["add"]))] = action["add"]
            elif "remove" in action:
                live.pop((action["remove"]["path"], _dv_key(action["remove"])), None)
    return list(live.values())


def live_files_in_range(commits: list[tuple[int, list[dict[str, Any]]]], column: str,
                        lo: str, hi: str) -> tuple[int, int]:
    """(live files whose partition value of `column` is in [lo, hi], all live files).

    Values are compared as strings, which is correct for ISO dates (YYYY-MM-DD).
    Used as the independent expectation for MERGE partition pruning (D2.3).
    Raises DeltaLogError if the commits do not start at version 0.
    """
    if not commits or commits[0][0] != 0:
        raise DeltaLogError("live files need every commit from version 0 (log was cleaned up)")
    adds = live_adds(commits)
    inside = sum(1 for a in adds
                 if (v := (a.get("partitionValues") or {}).get(column)) is not None
                 and lo <= v <= hi)
    return inside, len(adds)


def table_id(table: Path | str) -> str | None:
    """The table's id (`metaData.id`, the Delta source's reservoirId) from the newest
    commit JSON that has a metaData action; None if no remaining commit has one."""
    log = _log_dir(table)
    if not log.is_dir():
        return None
    versions = sorted((int(f.name[:20]) for f in log.iterdir() if COMMIT_RE.match(f.name)),
                      reverse=True)
    for v in versions:
        for action in read_commit(table, v):
            if "metaData" in action:
                return str(action["metaData"]["id"])
    return None


def txn_actions(commits: list[tuple[int, list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    """The `txn` (SetTransaction) actions in commit order, with their version."""
    return [{"delta_version": v, "appId": a["txn"]["appId"], "version": a["txn"]["version"]}
            for v, actions in commits for a in actions if "txn" in a]


@dataclass(frozen=True)
class AppendSummary:
    """What commits (v_before, v_after] added to an append-only table."""

    v_before: int
    v_after: int
    rows: int
    files: int
    commits: int
    txns: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"v_before": self.v_before, "v_after": self.v_after, "rows": self.rows,
                "files": self.files, "commits": self.commits, "txns": self.txns}


def appended_between(table: Path | str, v_before: int, v_after: int) -> AppendSummary:
    """Rows added by commits v_before+1 .. v_after, read from the commit JSON files.

    Counts `add` actions with dataChange=true (OPTIMIZE re-adds rows with
    dataChange=false; those are not new rows). Pass v_before = -1 to start at
    version 0.

    Raises:
        DeltaLogError: v_after < v_before, a commit file in the range is
            missing, an `add` has no numRecords (stats off: never count it as
            0), or a commit deletes data (a dataChange=true `remove` or a
            deletion vector), which an append-only bronze table must never do.
    """
    if v_after < v_before:
        raise DeltaLogError(f"version went backwards: {v_before} -> {v_after}")
    rows = files = 0
    txns: list[dict[str, Any]] = []
    for v in range(v_before + 1, v_after + 1):
        for action in read_commit(table, v):
            if "add" in action:
                add = action["add"]
                if add.get("deletionVector"):
                    raise DeltaLogError(f"version {v}: add with a deletion vector "
                                        f"({add.get('path')}); rows were deleted")
                if not add.get("dataChange", True):
                    continue
                stats = json.loads(add["stats"]) if add.get("stats") else {}
                if "numRecords" not in stats:
                    raise DeltaLogError(f"version {v}: add without numRecords "
                                        f"({add.get('path')}); cannot count rows")
                rows += int(stats["numRecords"])
                files += 1
            elif "remove" in action and action["remove"].get("dataChange", True):
                raise DeltaLogError(f"version {v}: remove with dataChange=true "
                                    f"({action['remove'].get('path')}); not append-only")
            elif "txn" in action:
                txns.append({"delta_version": v, "appId": action["txn"]["appId"],
                             "version": action["txn"]["version"]})
    return AppendSummary(v_before, v_after, rows, files, v_after - v_before, txns)


def latest_txn_in_commits(table: Path | str, app_id: str) -> tuple[int | None, bool]:
    """Newest txn version for `app_id`, scanning commit JSONs from newest to oldest.

    A streaming query's txn version only grows, so the newest commit that has
    one holds the maximum. Returns (version or None, complete): complete is True
    when the scan reached version 0 (or found the txn), so None really means
    "no txn". complete is False when older commit files were removed by log
    cleanup; then the answer is in the Delta checkpoint (`*.checkpoint*.parquet`),
    which needs a Parquet reader (streaming.bronze uses Spark for that).
    """
    log = _log_dir(table)
    if not log.is_dir():
        return None, True
    versions = sorted((int(f.name[:20]) for f in log.iterdir() if COMMIT_RE.match(f.name)),
                      reverse=True)
    for v in versions:
        for action in read_commit(table, v):
            if "txn" in action and action["txn"].get("appId") == app_id:
                return int(action["txn"]["version"]), True
    return None, not versions or versions[-1] == 0


def newest_checkpoint_files(table: Path | str) -> list[Path]:
    """Files of the newest Delta checkpoint (classic, multi-part or V2 top file)."""
    log = _log_dir(table)
    if not log.is_dir():
        return []
    cps = [f for f in log.iterdir()
           if re.match(r"^\d{20}\.checkpoint.*\.parquet$", f.name)]
    if not cps:
        return []
    newest = max(f.name[:20] for f in cps)
    return sorted(f for f in cps if f.name.startswith(newest))
