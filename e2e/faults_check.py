"""smoke-faults evidence and judges (Phase 2 slice 6, DECISIONS D2.6).

Two parts, both laptop side (the job is DOWN when most of this runs):
  * read-only readers of the Delta tables and Spark checkpoints under ./data:
    live files from the Delta log (JSON commits from version 0), rows from their
    `numRecords` stats, selected columns of live files with pyarrow, and ORPHAN
    data files (on disk, referenced by no `add` in any commit; condition K3:
    counted, never deleted);
  * pure judges for "the fault fired where claimed" (F4) and the exact outcome
    after the restart (F3). Every judge returns a list of problems; empty = OK.

Not Spark: a different reader of the same Delta log, so it is also an
independent check of what Spark wrote.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from common.delta_log import live_adds, read_commits

SILVER_POINTS = ("after_read", "after_late", "after_silver", "after_control")
LOG_FILE_RE = re.compile(r"^\d{20}\.(json|crc|checkpoint.*\.parquet)$|^_last_checkpoint$")


# --- Delta tables on the host (read-only) -------------------------------------------------

@dataclass(frozen=True)
class TableState:
    version: int | None
    rows: int
    live_files: int
    orphan_files_empty: int       # 0-row data files no commit references (normal: see D2.6)
    orphan_files_with_rows: int   # a killed write would leave these
    orphan_rows: int
    log_other_files: list[str]    # e.g. temp files of an unfinished commit

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parquet_rows(f: Path) -> int:
    import pyarrow.parquet as pq

    return int(pq.read_metadata(f).num_rows)


def table_state(path: Path) -> TableState:
    """Rows and orphans of one Delta table. Raises ValueError if commit 0 is gone."""
    log = path / "_delta_log"
    if not log.is_dir():
        return TableState(None, 0, 0, 0, 0, 0, [])
    commits = read_commits(path)
    if not commits or commits[0][0] != 0:
        raise ValueError(f"{path}: JSON commit 0 is missing; cannot list every add")
    adds = live_adds(commits)
    rows = sum(int(json.loads(a["stats"])["numRecords"]) for a in adds)
    referenced = {unquote(a["add"]["path"]) for _, acts in commits for a in acts if "add" in a}
    empty = with_rows = orphan_rows = 0
    for f in path.rglob("*.parquet"):
        if "_delta_log" in f.parts or str(f.relative_to(path)) in referenced:
            continue
        n = _parquet_rows(f)
        if n == 0:
            empty += 1
        else:
            with_rows += 1
            orphan_rows += n
    other = sorted(f.name for f in log.iterdir() if f.name != "_staged_commits"
                   and not LOG_FILE_RE.match(f.name) and not f.name.startswith("."))
    staged = log / "_staged_commits"           # Delta 4.x; empty unless a commit is in flight
    if staged.is_dir():
        other += sorted(f"_staged_commits/{f.name}" for f in staged.iterdir())
    hidden_tmp = sorted(f.name for f in log.iterdir()
                        if f.name.startswith(".") and not f.name.endswith(".crc"))
    return TableState(commits[-1][0], rows, len(adds), empty, with_rows, orphan_rows,
                      other + hidden_tmp)


def live_rows(path: Path, columns: Iterable[str]) -> list[dict[str, Any]]:
    """Selected columns of every live row (pyarrow over the Delta log's live files)."""
    import pyarrow.parquet as pq

    cols = list(columns)
    out: list[dict[str, Any]] = []
    for a in live_adds(read_commits(path)):
        out += pq.read_table(path / unquote(a["path"]), columns=cols).to_pylist()
    return out


def lake_state(paths: Mapping[str, Path]) -> dict[str, dict[str, Any]]:
    return {name: table_state(p).as_dict() for name, p in paths.items()}


def deltas(before: Mapping[str, Mapping[str, Any]],
           after: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    """K2/K3: change of rows and orphans per table."""
    keys = ("rows", "orphan_files_empty", "orphan_files_with_rows", "orphan_rows")
    return {t: {k: int(after[t][k]) - int(before[t][k]) for k in keys} for t in after}


def checkpoint_batch(cp: Path, n: int) -> dict[str, bool]:
    return {"offsets": (cp / "offsets" / str(n)).is_file(),
            "commits": (cp / "commits" / str(n)).is_file()}


def newest_batch(d: Path) -> int | None:
    ids = [int(f.name) for f in d.iterdir() if f.name.isdigit()] if d.is_dir() else []
    return max(ids) if ids else None


def log_records(lines: Iterable[str], msg_prefix: str) -> list[dict[str, Any]]:
    """JSON log records whose msg starts with `msg_prefix`, in order."""
    out = []
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if str(rec.get("msg", "")).startswith(msg_prefix):
            out.append(rec)
    return out


# --- F4: did the fault fire where claimed? -------------------------------------------------

def fired_problems(point: str, marker: dict[str, Any] | None, killed_logs: list[str],
                   exit_code: int | None, oom_killed: bool) -> list[str]:
    """Marker, log line, exit code 137 without OOM, no fallback."""
    p: list[str] = []
    if marker is None:
        return [f"no marker for {point}: the hook never reached its point"]
    n = marker.get("batch_id")
    if marker.get("point") != point:
        p.append(f"marker names point {marker.get('point')!r}, expected {point!r}")
    if not (marker.get("late_rows", 0) > 0 and marker.get("ontime_rows", 0) > 0):
        p.append(f"batch {n} did not hold both late and on-time rows: {marker}")
    reached = [r for r in log_records(killed_logs, "FAULT REACHED")
               if r.get("point") == point and r.get("batch_id") == n]
    if len(reached) != 1:
        p.append(f"{len(reached)} 'FAULT REACHED point={point} batch={n}' log lines (want 1)")
    if log_records(killed_logs, "FAULT FALLBACK"):
        p.append("the hook killed itself (FAULT FALLBACK): no external kill arrived")
    if not log_records(killed_logs, "FAULT INJECTION ACTIVE"):
        p.append("the killed process never logged FAULT INJECTION ACTIVE")
    if exit_code != 137:
        p.append(f"container exit code {exit_code}, expected 137 (SIGKILL)")
    if oom_killed:
        p.append("container was OOM-killed, not killed by smoke-faults")
    return p


DOWN_EXPECT = {  # point -> (B in late, C in silver, control row N)
    "after_read": (False, False, False),
    "after_late": (True, False, False),
    "after_silver": (True, True, False),
    "after_control": (True, True, True),
}


def down_state_problems(point: str, n: int, query_id: str, b_ids: set[str], c_ids: set[str],
                        late: list[dict[str, Any]], silver: list[dict[str, Any]],
                        control: list[dict[str, Any]], batch_files: Mapping[str, bool]
                        ) -> list[str]:
    """The table state while the job is down (F4 item 4)."""
    want_late, want_silver, want_ctl = DOWN_EXPECT[point]
    p: list[str] = []
    late_b = {r["event_id"] for r in late if r["event_id"] in b_ids}
    late_b_n = {r["event_id"] for r in late if r["event_id"] in b_ids
                and r["silver_query_id"] == query_id and r["silver_batch_id"] == n}
    silver_c = {r["event_id"] for r in silver if r["event_id"] in c_ids}
    silver_c_n = {r["event_id"] for r in silver if r["event_id"] in c_ids
                  and r["silver_query_id"] == query_id and r["silver_batch_id"] == n}
    ctl_n = [r for r in control if r["silver_query_id"] == query_id and r["batch_id"] == n]
    if want_late and late_b_n != b_ids:
        p.append(f"late holds {len(late_b_n)} of {len(b_ids)} B ids from batch {n} (want all)")
    if not want_late and late_b:
        p.append(f"late already holds {len(late_b)} B ids (want none)")
    if want_silver and silver_c_n != c_ids:
        p.append(f"silver holds {len(silver_c_n)} of {len(c_ids)} C ids from batch {n} "
                 "(want all)")
    if not want_silver and silver_c:
        p.append(f"silver already holds {len(silver_c)} C ids (want none)")
    if len(ctl_n) != (1 if want_ctl else 0):
        p.append(f"{len(ctl_n)} control rows for batch {n} (want {1 if want_ctl else 0})")
    if not batch_files.get("offsets"):
        p.append(f"silver offsets/{n} is missing (batch {n} was never planned?)")
    if batch_files.get("commits"):
        p.append(f"silver commits/{n} exists: the batch completed, the kill came too late")
    return p


# --- F3: the outcome after the restart ---------------------------------------------------

def guard_problems(point: str, n: int, silver_guard: dict[str, Any] | None,
                   bronze_guard: dict[str, Any] | None) -> list[str]:
    """The restarted process's start-up guards: exact (T, C, P) per point."""
    p: list[str] = []
    want_txn = n if point == "after_control" else n - 1
    if silver_guard is None:
        p.append("no silver 'start-up state' line after the restart")
    else:
        got = (silver_guard.get("mode"), silver_guard.get("control_txn"),
               silver_guard.get("checkpoint_committed"), silver_guard.get("checkpoint_planned"))
        if got != ("resume", want_txn, n - 1, n):
            p.append(f"silver guard (mode, T, C, P) = {got}, expected "
                     f"('resume', {want_txn}, {n - 1}, {n})")
    if bronze_guard is None or bronze_guard.get("mode") != "resume":
        p.append(f"bronze guard did not resume: {bronze_guard}")
    return p


def control_row_problems(point: str, row: dict[str, Any] | None, n_b: int, n_c: int,
                         n_a: int, wm_expected: Any) -> list[str]:
    """Control row of batch N: deterministic fields, and which attempt wrote it."""
    if row is None:
        return ["no control row for the faulted batch after the restart"]
    want = {"late_rows": n_b, "ontime_rows": n_c + n_a, "in_initial_snapshot": False,
            "late_dup_of_silver": 0, "dup_of_late": 0}
    if point in ("after_read", "after_control"):     # all inserts in the attempt that wrote it
        want.update(late_inserted_this_attempt=n_b, inserted_this_attempt=n_c)
    elif point == "after_late":                      # replay: late already there
        want.update(late_inserted_this_attempt=0, inserted_this_attempt=n_c)
    else:                                            # after_silver: both already there
        want.update(late_inserted_this_attempt=0, inserted_this_attempt=0)
    p = [f"control row {k} = {row.get(k)!r}, expected {v!r}" for k, v in want.items()
         if row.get(k) != v]
    if wm_expected is not None and row.get("watermark_used") != wm_expected:
        p.append(f"control row watermark_used {row.get('watermark_used')} != "
                 f"max(m_k, k < N) - W = {wm_expected}")
    return p


def run_problems(name: str, report: dict[str, Any], expect: str, n_ids: int) -> list[str]:
    """verify's report for run A ('silver', kept from its first send), B ('late') or C."""
    sv = report.get("silver") or {}
    p: list[str] = []
    if report.get("result") != "PASS":
        p.append(f"{name}: verify {report.get('result')}: {report.get('notes')}")
    if sv.get("found_unique_ids") != n_ids or sv.get("duplicate_rows") != 0:
        p.append(f"{name}: silver ∪ late has {sv.get('found_unique_ids')}/{n_ids} ids, "
                 f"{sv.get('duplicate_rows')} duplicate rows")
    s, lt = sv.get("silver_found_ids"), sv.get("late_found_ids")
    if (expect == "late" and (s, lt) != (0, n_ids)) or \
            (expect == "silver" and (s, lt) != (n_ids, 0)):
        p.append(f"{name}: |S| {s}, |L| {lt}; expected all {n_ids} in {expect}")
    if expect == "silver" and sv.get("rows_from_this_send") != n_ids:
        p.append(f"{name}: {sv.get('rows_from_this_send')} of {n_ids} kept rows are from this "
                 "send (A: the first send's copies must stay)")
    v = (sv.get("oracle") or {}).get("verdicts") or {}
    for rule in ("R6a", "R6b", "R6c", "R6d", "R4", "R5"):
        if v.get(rule) == "FAIL":
            p.append(f"{name}: {rule} FAIL")
    if not (sv.get("consistency") or {}).get("checkable"):
        p.append(f"{name}: consistency not checkable: {(sv.get('consistency') or {})}")
    return p


def classify_bronze(n: int, files: Mapping[str, bool], txn: int | None) -> str:
    """inside_bronze outcome: 'i' planned not in Delta, 'ii' in Delta not committed,
    'iii' too late (commits/N exists), or 'unknown'."""
    if not files.get("offsets"):
        return "unknown"
    if files.get("commits"):
        return "iii"
    if txn == n - 1:
        return "i"
    if txn == n:
        return "ii"
    return "unknown"
