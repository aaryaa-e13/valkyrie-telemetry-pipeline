"""Pure judges and host-side Delta readers of smoke-faults (e2e/faults_check.py)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from e2e import faults_check as fc

B, C = {"b1", "b2"}, {"c1"}


def _log(name: str, **fields: Any) -> str:
    return json.dumps({"msg": name, **fields})


ACTIVE = _log("FAULT INJECTION ACTIVE point=after_late")
REACHED = _log("FAULT REACHED point=after_late batch=7", point="after_late", batch_id=7)
MARKER = {"point": "after_late", "batch_id": 7, "late_rows": 2, "ontime_rows": 3}


def test_fired_needs_marker_log_line_exit_137_and_no_oom() -> None:
    assert fc.fired_problems("after_late", MARKER, [ACTIVE, REACHED], 137, False) == []
    assert "no marker" in fc.fired_problems("after_late", None, [], 137, False)[0]
    p = fc.fired_problems("after_late", MARKER, [ACTIVE], 1, True)
    assert any("0 'FAULT REACHED" in x for x in p) and any("exit code 1" in x for x in p)
    assert any("OOM" in x for x in p)
    fb = _log("FAULT FALLBACK point=after_late batch=7")
    assert any("FALLBACK" in x for x in
               fc.fired_problems("after_late", MARKER, [ACTIVE, REACHED, fb], 137, False))
    one_kind = {**MARKER, "ontime_rows": 0}
    assert any("both late and on-time" in x for x in
               fc.fired_problems("after_late", one_kind, [ACTIVE, REACHED], 137, False))


def rows(ids: set[str], n: int = 7, q: str = "q") -> list[dict[str, Any]]:
    return [{"event_id": e, "silver_query_id": q, "silver_batch_id": n} for e in sorted(ids)]


DOWN = {"offsets": True, "commits": False}


@pytest.mark.parametrize(("point", "late", "silver", "ctl"), [
    ("after_read", set(), set(), 0), ("after_late", B, set(), 0),
    ("after_silver", B, C, 0), ("after_control", B, C, 1)])
def test_down_state_per_point(point: str, late: set[str], silver: set[str], ctl: int) -> None:
    control = [{"silver_query_id": "q", "batch_id": 7}] * ctl
    assert fc.down_state_problems(point, 7, "q", B, C, rows(late), rows(silver), control,
                                  DOWN) == []
    # One step further than the point claims is caught ...
    assert fc.down_state_problems(point, 7, "q", B, C, rows(B), rows(C),
                                  [{"silver_query_id": "q", "batch_id": 7}], DOWN) != [] \
        or point == "after_control"
    # ... and so is a completed batch (the kill came too late).
    assert any("commits/7 exists" in x for x in fc.down_state_problems(
        point, 7, "q", B, C, rows(late), rows(silver), control,
        {"offsets": True, "commits": True}))


def test_down_state_rows_must_come_from_batch_n() -> None:
    p = fc.down_state_problems("after_late", 7, "q", B, C, rows(B, n=6), [], [], DOWN)
    assert p == ["late holds 0 of 2 B ids from batch 7 (want all)"]


def test_guard_exact_t_c_p() -> None:
    ok = {"mode": "resume", "control_txn": 6, "checkpoint_committed": 6,
          "checkpoint_planned": 7}
    bronze = {"mode": "resume"}
    assert fc.guard_problems("after_silver", 7, ok, bronze) == []
    assert fc.guard_problems("after_control", 7, {**ok, "control_txn": 7}, bronze) == []
    assert fc.guard_problems("after_control", 7, ok, bronze) != []          # T must be C + 1
    assert fc.guard_problems("after_late", 7, None, bronze) != []
    assert fc.guard_problems("after_late", 7, ok, {"mode": "refuse"}) != []


@pytest.mark.parametrize(("point", "late_ins", "ins"), [
    ("after_read", 2, 1), ("after_late", 0, 1), ("after_silver", 0, 0), ("after_control", 2, 1)])
def test_control_row_tells_which_attempt_wrote_it(point: str, late_ins: int, ins: int) -> None:
    row = {"late_rows": 2, "ontime_rows": 4, "in_initial_snapshot": False,
           "late_dup_of_silver": 0, "dup_of_late": 0, "late_inserted_this_attempt": late_ins,
           "inserted_this_attempt": ins, "watermark_used": 5}
    assert fc.control_row_problems(point, row, 2, 1, 3, 5) == []
    assert fc.control_row_problems(point, {**row, "inserted_this_attempt": 9}, 2, 1, 3, 5) != []
    assert fc.control_row_problems(point, row, 2, 1, 3, 6) != []           # wm formula
    assert fc.control_row_problems(point, None, 2, 1, 3, 5) != []


def report(s: int, lt: int, **kw: Any) -> dict[str, Any]:
    return {"result": "PASS", "silver": {"found_unique_ids": s + lt, "duplicate_rows": 0,
                                         "silver_found_ids": s, "late_found_ids": lt,
                                         "rows_from_this_send": s + lt,
                                         "consistency": {"checkable": True},
                                         "oracle": {"verdicts": {"R4": "not checkable: x"}},
                                         **kw}}


def test_run_problems() -> None:
    assert fc.run_problems("B", report(0, 2), "late", 2) == []
    assert fc.run_problems("C", report(3, 0), "silver", 3) == []
    assert fc.run_problems("B", report(1, 1), "late", 2) != []
    assert fc.run_problems("A", report(3, 0, rows_from_this_send=2), "silver", 3) != []
    bad = report(3, 0)
    bad["silver"]["oracle"]["verdicts"]["R6b"] = "FAIL"
    assert fc.run_problems("C", bad, "silver", 3) == ["C: R6b FAIL"]


def test_classify_bronze() -> None:
    assert fc.classify_bronze(5, {"offsets": True, "commits": False}, 4) == "i"
    assert fc.classify_bronze(5, {"offsets": True, "commits": False}, 5) == "ii"
    assert fc.classify_bronze(5, {"offsets": True, "commits": True}, 5) == "iii"
    assert fc.classify_bronze(5, {"offsets": False, "commits": False}, 4) == "unknown"


def _fake_table(root: Path) -> Path:
    """A Delta-shaped folder: 2 commits, 2 live files, plus 1 empty and 1 full orphan."""
    t = root / "t"
    (t / "_delta_log" / "_staged_commits").mkdir(parents=True)
    tab = pa.table({"event_id": ["a", "b", "c"]})
    pq.write_table(tab.slice(0, 2), t / "p1.parquet")
    pq.write_table(tab.slice(2, 1), t / "p2.parquet")
    pq.write_table(tab.slice(0, 0), t / "empty.parquet")             # 0-row orphan
    pq.write_table(tab, t / "killed.parquet")                        # orphan with rows
    for v, (name, n) in enumerate((("p1.parquet", 2), ("p2.parquet", 1))):
        add = {"add": {"path": name, "dataChange": True,
                       "stats": json.dumps({"numRecords": n})}}
        (t / "_delta_log" / f"{v:020d}.json").write_text(json.dumps(add) + "\n")
    (t / "_delta_log" / "_staged_commits" / "x.json").write_text("{}")
    return t


def test_table_state_counts_rows_and_orphans(tmp_path: Path) -> None:
    st = fc.table_state(_fake_table(tmp_path))
    assert (st.version, st.rows, st.live_files) == (1, 3, 2)
    assert (st.orphan_files_empty, st.orphan_files_with_rows, st.orphan_rows) == (1, 1, 3)
    assert st.log_other_files == ["_staged_commits/x.json"]
    assert sorted(r["event_id"] for r in fc.live_rows(tmp_path / "t", ["event_id"])) == \
        ["a", "b", "c"]
    before = {"t": st.as_dict()}
    after = {"t": {**st.as_dict(), "rows": 5, "orphan_files_with_rows": 2}}
    assert fc.deltas(before, after)["t"] == {"rows": 2, "orphan_files_empty": 0,
                                            "orphan_files_with_rows": 1, "orphan_rows": 0}
    assert not list((tmp_path / "t").glob("*.deleted"))     # K3: nothing removed


def test_table_state_refuses_without_commit_0(tmp_path: Path) -> None:
    t = _fake_table(tmp_path)
    (t / "_delta_log" / f"{0:020d}.json").unlink()
    with pytest.raises(ValueError, match="commit 0"):
        fc.table_state(t)


def test_live_rows_fills_columns_an_older_file_lacks(tmp_path: Path) -> None:
    t = _fake_table(tmp_path)
    pq.write_table(pa.table({"event_id": ["d"], "extra": [7]}), t / "p3.parquet")
    add = {"add": {"path": "p3.parquet", "dataChange": True,
                   "stats": json.dumps({"numRecords": 1})}}
    (t / "_delta_log" / f"{2:020d}.json").write_text(json.dumps(add) + "\n")
    got = sorted(fc.live_rows(t, ["event_id", "extra"]), key=lambda r: r["event_id"])
    assert got == [{"event_id": "a", "extra": None}, {"event_id": "b", "extra": None},
                   {"event_id": "c", "extra": None}, {"event_id": "d", "extra": 7}]
