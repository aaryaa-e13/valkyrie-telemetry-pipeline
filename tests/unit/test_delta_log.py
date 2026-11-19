"""Unit tests for common.delta_log's append counter (hand-written _delta_log folders).

`read_commits` / `reconcile` / `txn_actions` (moved from the slice 1 gate) are
tested in test_delta_compat_helpers.py.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from common.delta_log import DeltaLogError, appended_between, current_version, read_commit


def _commit(table: Path, version: int, actions: list[dict[str, Any]]) -> None:
    log = table / "_delta_log"
    log.mkdir(parents=True, exist_ok=True)
    (log / f"{version:020d}.json").write_text("\n".join(json.dumps(a) for a in actions))


def _add(path: str, n: int | None, data_change: bool = True, **extra: Any) -> dict:
    add: dict[str, Any] = {"path": path, "dataChange": data_change, **extra}
    if n is not None:
        add["stats"] = json.dumps({"numRecords": n})
    return {"add": add}


def _txn(app: str, version: int) -> dict:
    return {"txn": {"appId": app, "version": version}}


def _table(tmp_path: Path) -> Path:
    t = tmp_path / "bronze"
    _commit(t, 0, [{"commitInfo": {}}, {"metaData": {}}])               # create, no rows
    _commit(t, 1, [{"commitInfo": {}}, _txn("q", 0), _add("a", 5), _add("b", 2)])
    _commit(t, 2, [{"commitInfo": {}}, _txn("q", 1), _add("c", 4)])
    return t


def test_current_version_and_missing_table(tmp_path: Path) -> None:
    t = _table(tmp_path)
    (t / "_delta_log" / "00000000000000000002.checkpoint.parquet").write_text("x")
    (t / "_delta_log" / "_last_checkpoint").write_text("{}")
    assert current_version(t) == 2
    assert current_version(tmp_path / "nope") is None
    (tmp_path / "empty" / "_delta_log").mkdir(parents=True)
    assert current_version(tmp_path / "empty") is None


def test_appended_between_counts_rows_files_and_txns(tmp_path: Path) -> None:
    t = _table(tmp_path)
    s = appended_between(t, 0, 2)
    assert (s.rows, s.files, s.commits) == (11, 3, 2)
    assert s.txns == [{"delta_version": 1, "appId": "q", "version": 0},
                      {"delta_version": 2, "appId": "q", "version": 1}]
    assert appended_between(t, 1, 2).rows == 4
    assert appended_between(t, -1, 2).rows == 11   # from version 0
    assert appended_between(t, 2, 2).as_dict() == {"v_before": 2, "v_after": 2, "rows": 0,
                                                   "files": 0, "commits": 0, "txns": []}


def test_optimize_rewrite_is_not_new_rows(tmp_path: Path) -> None:
    t = _table(tmp_path)
    _commit(t, 3, [{"commitInfo": {"operation": "OPTIMIZE"}},
                   {"remove": {"path": "a", "dataChange": False}},
                   {"remove": {"path": "b", "dataChange": False}},
                   _add("ab", 7, data_change=False)])
    assert appended_between(t, 2, 3).rows == 0


@pytest.mark.parametrize(("actions", "message"), [
    ([_add("x", None)], "add without numRecords"),
    ([{"remove": {"path": "a", "dataChange": True}}], "remove with dataChange=true"),
    ([{"remove": {"path": "a"}}], "remove with dataChange=true"),   # default is true
    ([_add("a", 5, deletionVector={"cardinality": 1})], "deletion vector"),
], ids=["no-stats", "remove", "remove-default", "deletion-vector"])
def test_appended_between_fails_loudly(tmp_path: Path, actions: list[dict],
                                       message: str) -> None:
    t = _table(tmp_path)
    _commit(t, 3, actions)
    with pytest.raises(DeltaLogError, match=message):
        appended_between(t, 2, 3)


def test_gap_and_backwards_range_fail(tmp_path: Path) -> None:
    t = _table(tmp_path)
    _commit(t, 4, [_add("z", 1)])                   # version 3 is missing
    with pytest.raises(DeltaLogError, match="commit file missing"):
        appended_between(t, 2, 4)
    with pytest.raises(DeltaLogError, match="backwards"):
        appended_between(t, 2, 1)
    with pytest.raises(DeltaLogError):
        read_commit(t, 99)
