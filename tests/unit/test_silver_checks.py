"""Unit tests for e2e.silver_checks (log parsing, MERGE timing, pruning evidence)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.spark_rest import MergeScan
from e2e import silver_checks


def _rec(batch: int, merge_ms: int, skipped: bool = False, inserted: int = 1) -> str:
    return json.dumps({"msg": "silver batch", "batch_id": batch, "rows_in": 5,
                       "merge_skipped": skipped, "merge_ms": merge_ms,
                       "inserted_this_attempt": inserted,
                       "merge_lo": None if skipped else "2026-11-09",
                       "merge_hi": None if skipped else "2026-11-10"})


def test_records_and_timing() -> None:
    lines = ["not json", _rec(0, 900), '{"msg": "batch progress", "batch_id": 0}',
             _rec(1, 0, skipped=True), _rec(2, 12_000)]
    recs = silver_checks.silver_batch_records(lines)
    assert [r["batch_id"] for r in recs] == [0, 1, 2]
    t = silver_checks.merge_timing(recs, 10.0)
    assert (t["batches"], t["batches_with_merge"], t["max_merge_ms"], t["over_trigger"]) == (
        3, 2, 12_000, 1)
    assert silver_checks.merge_timing([], 10.0)["max_merge_ms"] is None


def _commit(table: Path, v: int, actions: list[dict]) -> None:
    log = table / "_delta_log"
    log.mkdir(parents=True, exist_ok=True)
    (log / f"{v:020d}.json").write_text("\n".join(json.dumps(a) for a in actions))


def test_scan_evidence_uses_version_before_the_merge(tmp_path: Path,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    t = tmp_path / "silver"
    add = lambda p, d: {"add": {"path": p, "partitionValues": {"event_date": d}}}  # noqa: E731
    _commit(t, 0, [{"metaData": {"id": "s"}}])
    _commit(t, 1, [{"commitInfo": {"operation": "MERGE"}}, add("a", "2026-11-01"),
                   add("b", "2026-11-09")])
    _commit(t, 2, [{"commitInfo": {"operation": "MERGE"}}, add("c", "2026-11-10")])
    monkeypatch.setattr(silver_checks.spark_rest, "newest_merge_scan",
                        lambda ui: MergeScan(7, "MERGE", 1, 1))
    ev = silver_checks.merge_scan_evidence("http://ui", t, silver_checks.silver_batch_records(
        [_rec(4, 300)]))
    assert ev["measured"] and (ev["files_in_window"], ev["live_files"]) == (1, 2)
    assert ev["matches_log"] is True
    none = silver_checks.merge_scan_evidence("http://ui", t, [])
    assert none["measured"] is False
