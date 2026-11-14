"""Tests for streaming.verify_manifest (run inside the Spark image: make test-spark).

Covers: manifest v1 and v2 are both accepted, duplicates are reported but only
fail with --fail-on-duplicates, missing ids always fail, unrelated rows
(other runs, malformed messages) never affect the result.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from streaming.verify_manifest import evaluate, main, verify  # noqa: E402

V1: dict[str, Any] = {
    "manifest_version": 1, "run_id": "r1", "seed": 501, "topic": "game-events",
    "totals": {"matches": 1, "events_generated": 3, "events_delivered": 3, "events_failed": 0},
    "matches": {"m-a": {"events_generated": 3, "events_delivered": 3, "max_match_seq": 3,
                        "partitions": [3], "event_ids": ["e1", "e2", "e3"]}},
    "failed_event_ids": [],
}
V2: dict[str, Any] = {**copy.deepcopy(V1), "manifest_version": 2,
                      "kafka_offsets": {"3": {"min": 0, "max": 2, "count": 3}}}


def _lake(spark: SparkSession, tmp_path: Path, event_ids: list[str | None]) -> str:
    """Write a tiny raw table: given event_ids plus unrelated noise rows."""
    rows = [(e, e is not None) for e in event_ids]
    rows += [("other-run-1", True), ("other-run-1", True), (None, False)]  # noise
    path = str(tmp_path / "lake")
    spark.createDataFrame(rows, "event_id string, parse_ok boolean").write.parquet(path)
    return path


def _manifest(tmp_path: Path, data: dict[str, Any]) -> Path:
    path = tmp_path / "run.json"
    path.write_text(json.dumps(data))
    return path


@pytest.mark.parametrize("manifest", [V1, V2], ids=["v1", "v2"])
def test_complete_run_passes_for_v1_and_v2(spark, tmp_path: Path, manifest) -> None:
    lake = _lake(spark, tmp_path, ["e1", "e2", "e3"])
    report = verify(spark, _manifest(tmp_path, manifest), lake)
    assert report["manifest_version"] == manifest["manifest_version"]
    assert (report["missing_ids"], report["duplicate_rows"]) == (0, 0)
    assert report["parquet_total_rows"] == 6        # noise is counted ...
    assert report["parquet_unparsed_rows"] == 1
    assert evaluate(report, fail_on_duplicates=True)[0] == "PASS"  # ... but never judged


def test_duplicates_reported_but_fail_only_with_flag(spark, tmp_path: Path) -> None:
    report = verify(spark, _manifest(tmp_path, V1),
                    _lake(spark, tmp_path, ["e1", "e2", "e3", "e1", "e2", "e3"]))
    assert report["duplicate_rows"] == 3
    assert report["max_copies_of_one_id"] == 2
    result, notes = evaluate(report, fail_on_duplicates=False)
    assert result == "PASS"
    assert any("duplicate rows reported" in n for n in notes)
    assert evaluate(report, fail_on_duplicates=True)[0] == "FAIL"


def test_missing_ids_always_fail(spark, tmp_path: Path) -> None:
    report = verify(spark, _manifest(tmp_path, V2), _lake(spark, tmp_path, ["e1"]))
    assert report["missing_ids"] == 2
    assert sorted(report["missing_examples"]) == ["e2", "e3"]
    assert evaluate(report, fail_on_duplicates=False)[0] == "FAIL"


def test_unusable_manifest_exits_2_before_starting_spark(tmp_path: Path) -> None:
    bad = copy.deepcopy(V1)
    bad["manifest_version"] = 99
    assert main(["--manifest", str(_manifest(tmp_path, bad))]) == 2
