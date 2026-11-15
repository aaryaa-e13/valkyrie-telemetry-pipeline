"""Tests for streaming.verify_manifest (run inside the Spark image: make test-spark).

Covers: manifest v1, v2 and v3 are all accepted, duplicates are reported but only
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
V3: dict[str, Any] = {
    **copy.deepcopy(V2), "manifest_version": 3,
    "pacing": {
        "mode": "realtime", "time_scale": 10.0, "assumed_watermark_s": 30.0,
        "late_fraction": 0.3, "late_too_late_share": 0.2,
        "bands": {
            "slight": {"min_x_watermark": 0.1, "max_x_watermark": 0.5,
                       "min_delay_ms": 3000, "max_delay_ms": 15000, "expected": "keep"},
            "very": {"min_x_watermark": 2.0, "max_x_watermark": 3.0,
                     "min_delay_ms": 60000, "max_delay_ms": 90000, "expected": "may_drop"},
        },
        "measured": {"sent": 3, "future_events": 0, "max_ahead_ms": 0, "max_behind_ms": 0},
    },
    "late_counts": {"slight": 0, "very": 1},
    "late_events_omitted_reason": None,
    # A very late event is still required in the raw layer.
    "late_events": [{"event_id": "e3", "match_id": "m-a", "delay_ms": 70000, "band": "very",
                     "expected": "may_drop", "stream_lag_ms": 5000}],
}


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


@pytest.mark.parametrize("manifest", [V1, V2, V3], ids=["v1", "v2", "v3"])
def test_complete_run_passes_for_v1_v2_and_v3(spark, tmp_path: Path, manifest) -> None:
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
