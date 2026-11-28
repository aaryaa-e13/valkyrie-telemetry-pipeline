"""Tests for streaming.verify_manifest (run inside the Spark image: make test-spark).

Covers: manifest v1, v2 and v3 are all accepted, duplicates are reported but only
fail with --fail-on-duplicates, missing ids always fail, unrelated rows
(other runs, malformed messages) never affect the result. Bronze is Delta
(slice 2): R0 (duplicate Kafka offset) always fails, and --versions reports
time-travel counts.
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


LAKE_SCHEMA = ("event_id string, parse_ok boolean, kafka_topic string, "
               "kafka_partition int, kafka_offset long")


def _lake(spark: SparkSession, tmp_path: Path, event_ids: list[str | None],
          repeat_offset: bool = False) -> str:
    """Write a tiny bronze Delta table: given event_ids plus unrelated noise rows.

    Every row gets its own Kafka offset, as in real bronze. repeat_offset=True
    gives the last row the offset of the first one (an R0 violation).
    """
    ids = [(e, e is not None) for e in event_ids]
    ids += [("other-run-1", True), ("other-run-1", True), (None, False)]  # noise
    rows = [(e, ok, "game-events", 3, i) for i, (e, ok) in enumerate(ids)]
    if repeat_offset:
        rows[-1] = (*rows[-1][:4], 0)
    path = str(tmp_path / "lake")
    spark.createDataFrame(rows, LAKE_SCHEMA).write.format("delta").save(path)
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
    assert report["total_rows"] == 6        # noise is counted ...
    assert report["unparsed_rows"] == 1
    assert report["duplicate_kafka_offsets"] == 0
    assert report["null_kafka_offset_rows"] == 0
    assert report["table_version"] == 0
    assert evaluate(report, fail_on_duplicates=True)[0] == "PASS"  # ... but never judged


@pytest.mark.spark_fast
def test_duplicates_reported_but_fail_only_with_flag(spark, tmp_path: Path) -> None:
    report = verify(spark, _manifest(tmp_path, V1),
                    _lake(spark, tmp_path, ["e1", "e2", "e3", "e1", "e2", "e3"]))
    assert report["duplicate_rows"] == 3
    assert report["max_copies_of_one_id"] == 2
    result, notes = evaluate(report, fail_on_duplicates=False)
    assert result == "PASS"
    assert any("duplicate rows reported" in n for n in notes)
    assert evaluate(report, fail_on_duplicates=True)[0] == "FAIL"


@pytest.mark.spark_fast
def test_duplicate_kafka_offset_always_fails_r0(spark, tmp_path: Path) -> None:
    report = verify(spark, _manifest(tmp_path, V1),
                    _lake(spark, tmp_path, ["e1", "e2", "e3"], repeat_offset=True))
    assert (report["missing_ids"], report["duplicate_rows"]) == (0, 0)
    assert report["duplicate_kafka_offsets"] == 1
    result, notes = evaluate(report, fail_on_duplicates=False)
    assert result == "FAIL"
    assert any(n.startswith("R0:") for n in notes)


@pytest.mark.parametrize("null_col", ["kafka_topic", "kafka_partition", "kafka_offset"])
def test_null_kafka_offset_column_fails_r0(spark, tmp_path: Path, null_col: str) -> None:
    lake = _lake(spark, tmp_path, ["e1", "e2", "e3"])
    row = {"event_id": "x", "parse_ok": True, "kafka_topic": "game-events",
           "kafka_partition": 4, "kafka_offset": 7, null_col: None}
    spark.createDataFrame([tuple(row.values())], LAKE_SCHEMA) \
        .write.format("delta").mode("append").save(lake)
    report = verify(spark, _manifest(tmp_path, V1), lake)
    assert (report["null_kafka_offset_rows"], report["duplicate_kafka_offsets"]) == (1, 0)
    result, notes = evaluate(report, fail_on_duplicates=False)
    assert result == "FAIL"
    assert any("NULL Kafka topic, partition or offset" in n for n in notes)


def test_versions_report_time_travel_counts(spark, tmp_path: Path) -> None:
    lake = _lake(spark, tmp_path, ["e1", "e2", "e3"])  # version 0: 6 rows
    spark.createDataFrame([("e9", True, "game-events", 4, 0)], LAKE_SCHEMA) \
        .write.format("delta").mode("append").save(lake)  # version 1: 7 rows
    report = verify(spark, _manifest(tmp_path, V1), lake, versions=[0, 1])
    assert report["table_version"] == 1
    assert report["version_counts"] == {"0": 6, "1": 7}


@pytest.mark.spark_fast
def test_missing_ids_always_fail(spark, tmp_path: Path) -> None:
    report = verify(spark, _manifest(tmp_path, V2), _lake(spark, tmp_path, ["e1"]))
    assert report["missing_ids"] == 2
    assert sorted(report["missing_examples"]) == ["e2", "e3"]
    assert evaluate(report, fail_on_duplicates=False)[0] == "FAIL"


def test_unusable_manifest_exits_2_before_starting_spark(tmp_path: Path) -> None:
    bad = copy.deepcopy(V1)
    bad["manifest_version"] = 99
    assert main(["--manifest", str(_manifest(tmp_path, bad))]) == 2


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "manifests"


@pytest.mark.parametrize("name", [
    "v1_run_20261114T131326Z_seed42.json",
    "v2_pre_e2_run_20261115T095317Z_seed1794736395320.json",
    "v2_e2_run_20261115T101248Z_seed703.json",
    "v3_run_20261115T105409Z_seed723.json",
], ids=["v1", "v2_pre_e2", "v2_e2", "v3"])
def test_real_manifest_fixtures_of_every_version_verify(spark, tmp_path: Path, name: str) -> None:
    """Real manifests (committed copies) against a lake that holds exactly their ids."""
    raw = json.loads((FIXTURES / name).read_text())
    ids = [i for match in raw["matches"].values() for i in match["event_ids"]]
    report = verify(spark, FIXTURES / name, _lake(spark, tmp_path, ids))
    assert report["manifest_version"] == raw["manifest_version"]
    assert report["expected_unique_ids"] == len(ids) == raw["totals"]["events_delivered"]
    assert (report["missing_ids"], report["duplicate_rows"]) == (0, 0)
    assert evaluate(report, fail_on_duplicates=True)[0] == "PASS"


def test_main_refuses_a_v4_manifest_without_its_bad_event_list(tmp_path: Path) -> None:
    """Bad events were sent but not listed (the cap): R1, R2, R10 and R11 cannot be
    judged, so verify refuses (exit 2) before starting a JVM."""
    m = json.loads(Path("tests/fixtures/bad_run.json").read_text(encoding="utf-8"))["manifest"]
    m["bad_data"].update(bad_events=None, bad_events_omitted_reason="too_many_bad_events")
    path = tmp_path / "run.json"
    path.write_text(json.dumps(m), encoding="utf-8")
    assert main(["--manifest", str(path)]) == 2
