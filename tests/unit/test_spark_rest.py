"""Unit tests for common.spark_rest (metric parsing, no Spark, no network)."""

from __future__ import annotations

import pytest

from common import spark_rest
from common.spark_rest import scan_metrics


def _node(name: str, **metrics: str) -> dict:
    return {"nodeName": name, "metrics": [{"name": k.replace("_", " "), "value": v}
                                          for k, v in metrics.items()]}


def test_scan_metrics_sums_parquet_scans_only() -> None:
    ex = {"nodes": [
        _node("Scan parquet", number_of_files_read="2", number_of_partitions_read="2"),
        _node("Scan json", number_of_files_read="9", number_of_partitions_read="9"),
        _node("LocalTableScan"),
    ]}
    assert scan_metrics(ex) == (2, 2)
    assert scan_metrics({"nodes": [_node("Scan parquet", number_of_files_read="1,204")]}) == (
        1204, None)
    assert scan_metrics({"nodes": [_node("Scan ExistingRDD")]}) is None


def test_newest_merge_scan_skips_snapshot_and_old_executions(
        monkeypatch: pytest.MonkeyPatch) -> None:
    listing = [{"id": 3, "description": "MERGE operation - writing new files"},
               {"id": 7, "description": "Delta: MERGE operation: Compute snapshot for version"},
               {"id": 8, "description": "MERGE operation - writing new files for only inserts"},
               {"id": 9, "description": "count at x.py"}]
    details = {8: {"nodes": [_node("Scan parquet", number_of_files_read="2",
                                   number_of_partitions_read="2")]},
               3: {"nodes": [_node("Scan parquet", number_of_files_read="10",
                                   number_of_partitions_read="10")]}}

    def fake(url: str, timeout: float = 5.0) -> object:
        if url.endswith("/applications"):
            return [{"id": "app-1"}]
        if "/sql?" in url:
            return listing
        return details[int(url.rsplit("/", 1)[1].split("?")[0])]

    monkeypatch.setattr(spark_rest, "get_json", fake)
    scan = spark_rest.newest_merge_scan("http://ui")
    assert scan is not None and (scan.execution_id, scan.files_read) == (8, 2)
    assert spark_rest.newest_merge_scan("http://ui", after_id=8) is None
    assert spark_rest.newest_execution_id("http://ui") == 9
