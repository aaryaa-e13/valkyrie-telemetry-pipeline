"""Read Spark SQL execution metrics from the monitoring REST API (standard library only).

Used as the evidence for MERGE partition pruning (DECISIONS D2.3). Insert-only
MERGE reports no files-skipped metric in Delta's operationMetrics (slice 1), so
we read Spark's own scan metrics instead: the target table's `Scan parquet` node
in the MERGE execution has `number of files read` and `number of partitions
read` (VERIFIED in the slice 3 spike: 2 of 10 partitions with the date bound,
10 of 10 without).

Endpoints (public, documented Spark monitoring API):
    GET <ui>/api/v1/applications                         -> [{"id": ...}, ...]
    GET <ui>/api/v1/applications/<app>/sql?length=N       -> executions, no details
    GET <ui>/api/v1/applications/<app>/sql/<id>?details=true

Limits: only "Scan parquet" nodes are counted, so the MERGE source must not be a
Parquet scan (a persisted micro-batch or a local DataFrame is not). Executions
that rebuild the Delta log snapshot are skipped (their Parquet scans read
checkpoint files, not data).
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from typing import Any

SCAN_NODE = "Scan parquet"
FILES_READ = "number of files read"
PARTITIONS_READ = "number of partitions read"


class SparkRestError(Exception):
    """The REST API is unreachable or returned something unexpected."""


def get_json(url: str, timeout: float = 5.0) -> Any:
    """GET a JSON document."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 (local UI)
            return json.load(resp)
    except (OSError, ValueError) as exc:
        raise SparkRestError(f"{url}: {exc}") from exc


def application_id(ui_url: str) -> str:
    """Id of the (only) application served by this UI."""
    apps = get_json(f"{ui_url}/api/v1/applications")
    if not apps:
        raise SparkRestError(f"{ui_url}: no application")
    return str(apps[0]["id"])


def _metric_int(value: str) -> int:
    return int(str(value).replace(",", "").split()[0])


@dataclass(frozen=True)
class MergeScan:
    """Target-scan metrics of one MERGE execution."""

    execution_id: int
    description: str
    files_read: int
    partitions_read: int | None

    def as_dict(self) -> dict[str, Any]:
        return {"execution_id": self.execution_id, "files_read": self.files_read,
                "partitions_read": self.partitions_read}


def scan_metrics(execution: dict[str, Any]) -> tuple[int, int | None] | None:
    """(files read, partitions read) summed over the execution's Parquet scans, or None."""
    files, parts, seen = 0, 0, False
    have_parts = True
    for node in execution.get("nodes", []):
        if not str(node.get("nodeName", "")).startswith(SCAN_NODE):
            continue
        metrics = {m["name"]: m["value"] for m in node.get("metrics", [])}
        if FILES_READ not in metrics:
            continue
        seen = True
        files += _metric_int(metrics[FILES_READ])
        if PARTITIONS_READ in metrics:
            parts += _metric_int(metrics[PARTITIONS_READ])
        else:
            have_parts = False
    if not seen:
        return None
    return files, (parts if have_parts else None)


def newest_merge_scan(ui_url: str, after_id: int = -1, app: str | None = None) -> MergeScan | None:
    """Newest MERGE execution with id > after_id that scanned Parquet, or None."""
    app = app or application_id(ui_url)
    base = f"{ui_url}/api/v1/applications/{app}/sql"
    executions = get_json(f"{base}?details=false&length=100000")
    candidates = sorted(
        (e for e in executions
         if int(e["id"]) > after_id and "MERGE" in str(e.get("description", ""))
         and "snapshot" not in str(e.get("description", "")).lower()),
        key=lambda e: int(e["id"]), reverse=True)
    for e in candidates:
        detail = get_json(f"{base}/{e['id']}?details=true")
        found = scan_metrics(detail)
        if found is not None:
            return MergeScan(int(e["id"]), str(e.get("description", ""))[:120], *found)
    return None


def newest_execution_id(ui_url: str, app: str | None = None) -> int:
    """Highest SQL execution id so far (-1 if none): take it before a MERGE."""
    app = app or application_id(ui_url)
    executions = get_json(f"{ui_url}/api/v1/applications/{app}/sql?details=false&length=100000")
    return max((int(e["id"]) for e in executions), default=-1)
