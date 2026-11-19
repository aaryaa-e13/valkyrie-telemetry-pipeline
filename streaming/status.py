"""Status file for the bronze streaming job (standard library only).

After every completed micro-batch the job's progress listener rewrites a small
JSON file (atomically) describing the last committed batch and a short batch
history. Test tooling polls this file to decide whether Spark has processed a
given run, instead of parsing Spark's internal checkpoint format.

Why this is a safe "batch is committed" signal (verified for Spark 4.1.4, see
docs/DECISIONS.md D1.15 and D2.1 C12): Spark writes the sink commit (a Delta
commit; `_spark_metadata/N` in Phase 1), then the checkpoint commit
(commits/N), and only then posts the progress event that triggers this write.

Offsets follow Kafka/Spark semantics: a batch's `end_offset` for a partition is
EXCLUSIVE, i.e. the next offset to read. Offset N has been processed iff
committed end_offset > N.

File format (status_version 1):
{
  "status_version": 1,
  "query_name": "bronze",
  "query_id": "...",            # stable across restarts (stored in the checkpoint)
  "updated_at": "...Z",
  "last_batch": {batch record},
  "history": [{batch record}, ...]   # oldest first, at most HISTORY_LIMIT
}
batch record:
{
  "batch_id": 3, "run_id": "...",   # run_id changes on every restart
  "num_input_rows": 13, "timestamp": "...Z", "trigger_ms": 812,
  "start_offset": {"game-events": {"0": 7, ...}} | null,
  "end_offset":   {"game-events": {"0": 13, ...}}
}
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STATUS_VERSION = 1
HISTORY_LIMIT = 100


def parse_offsets(raw: str | Mapping[str, Any] | None) -> dict[str, dict[str, int]] | None:
    """Normalise Spark Kafka source offsets to {topic: {partition_str: offset}}.

    Spark hands offsets to Python as a JSON string; the first batch of a new
    query has no start offset ("None"/"null"/None). Partition keys stay strings
    so the structure round-trips through JSON unchanged.

    Raises:
        ValueError: The offsets are not in the expected shape.
    """
    if raw is None or (isinstance(raw, str) and raw in {"None", "null", ""}):
        return None
    data = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(data, Mapping):
        raise ValueError(f"offsets must be a JSON object, got {type(data).__name__}")
    out: dict[str, dict[str, int]] = {}
    for topic, partitions in data.items():
        if not isinstance(partitions, Mapping):
            raise ValueError(f"offsets for topic {topic!r} must be an object")
        out[str(topic)] = {str(p): int(o) for p, o in partitions.items()}
    return out


def batch_record(
    *,
    batch_id: int,
    run_id: str,
    num_input_rows: int,
    timestamp: str,
    trigger_ms: int | None,
    start_offset: str | Mapping[str, Any] | None,
    end_offset: str | Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build one history entry from progress values."""
    return {
        "batch_id": int(batch_id),
        "run_id": str(run_id),
        "num_input_rows": int(num_input_rows),
        "timestamp": timestamp,
        "trigger_ms": trigger_ms,
        "start_offset": parse_offsets(start_offset),
        "end_offset": parse_offsets(end_offset),
    }


def update_status(
    previous: Mapping[str, Any] | None,
    record: Mapping[str, Any],
    *,
    query_name: str,
    query_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a new status with `record` applied (pure function).

    * A record for a batch_id already in history replaces that entry, so a
      repeated progress event for the same batch never creates a second entry.
    * If the stored status belongs to a different query_id (checkpoint was
      reset), history starts fresh: old offsets would be misleading.
    * History is kept sorted by batch_id and capped at HISTORY_LIMIT.
    * `last_batch` is always the highest batch_id seen.
    """
    history: list[dict[str, Any]] = []
    if previous and previous.get("query_id") == query_id:
        history = [dict(h) for h in previous.get("history", [])]
    history = [h for h in history if h.get("batch_id") != record["batch_id"]]
    history.append(dict(record))
    history.sort(key=lambda h: h["batch_id"])
    history = history[-HISTORY_LIMIT:]
    stamp = (now or datetime.now(UTC)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return {
        "status_version": STATUS_VERSION,
        "query_name": query_name,
        "query_id": query_id,
        "updated_at": stamp,
        "last_batch": history[-1],
        "history": history,
    }


def write_status(status: Mapping[str, Any], path: str | Path) -> None:
    """Write the status atomically (temp file in the same folder + rename).

    Readers therefore see either the old or the new file, never a partial one.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(status, fh, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_name, 0o644)
        os.replace(tmp_name, target)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def read_status(path: str | Path) -> dict[str, Any] | None:
    """Read the status file. Returns None if it is missing, unreadable or invalid.

    Callers treat None as "no committed batch known yet" (keep waiting), never
    as success.
    """
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("status_version") != STATUS_VERSION:
        return None
    if not isinstance(data.get("last_batch"), dict):
        return None
    return data
