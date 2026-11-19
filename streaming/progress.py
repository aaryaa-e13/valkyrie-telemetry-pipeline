"""Streaming query listener shared by bronze and silver: progress logs + status files.

One listener sees the events of every query in the SparkSession, so it routes
by query name: each name has its own status file (status v2, streaming/status.py),
and every log line carries `"query": <name>`.

Spark posts a progress event only after the batch's sink commit and the
checkpoint commit (commits/N) are written, so a status file never claims a
batch that is not durable (D1.15; checked for the Delta sink in D2.1 C12). For
silver the sink is foreachBatch: the event comes after process_batch returned,
i.e. after the MERGE and the control row.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

from streaming.status import batch_record, read_status, source_kind, update_status, write_status

log = logging.getLogger("streaming.progress")


def decode_offsets(raw: str | None) -> object:
    """Source offsets arrive as a JSON string; decode for readable logs."""
    if raw is None or raw in {"None", "null"}:  # first batch has no start offset
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


class ProgressLogger(StreamingQueryListener):
    """Per completed micro-batch: one structured log line and a status-file update."""

    def __init__(self, status_paths: Mapping[str, str | None]) -> None:
        """status_paths: query name -> status file path ("" or None: no file)."""
        super().__init__()
        self._paths = {name: path for name, path in status_paths.items() if path}
        # Keep history across restarts: start from what is on disk.
        self._status: dict[str, dict[str, Any] | None] = {
            name: read_status(path) for name, path in self._paths.items()}

    def onQueryStarted(self, event) -> None:  # noqa: N802 (Spark API name)
        log.info("query started", extra={"fields": {
            "query_id": str(event.id), "run_id": str(event.runId), "name": event.name,
            "query": event.name}})

    def onQueryProgress(self, event) -> None:  # noqa: N802
        p = event.progress
        source = p.sources[0] if p.sources else None
        fields = {
            "query": p.name,
            "batch_id": p.batchId,
            "num_input_rows": p.numInputRows,
            "processed_rows_per_s": round(p.processedRowsPerSecond or 0.0, 1),
            "trigger_ms": p.durationMs.get("triggerExecution"),
            "add_batch_ms": p.durationMs.get("addBatch"),
            "start_offset": decode_offsets(source.startOffset) if source else None,
            "end_offset": decode_offsets(source.endOffset) if source else None,
        }
        level = logging.INFO if p.numInputRows > 0 else logging.DEBUG
        log.log(level, "batch progress", extra={"fields": fields})
        if p.name in self._paths and source is not None:
            self._write_status(p, source)

    def _write_status(self, p, source) -> None:
        """Update the query's status file. Never raises: status is diagnostics, not data."""
        path = self._paths[p.name]
        try:
            kind = source_kind(source.endOffset) or "kafka"
            record = batch_record(
                batch_id=p.batchId, run_id=str(p.runId), num_input_rows=p.numInputRows,
                timestamp=p.timestamp, trigger_ms=p.durationMs.get("triggerExecution"),
                start_offset=source.startOffset, end_offset=source.endOffset, source=kind,
            )
            self._status[p.name] = update_status(
                self._status.get(p.name), record, query_name=p.name, query_id=str(p.id),
                source=kind)
            write_status(self._status[p.name], path)
        except Exception as exc:  # noqa: BLE001 - must not kill the streaming query
            log.warning("could not write status file", extra={"fields": {
                "query": p.name, "path": path, "error": str(exc)}})

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        if event.exception:
            log.error("query failed", extra={"fields": {
                "query_id": str(event.id), "error": event.exception}})
        else:
            log.info("query stopped", extra={"fields": {"query_id": str(event.id)}})
