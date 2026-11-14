"""Structured (JSON-lines) logging shared by all VALKYRIE Python components.

Usage:
    from common.logging_setup import setup_logging
    import logging

    setup_logging("INFO", component="simulator")
    log = logging.getLogger(__name__)
    log.info("sent batch", extra={"fields": {"count": 10}})

Every log line is a single JSON object, so tools like `jq`, Loki or Elasticsearch
can parse it without regexes.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format log records as one JSON object per line."""

    def __init__(self, component: str | None = None) -> None:
        super().__init__()
        self._component = component

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if self._component:
            payload["component"] = self._component
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO", component: str | None = None) -> None:
    """Configure the root logger to emit JSON lines to stderr.

    Args:
        level: Log level name, e.g. "DEBUG" or "INFO".
        component: Optional component name added to every line.
    """
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter(component))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
