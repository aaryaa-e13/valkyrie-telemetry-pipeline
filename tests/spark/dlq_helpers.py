"""Shared builders for the Phase 3 slice 1 DLQ tests (test_dlq*.py); not a test file.

Split out of test_dlq.py so its tests can run in three JVMs (D3.5, test memory line).
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from streaming import dlq  # noqa: E402
from streaming import oracle_dlq as od  # noqa: E402
from streaming.bronze import bronze_schema, ensure_bronze_table, to_raw_rows  # noqa: E402
from streaming.config import SilverJobConfig  # noqa: E402
from streaming.silver import (  # noqa: E402
    ensure_silver_tables,
)
from tests.gates_corpus import DROP, T0, ev, raw  # noqa: E402,F401 (re-exported)

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)
TOPIC = "game-events"
fast = pytest.mark.spark_fast
# Control fields that legitimately differ per attempt (D2.4 C2 + Phase 3 plan 2.5).
ATTEMPT_FIELDS = {
    "inserted_this_attempt", "late_inserted_this_attempt", "merge_ms", "late_ms",
    "wm_read_ms", "processed_at", "dlq_keyless_inserted_this_attempt",
    "dlq_keyless_already_present", "dlq_keyed_inserted_this_attempt",
    "dlq_keyed_already_present", "late_already_present", "ontime_already_present",
    "dlq_ms", "d_open_read_ms", "cut_ms"}


Msg = tuple[int, "dict[str, Any] | str"]          # (offset, event dict or raw text)


def bronze(spark: SparkSession, msgs: list[Msg], kafka_delay_s: int = 1) -> DataFrame:
    """Bronze rows (to_raw_rows) for these messages, partition 0; Kafka time = event_time
    + kafka_delay_s when the event has a parseable time, else T0."""
    data = []
    for off, m in msgs:
        when = T0
        if isinstance(m, dict) and isinstance(m.get("event_time"), str):
            with contextlib.suppress(ValueError):
                when = datetime.strptime(m["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                    tzinfo=UTC)
        data.append((b"m-1", raw(m), TOPIC, 0, off, when + timedelta(seconds=kafka_delay_s)))
    return to_raw_rows(spark.createDataFrame(data, KAFKA_SCHEMA))


def frozen(spark: SparkSession, msgs: list[Msg], path: Path) -> DataFrame:
    """Written once and read back: every read is the same data (a batch re-run)."""
    if not path.exists():
        bronze(spark, msgs).write.format("delta").save(str(path))
    return spark.read.format("delta").load(str(path))



# --- lake helpers -----------------------------------------------------------------------

def _cfg(tmp_path: Path, **kw: Any) -> SilverJobConfig:
    return SilverJobConfig(
        bronze_path=str(tmp_path / "bronze"), table_path=str(tmp_path / "silver"),
        control_path=str(tmp_path / "_control" / "silver_batches"),
        checkpoint_path=str(tmp_path / "cp"), late_path=str(tmp_path / "silver_late"),
        status_path="", **kw)


def _ready(spark: SparkSession, tmp_path: Path, tail_offsets: Any, n_tail: int = 10,
           **kw: Any) -> SilverJobConfig:
    cfg = _cfg(tmp_path, **kw)
    ensure_bronze_table(spark, cfg.bronze_path,
                        bronze_schema(spark.createDataFrame([], KAFKA_SCHEMA)))
    ensure_silver_tables(spark, cfg)
    tail_offsets(cfg.checkpoint_path, n_tail)
    return cfg


def _load(spark: SparkSession, path: str) -> list[dict[str, Any]]:
    return [x.asDict() for x in spark.read.format("delta").load(path).collect()]


def _ids(spark: SparkSession, path: str) -> set[str]:
    return {x["event_id"] for x in _load(spark, path)}


def _dlq(spark: SparkSession, cfg: SilverJobConfig) -> dict[str, dict[str, Any]]:
    rows = _load(spark, cfg.dlq_path)
    out = {x["dlq_key"]: x for x in rows}
    assert len(out) == len(rows), "a dlq_key twice"
    return out


def _check_r8(row: dict[str, Any]) -> None:
    assert od.r8_row_problems(row) == [], row


def _check_disjoint(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """R9's disjointness over whole tables: S, L, D_open pairwise disjoint."""
    s, lt = _ids(spark, cfg.table_path), _ids(spark, cfg.late_path)
    d_open = {x.event_id for x in
              dlq.open_keyed_ids(spark, cfg.dlq_path, cfg.resolutions_path).collect()}
    assert s & lt == set() and s & d_open == set() and lt & d_open == set(), (s, lt, d_open)


def off_key(offset: int) -> str:
    return f"off:{TOPIC}/0/{offset}"
