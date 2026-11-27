"""Shared builders for the silver Spark tests (test_silver*.py); not a test file.

Split out of test_silver.py so its tests can run in two JVMs (D3.5, test memory line).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from simulator.events import EventType, GameEvent  # noqa: E402
from streaming.bronze import bronze_schema, ensure_bronze_table, to_raw_rows  # noqa: E402
from streaming.config import SilverJobConfig  # noqa: E402
from streaming.silver import (  # noqa: E402
    ensure_silver_tables,
    prepare_silver,
    start_silver_query,
)

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)
T0 = datetime(2026, 11, 19, 12, 0, 0, tzinfo=UTC)
fast = pytest.mark.spark_fast


# --- helpers ----------------------------------------------------------------------------

def _value(event_id: str, when: datetime, seq: int = 1) -> bytes:
    return GameEvent(
        event_id=event_id, event_type=EventType.ITEM_PURCHASE, event_time=when,
        match_id="m-1", match_seq=seq, patch_version="14.3", region="EUW",
        player_id="p_000001", champion_id="ch_quillon", item_id="it_stormglass", gold=100,
    ).to_json_bytes()


def _bronze_rows(spark: SparkSession, spec: list[tuple[int, str | None, datetime]],
                 partition: int = 0) -> DataFrame:
    """Bronze-shaped rows. spec: (offset, event_id or None = malformed message, event_time)."""
    rows = [(b"m-1", _value(eid, ts, i + 1) if eid else b"{not json", "game-events",
             partition, off, datetime(2026, 11, 19, 12, 0, 1))
            for i, (off, eid, ts) in enumerate(spec)]
    return to_raw_rows(spark.createDataFrame(rows, KAFKA_SCHEMA))


def _cfg(tmp_path: Path, **kw: Any) -> SilverJobConfig:
    return SilverJobConfig(
        bronze_path=str(tmp_path / "bronze"), table_path=str(tmp_path / "silver"),
        control_path=str(tmp_path / "control"), checkpoint_path=str(tmp_path / "cp_silver"),
        late_path=str(tmp_path / "late"), status_path="", **kw)


def _bronze(spark: SparkSession, cfg: SilverJobConfig,
            spec: list[tuple[int, str | None, datetime]], partition: int = 0) -> None:
    """Create the bronze table if needed and append one commit of rows."""
    empty = spark.createDataFrame([], KAFKA_SCHEMA)
    ensure_bronze_table(spark, cfg.bronze_path, bronze_schema(empty))
    if spec:
        _bronze_rows(spark, spec, partition).coalesce(1).write.format("delta") \
            .mode("append").save(cfg.bronze_path)


def _ready(spark: SparkSession, tmp_path: Path, **kw: Any) -> SilverJobConfig:
    cfg = _cfg(tmp_path, **kw)
    _bronze(spark, cfg, [])
    ensure_silver_tables(spark, cfg)
    return cfg


def _silver(spark: SparkSession, cfg: SilverJobConfig) -> DataFrame:
    return spark.read.format("delta").load(cfg.table_path)


def _control(spark: SparkSession, cfg: SilverJobConfig) -> list[dict[str, Any]]:
    return [r.asDict() for r in spark.read.format("delta").load(cfg.control_path)
            .orderBy("silver_query_id", "batch_id").collect()]


def _run(spark: SparkSession, cfg: SilverJobConfig) -> str:
    """One availableNow run of the real silver query. Returns the query id."""
    assert prepare_silver(spark, cfg) in {"fresh", "resume", "rebuild-from-bronze"}
    q = start_silver_query(spark, cfg, trigger={"availableNow": True})
    q.awaitTermination(180)
    assert q.exception() is None, q.exception()
    return str(q.id)


def _committed_batches(cfg: SilverJobConfig) -> list[int]:
    commits = Path(cfg.checkpoint_path) / "commits"
    return sorted(int(f.name) for f in commits.iterdir() if f.name.isdigit())


def _seed_like(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """Two bronze commits: 3 ids sent twice (second copy +64 s, later offsets), 1 keyless."""
    _bronze(spark, cfg, [(8, "s-1", T0), (9, "s-2", T0), (10, "s-3", T0), (11, None, T0)])
    _bronze(spark, cfg, [(14, "s-1", T0 + timedelta(seconds=64)),
                         (15, "s-2", T0 + timedelta(seconds=64)),
                         (16, "s-3", T0 + timedelta(seconds=64))])



def _manifest(tmp_path: Path, ids: list[str]) -> Path:
    import json

    path = tmp_path / "run.json"
    path.write_text(json.dumps({
        "manifest_version": 1, "run_id": "r1", "seed": 501, "topic": "game-events",
        "totals": {"matches": 1, "events_generated": len(ids), "events_delivered": len(ids),
                   "events_failed": 0},
        "matches": {"m-1": {"events_generated": len(ids), "events_delivered": len(ids),
                            "max_match_seq": len(ids), "partitions": [0], "event_ids": ids}},
        "failed_event_ids": []}))
    return path

