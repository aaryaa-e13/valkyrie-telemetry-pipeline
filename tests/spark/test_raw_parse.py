"""Tests for streaming.raw_to_parquet.to_raw_rows (runs in the Spark container).

Run with: make test-spark
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

pyspark = pytest.importorskip("pyspark")  # skip cleanly outside the Spark image

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from simulator.events import EventType, GameEvent  # noqa: E402
from streaming.raw_to_parquet import to_raw_rows  # noqa: E402
from streaming.schemas import EVENT_JSON_SCHEMA  # noqa: E402

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)
T = datetime(2026, 11, 14, 12, 0, 0, 123000, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]").appName("test_raw_parse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _event() -> GameEvent:
    return GameEvent(
        event_id="e-1", event_type=EventType.ITEM_PURCHASE, event_time=T, match_id="m-1",
        match_seq=4, patch_version="14.3", region="EUW", player_id="p_000001",
        champion_id="ch_quillon", item_id="it_stormglass", gold=1300,
    )


def _kafka_df(spark: SparkSession, values: list[bytes | None], key: bytes = b"m-1") -> DataFrame:
    rows = [
        (key, v, "game-events", 2, 100 + i, datetime(2026, 11, 14, 12, 0, 1))
        for i, v in enumerate(values)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def test_schema_matches_simulator_event_fields() -> None:
    assert [f.name for f in EVENT_JSON_SCHEMA.fields] == list(_event().to_dict())


def test_valid_event_is_parsed(spark: SparkSession) -> None:
    row = to_raw_rows(_kafka_df(spark, [_event().to_json_bytes()])).first()
    assert row.parse_ok is True
    assert row.event_id == "e-1"
    assert row.event_type == "item_purchase"
    assert row.match_seq == 4
    assert row.gold == 1300
    assert row.event_time.replace(tzinfo=UTC) == T  # Spark returns naive UTC (session tz)
    assert row.kafka_key == "m-1"
    assert (row.kafka_topic, row.kafka_partition, row.kafka_offset) == ("game-events", 2, 100)
    assert row.raw_value == _event().to_json_bytes().decode()
    assert row.ingest_date == row.ingest_time.date()


@pytest.mark.parametrize("value", [
    b"this is not json{",
    b'{"event_id": "e-2"}',                                     # required fields missing
    json.dumps({**_event().to_dict(), "match_seq": "abc"}).encode(),       # wrong type
    json.dumps({**_event().to_dict(), "event_time": "yesterday"}).encode(),  # bad timestamp
])
def test_bad_messages_are_kept_with_parse_ok_false(spark: SparkSession, value: bytes) -> None:
    row = to_raw_rows(_kafka_df(spark, [value])).first()  # must not raise (ANSI mode)
    assert row.parse_ok is False
    assert row.raw_value == value.decode()
    assert row.kafka_offset == 100


def test_null_value_tombstone_is_kept(spark: SparkSession) -> None:
    row = to_raw_rows(_kafka_df(spark, [None])).first()
    assert row.parse_ok is False
    assert row.raw_value is None


def test_one_bad_message_does_not_affect_good_ones(spark: SparkSession) -> None:
    out = to_raw_rows(_kafka_df(spark, [b"garbage", _event().to_json_bytes()])).collect()
    assert [r.parse_ok for r in sorted(out, key=lambda r: r.kafka_offset)] == [False, True]
