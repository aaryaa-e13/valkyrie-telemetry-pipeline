"""Explicit Spark schemas for VALKYRIE events.

We never let Spark infer the schema of streaming JSON: inference would need to
scan data up front, and a single odd message could silently change column
types. The field list must match `simulator.events.GameEvent.to_dict()`
(enforced by tests/spark/test_raw_parse.py).

`event_time` is parsed as a STRING here and converted with `try_to_timestamp`
in the job, so a malformed timestamp becomes NULL instead of failing the
whole micro-batch (Spark 4 runs with ANSI mode on by default).
"""

from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# JSON payload as produced by the simulator (schema_version 1).
EVENT_JSON_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("event_time", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("match_seq", IntegerType(), True),
    StructField("patch_version", StringType(), True),
    StructField("region", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("champion_id", StringType(), True),
    StructField("team", StringType(), True),
    StructField("victim_id", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("gold", LongType(), True),
    StructField("winning_team", StringType(), True),
    StructField("duration_s", IntegerType(), True),
])

# Envelope fields that must be present for a row to count as parsed.
REQUIRED_FIELDS: tuple[str, ...] = (
    "event_id", "event_type", "schema_version", "event_time", "match_id", "match_seq",
)

# ISO-8601 UTC with milliseconds and 'Z', e.g. 2026-11-14T12:00:00.123Z
EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
