"""Phase 3 dead-letter queue (DLQ): table schemas, keys and the open set (plan section 2).

Three append-only Delta tables (`delta.appendOnly=true`, like bronze, silver and late):

* dead letter (`SILVER_DLQ_PATH`, default `<silver>/dead_letter`): one row per rejected
  bronze row decision. `dlq_key` is never NULL:
    - `id:<event_id>`                        keyed rows (key_ok true; hard rules H4-H10)
    - `off:<topic>/<partition>/<offset>`     keyless rows (no usable id or time)
  It is the MERGE key, so a NULL never reaches an ON clause (the D2.3 NULL hazard).
* resolutions (`_control/dlq_resolutions`): one row per DLQ entry whose decision was
  changed (replay, slice 3; a later valid copy, Phase 4 D3.1). Insert-if-absent on
  `dlq_key`, so an entry resolves at most once.
* replays (`_control/replays`): the replay audit trail (slice 3). Created now so the
  schema is fixed before any writer exists.

D_open = keyed DLQ rows without a resolution. Only keyed entries block an id: a keyless
entry with an event_id (H3, bad time) is keyed by offset and never blocks (addendum A.3).

This module imports nothing from streaming.silver (silver imports it).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

KEY_KIND_ID, KEY_KIND_OFFSET = "event_id", "offset"

DLQ_SCHEMA = StructType([
    StructField("dlq_key", StringType(), True),
    StructField("key_kind", StringType(), True),
    StructField("reason_code", StringType(), True),
    StructField("reasons", ArrayType(StringType()), True),
    StructField("rules_version", IntegerType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("match_id", StringType(), True),
    StructField("match_seq", IntegerType(), True),
    StructField("raw_value", StringType(), True),
    StructField("kafka_key", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("bronze_ingest_time", TimestampType(), True),
    StructField("bronze_ingest_date", DateType(), True),
    StructField("silver_query_id", StringType(), True),
    StructField("silver_batch_id", LongType(), True),
    StructField("dlq_inserted_at", TimestampType(), True),
    StructField("watermark_used", TimestampType(), True),
    StructField("in_initial_snapshot", BooleanType(), True),
])
DLQ_COLUMNS: tuple[str, ...] = tuple(f.name for f in DLQ_SCHEMA.fields)

RESOLUTIONS_SCHEMA = StructType([
    StructField("dlq_key", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("resolved_to", StringType(), True),        # silver | late
    StructField("resolved_by", StringType(), True),        # replay | valid_copy (Phase 4)
    StructField("replay_id", StringType(), True),
    StructField("rules_version_used", IntegerType(), True),
    StructField("resolved_query_id", StringType(), True),
    StructField("resolved_batch_id", LongType(), True),
    StructField("resolved_at", TimestampType(), True),
])

REPLAYS_SCHEMA = StructType([
    StructField("replay_id", StringType(), True),
    StructField("phase", StringType(), True),              # started | completed
    StructField("target_rules_version", IntegerType(), True),
    StructField("selection", StringType(), True),          # JSON: reasons, versions
    StructField("counts", MapType(StringType(), LongType()), True),
    StructField("silver_checkpoint_committed", LongType(), True),
    StructField("silver_version", LongType(), True),       # the cut (addendum B.3)
    StructField("late_version", LongType(), True),
    StructField("dlq_version", LongType(), True),
    StructField("resolutions_version", LongType(), True),
    StructField("recorded_at", TimestampType(), True),
])

DLQ_VIEW = "valkyrie_dlq_batch"


class DlqWriteError(RuntimeError):
    """An insert-if-absent MERGE inserted a different number of rows than planned.

    The single writer (D2.4) anti-joins the target first, so the MERGE must insert
    exactly the planned rows. Anything else means a second writer or a bug.
    """


def dlq_key(keyed: Column) -> Column:
    """`id:<event_id>` for keyed rows, else `off:<topic>/<partition>/<offset>`."""
    off = F.concat_ws("/", F.col("kafka_topic"), F.col("kafka_partition").cast("string"),
                      F.col("kafka_offset").cast("string"))
    return F.when(keyed, F.concat(F.lit("id:"), F.col("event_id"))).otherwise(
        F.concat(F.lit("off:"), off))


def open_keyed_ids(spark: SparkSession, dlq_path: str, resolutions_path: str) -> DataFrame:
    """D_open's event ids: keyed DLQ rows anti-joined with resolutions on dlq_key."""
    dlq = (spark.read.format("delta").load(dlq_path)
           .where(F.col("key_kind") == F.lit(KEY_KIND_ID)).select("dlq_key", "event_id"))
    res = spark.read.format("delta").load(resolutions_path).select("dlq_key")
    return dlq.join(res, "dlq_key", "left_anti").select("event_id").distinct()


def insert_if_absent(spark: SparkSession, path: str, rows: DataFrame, columns: tuple[str, ...],
                     key: str, view: str, expected: int) -> int:
    """MERGE `rows` into `path` ON t.<key> = s.<key>, inserting only new keys.

    `rows` must already exclude keys present in the target (the caller anti-joined),
    so the MERGE must insert exactly `expected` rows; otherwise DlqWriteError.
    `expected` == 0: no MERGE, no commit. Returns the number inserted.
    """
    if expected == 0:
        return 0
    rows.select(*columns).createOrReplaceTempView(view)
    cols = ", ".join(f"`{c}`" for c in columns)
    vals = ", ".join(f"s.`{c}`" for c in columns)
    r = spark.sql(f"MERGE INTO delta.`{path}` t USING {view} s ON t.`{key}` = s.`{key}` "
                  f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})").first()
    inserted = int(r["num_inserted_rows"]) if r is not None else 0
    if inserted != expected:
        raise DlqWriteError(f"MERGE into {path} inserted {inserted} rows, planned {expected} "
                            "(rows absent before the MERGE): a second writer?")
    return inserted
