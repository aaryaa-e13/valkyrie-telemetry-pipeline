"""Silver job: bronze Delta -> silver Delta, one row per event_id (DECISIONS D2.3).

A streaming query reads the bronze table (Delta source) and, per micro-batch
(`foreachBatch` -> process_batch):

    1. key check (D9): parse_ok AND event_id IS NOT NULL AND event_time IS NOT NULL.
       Rows without a usable key are excluded and counted per reason.
    2. in-batch dedupe: per event_id keep the copy with the lowest
       (kafka_topic, kafka_partition, kafka_offset). That key is unique in bronze
       (R0), so a replay keeps the same copy: the first send.
    3. MERGE bounds from keyed rows only (streaming/silver_merge.py): DATE
       literals, never NULL. No keyed rows: no MERGE.
    4. insert-only MERGE into silver:
       ON t.event_id = s.event_id AND t.event_date BETWEEN DATE'lo' AND DATE'hi'
       WHEN NOT MATCHED THEN INSERT *.
       A replayed batch finds every id and writes no commit.
    5. one control row per batch, EVERY batch (also zero-row and all-keyless
       ones), a plain append with txnAppId = silver query id, txnVersion = the
       batch id Spark passes in: Delta's documented pattern for idempotent
       writes in foreachBatch, so a replay writes nothing. The guard reads that
       txn. Silver rows do NOT use txn: they are insert-if-absent (step 4).

Rules for foreachBatch kept here (Delta docs, "Idempotent table writes in
foreachBatch"; Databricks guidance): no exception is caught or retried inside
the batch (only try/finally to unpersist), so a failure fails the batch and
Spark re-runs it after a restart; txnVersion is always the batch id.

Correctness argument (effectively-once, not "exactly-once"): silver is
insert-if-absent by event_id within the date window, so re-running a batch, or
re-reading all of bronze after a checkpoint reset, never adds a second row for
an id already there.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.bronze import table_has_data, table_txn_version
from streaming.config import SilverJobConfig
from streaming.schemas import EVENT_JSON_SCHEMA
from streaming.silver_merge import merge_bounds, merge_condition
from streaming.start_guard import BronzeStartupError, decide, read_checkpoint_state

log = logging.getLogger("streaming.silver")

PARTITION_COLUMN = "event_date"
APPEND_ONLY_PROPERTY = "delta.appendOnly"
ORDER_KEY = ("kafka_topic", "kafka_partition", "kafka_offset")
EVENT_COLUMNS = tuple(f.name for f in EVENT_JSON_SCHEMA.fields)
LINEAGE_COLUMNS = ("kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
                   "kafka_timestamp")
MERGE_VIEW = "valkyrie_silver_batch"

CONTROL_SCHEMA = StructType([
    StructField("silver_query_id", StringType(), True),
    StructField("batch_id", LongType(), True),
    StructField("rows_in", LongType(), True),
    StructField("excluded_unparsed", LongType(), True),
    StructField("excluded_null_event_id", LongType(), True),
    StructField("excluded_null_event_time", LongType(), True),
    StructField("excluded_total", LongType(), True),
    StructField("dup_in_batch", LongType(), True),
    StructField("keyed_unique", LongType(), True),
    StructField("merge_skipped", BooleanType(), True),
    StructField("merge_lo", DateType(), True),
    StructField("merge_hi", DateType(), True),
    StructField("merge_window_days", IntegerType(), True),
    StructField("inserted_this_attempt", LongType(), True),
    StructField("merge_ms", LongType(), True),
    StructField("processed_at", TimestampType(), True),
])


class SilverStartupError(BronzeStartupError):
    """Silver refused to start (exit 2). Subclass so one except covers both jobs."""


# --- pure transformations ---------------------------------------------------------


def key_ok() -> Column:
    """D9 key check. parse_ok already implies the other two today; they are checked
    explicitly so a future change to parse_ok cannot let a NULL key through."""
    return (F.coalesce(F.col("parse_ok"), F.lit(False))
            & F.col("event_id").isNotNull() & F.col("event_time").isNotNull())


@dataclass(frozen=True)
class BatchCounts:
    """One aggregation pass over a bronze micro-batch."""

    rows_in: int
    excluded_unparsed: int
    excluded_null_event_id: int
    excluded_null_event_time: int
    excluded_total: int
    keyed_rows: int
    keyed_unique: int
    min_date: date | None
    max_date: date | None

    @property
    def dup_in_batch(self) -> int:
        return self.keyed_rows - self.keyed_unique


def batch_counts(df: DataFrame) -> BatchCounts:
    """Counts for the control row and the MERGE bounds (min/max from keyed rows only).

    A row can hit several exclusion reasons; excluded_total counts it once.
    """
    k = key_ok()
    one = lambda cond: F.coalesce(F.sum(F.when(cond, 1).otherwise(0)), F.lit(0))  # noqa: E731
    keyed_date = F.when(k, F.to_date("event_time"))
    r = df.agg(
        F.count(F.lit(1)).alias("rows_in"),
        one(~F.coalesce(F.col("parse_ok"), F.lit(False))).alias("unparsed"),
        one(F.col("event_id").isNull()).alias("null_id"),
        one(F.col("event_time").isNull()).alias("null_time"),
        one(~k).alias("excluded"),
        one(k).alias("keyed"),
        F.count_distinct(F.when(k, F.col("event_id"))).alias("keyed_unique"),
        F.min(keyed_date).alias("min_date"),
        F.max(keyed_date).alias("max_date"),
    ).first()
    assert r is not None  # an aggregate without grouping always returns one row
    return BatchCounts(int(r["rows_in"]), int(r["unparsed"]), int(r["null_id"]),
                       int(r["null_time"]), int(r["excluded"]), int(r["keyed"]),
                       int(r["keyed_unique"]), r["min_date"], r["max_date"])


def dedupe_in_batch(df: DataFrame) -> DataFrame:
    """Keyed rows only, one per event_id: the lowest (topic, partition, offset)."""
    w = Window.partitionBy("event_id").orderBy(*ORDER_KEY)
    return (df.where(key_ok()).withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1).drop("_rn"))


def to_silver_rows(df: DataFrame, query_id: str, batch_id: int,
                   inserted_at: datetime) -> DataFrame:
    """Project bronze rows to the silver columns (no raw_value, no parse_ok: Q5)."""
    return df.select(
        *EVENT_COLUMNS,
        F.to_date("event_time").alias(PARTITION_COLUMN),
        *LINEAGE_COLUMNS,
        F.col("ingest_time").alias("bronze_ingest_time"),
        F.lit(query_id).alias("silver_query_id"),
        F.lit(batch_id).cast("long").alias("silver_batch_id"),
        F.lit(inserted_at).cast("timestamp").alias("silver_inserted_at"),
    )


def silver_schema(bronze_df: DataFrame) -> StructType:
    """Silver schema derived from bronze's, every column nullable (like bronze, D2.2)."""
    shape = to_silver_rows(bronze_df.limit(0), "q", 0, datetime(2026, 1, 1, tzinfo=UTC))
    return StructType([StructField(f.name, f.dataType, True, f.metadata)
                       for f in shape.schema.fields])


# --- tables -------------------------------------------------------------------------


def _ensure_table(spark: SparkSession, path: str, schema: StructType,
                  partition: str | None, what: str) -> None:
    """Create if absent (explicit schema, appendOnly); check appendOnly + partitioning."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        b = DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
        if partition:
            b = b.partitionedBy(partition)
        b.property(APPEND_ONLY_PROPERTY, "true").execute()
        log.info(f"{what} table created", extra={"fields": {"table_path": path}})
    detail = DeltaTable.forPath(spark, path).detail().first()
    expected = [partition] if partition else []
    problems = []
    if list(detail["partitionColumns"]) != expected:
        problems.append(f"partition columns {list(detail['partitionColumns'])} "
                        f"(expected {expected})")
    if (detail["properties"] or {}).get(APPEND_ONLY_PROPERTY) != "true":
        problems.append(f"{APPEND_ONLY_PROPERTY} is not 'true'")
    if problems:
        raise SilverStartupError(f"{what} table {path}: " + "; ".join(problems))


def ensure_silver_tables(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """Create/check the silver table (partitioned by event_date) and the control table."""
    bronze = spark.read.format("delta").load(cfg.bronze_path)
    _ensure_table(spark, cfg.table_path, silver_schema(bronze), PARTITION_COLUMN, "silver")
    _ensure_table(spark, cfg.control_path, CONTROL_SCHEMA, None, "silver control")


# --- start-up guard ------------------------------------------------------------------


def check_silver_guard(spark: SparkSession, cfg: SilverJobConfig) -> str:
    """Silver's start-up guard (DECISIONS D2.3).

    Same rule as bronze (streaming/start_guard.decide), with the CONTROL table's
    txn in the role of the Delta sink's txn: T = newest txn version the control
    table holds for the silver checkpoint's query id, C = the checkpoint's newest
    committed batch. Start iff T == C, or T == C + 1 (crash after the control row,
    before commits/C+1). The control row is written for every batch, so a normal
    restart always has T == C. Additionally: if the checkpoint has batches, the
    silver table must exist.

    NOT refused: a missing/reset silver checkpoint on a populated silver table.
    The new query re-reads bronze and every MERGE is insert-if-absent, so nothing
    is doubled; it is logged as a WARNING.

    Returns "fresh", "rebuild-from-bronze", "resume" or "rebuild" (override).
    Raises SilverStartupError when refused and SILVER_ALLOW_REBUILD is not set.
    """
    from delta.tables import DeltaTable

    state = read_checkpoint_state(cfg.checkpoint_path)
    fields: dict[str, Any] = {
        "query": cfg.query_name, "checkpoint_path": cfg.checkpoint_path,
        "query_id": state.query_id, "checkpoint_planned": state.last_planned,
        "checkpoint_committed": state.last_committed}
    if state.last_planned is None:
        if table_has_data(spark, cfg.table_path):
            log.warning("silver rebuild from bronze (idempotent): the checkpoint has no "
                        "batches but silver has rows; every MERGE is insert-if-absent",
                        extra={"fields": fields})
            return "rebuild-from-bronze"
        log.info("start-up state", extra={"fields": {**fields, "mode": "fresh"}})
        return "fresh"

    problem: str | None = None
    txn: int | None = None
    if not DeltaTable.isDeltaTable(spark, cfg.table_path):
        problem = (f"checkpoint (query {state.query_id}) has batches but the silver table "
                   f"{cfg.table_path} is missing")
    else:
        if state.query_id:
            txn = table_txn_version(spark, cfg.control_path, state.query_id)
        decision = decide(state, txn, lambda: table_has_data(spark, cfg.control_path))
        fields["control_txn"] = txn
        if decision.mode != "refuse":
            log.info("start-up state", extra={"fields": {**fields, "mode": decision.mode,
                                                         "note": decision.note}})
            return decision.mode
        problem = f"control table {cfg.control_path}: {decision.problem}"
    message = (f"silver checkpoint {cfg.checkpoint_path} and silver tables disagree: "
               f"{problem}. Restore the matching checkpoint/tables, or set "
               "SILVER_ALLOW_REBUILD=1 if that is really intended (DECISIONS D2.3).")
    if not cfg.allow_rebuild:
        raise SilverStartupError(message)
    log.warning("REBUILD: silver starting although checkpoint and tables disagree",
                extra={"fields": {**fields, "problem": problem}})
    return "rebuild"


def prepare_silver(spark: SparkSession, cfg: SilverJobConfig) -> str:
    """Guard first (it must see the tables as they were), then create/check tables."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, cfg.bronze_path):
        raise SilverStartupError(f"bronze table {cfg.bronze_path} does not exist")
    mode = check_silver_guard(spark, cfg)
    ensure_silver_tables(spark, cfg)
    return mode


# --- per-batch processing -------------------------------------------------------------


class BatchProcessor:
    """The foreachBatch function. Reads its query id from the checkpoint `metadata`
    file (written by Spark before the first batch, stable across restarts)."""

    def __init__(self, cfg: SilverJobConfig) -> None:
        self.cfg = cfg
        self._query_id: str | None = None

    @property
    def query_id(self) -> str:
        if self._query_id is None:
            qid = read_checkpoint_state(self.cfg.checkpoint_path).query_id
            if not qid:
                raise RuntimeError(f"no query id in {self.cfg.checkpoint_path}/metadata")
            self._query_id = qid
        return self._query_id

    def __call__(self, df: DataFrame, batch_id: int) -> None:
        process_batch(df, batch_id, self.cfg, self.query_id)


def process_batch(df: DataFrame, batch_id: int, cfg: SilverJobConfig,
                  query_id: str) -> dict[str, Any]:
    """Dedupe one bronze micro-batch into silver and write its control row.

    Returns the control row as a dict (also logged as "silver batch").
    Raises MergeBoundsError if keyed rows exist but a bound is missing: the
    batch fails, the query stops, nothing is written for this batch.
    """
    spark = df.sparkSession
    started = datetime.now(UTC)
    df = df.persist()
    try:
        c = batch_counts(df)
        lo = hi = None
        inserted = 0
        merge_ms = 0
        if c.keyed_rows > 0:
            lo, hi = merge_bounds(c.min_date, c.max_date, cfg.dedupe_horizon_days)
            src = to_silver_rows(dedupe_in_batch(df), query_id, batch_id, started)
            src.createOrReplaceTempView(MERGE_VIEW)
            t0 = time.monotonic()
            result = spark.sql(
                f"MERGE INTO delta.`{cfg.table_path}` t USING {MERGE_VIEW} s "
                f"ON {merge_condition(lo, hi)} WHEN NOT MATCHED THEN INSERT *").first()
            merge_ms = int((time.monotonic() - t0) * 1000)
            inserted = int(result["num_inserted_rows"]) if result is not None else 0
        row = {
            "silver_query_id": query_id, "batch_id": int(batch_id), "rows_in": c.rows_in,
            "excluded_unparsed": c.excluded_unparsed,
            "excluded_null_event_id": c.excluded_null_event_id,
            "excluded_null_event_time": c.excluded_null_event_time,
            "excluded_total": c.excluded_total, "dup_in_batch": c.dup_in_batch,
            "keyed_unique": c.keyed_unique, "merge_skipped": c.keyed_rows == 0,
            "merge_lo": lo, "merge_hi": hi,
            "merge_window_days": (hi - lo).days + 1 if lo and hi else None,
            "inserted_this_attempt": inserted, "merge_ms": merge_ms,
            "processed_at": started,
        }
        write_control_row(spark, cfg.control_path, row)
        log.info("silver batch", extra={"fields": {
            "query": cfg.query_name, **{k: (v.isoformat() if isinstance(v, date) else v)
                                        for k, v in row.items()}}})
        return row
    finally:
        df.unpersist()


def write_control_row(spark: SparkSession, control_path: str, row: dict[str, Any]) -> None:
    """Append the control row with txn(appId = query id, version = batch id).

    Delta skips the write if this txn version is already in the table, so a
    replayed batch never adds a second control row.
    """
    (spark.createDataFrame([row], CONTROL_SCHEMA).write.format("delta").mode("append")
     .option("txnAppId", row["silver_query_id"]).option("txnVersion", row["batch_id"])
     .save(control_path))


def start_silver_query(spark: SparkSession, cfg: SilverJobConfig,
                       trigger: dict[str, Any] | None = None) -> StreamingQuery:
    """Start the streaming read of bronze with process_batch as the sink."""
    Path(cfg.checkpoint_path).parent.mkdir(parents=True, exist_ok=True)
    writer = (spark.readStream.format("delta")
              .option("maxFilesPerTrigger", cfg.max_files_per_trigger)
              .load(cfg.bronze_path)
              .writeStream.queryName(cfg.query_name)
              .option("checkpointLocation", cfg.checkpoint_path)
              .foreachBatch(BatchProcessor(cfg)))
    writer = writer.trigger(**(trigger or {"processingTime": cfg.trigger_interval}))
    return writer.start()
