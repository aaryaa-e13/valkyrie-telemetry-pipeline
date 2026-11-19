"""Bronze ingestion job: Kafka `game-events` -> Delta table (partitioned by ingest_date).

What it guarantees (Phase 2 slice 2, DECISIONS D2.2):
    * No message is skipped: Kafka offsets are tracked in the checkpoint, and
      failOnDataLoss=true stops the job if retention deleted unread data.
    * A replayed micro-batch is never written twice: every Delta commit
      carries txn(appId = query id, version = batch id), and the sink skips a
      batch it already committed (D2.1, C8/C9). Verify rule R0 checks it:
      (topic, partition, offset) is unique in bronze.
    * Unparseable messages are kept (parse_ok = false, raw_value preserved).
    * The table is append-only (delta.appendOnly = true): Delta refuses any
      UPDATE/DELETE/MERGE that would remove rows.
    * Start-up guard, both directions: the table's newest txn version for the
      checkpoint's query id must match the checkpoint's last committed batch
      (or be one ahead after a crash). Otherwise exit 2 unless
      BRONZE_ALLOW_REBUILD=1 (streaming/start_guard.py, DECISIONS D2.2).

What it does NOT do (slice 3+): deduplicate events, handle late data with
watermarks, or MERGE. Duplicate events in Kafka produce duplicate rows here
(one row per Kafka message, D1.14).

Run (inside the Spark container):
    python -m streaming.bronze
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from functools import reduce
from types import FrameType
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructField, StructType

from common.delta_log import latest_txn_in_commits, newest_checkpoint_files
from common.logging_setup import setup_logging
from streaming.config import BronzeJobConfig
from streaming.progress import ProgressLogger
from streaming.schemas import EVENT_JSON_SCHEMA, EVENT_TIME_FORMAT, REQUIRED_FIELDS
from streaming.spark_session import build_spark
from streaming.start_guard import BronzeStartupError, decide, read_checkpoint_state

log = logging.getLogger("streaming.bronze")

PARTITION_COLUMN = "ingest_date"
APPEND_ONLY_PROPERTY = "delta.appendOnly"
EXIT_REFUSED = 2  # start-up refused (guard or table check); 1 = runtime failure


def to_raw_rows(kafka_df: DataFrame) -> DataFrame:
    """Turn Kafka records into raw event rows. Pure transformation (no I/O).

    Input columns (Kafka source): key, value, topic, partition, offset, timestamp.
    Output: parsed event fields, parse_ok, Kafka metadata, raw_value,
    ingest_time and ingest_date.

    Malformed JSON or bad field types never fail the batch: the affected
    fields become NULL and parse_ok is false. The raw message is always kept.
    """
    raw = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    parsed = raw.withColumn(
        "_e", F.from_json(F.col("raw_value"), EVENT_JSON_SCHEMA, {"mode": "PERMISSIVE"})
    )

    event_cols: list[Column] = []
    for field in EVENT_JSON_SCHEMA.fields:
        if field.name == "event_time":
            # try_to_timestamp returns NULL on bad input instead of raising (ANSI mode).
            event_cols.append(
                F.try_to_timestamp(F.col("_e.event_time"), F.lit(EVENT_TIME_FORMAT))
                .alias("event_time")
            )
        else:
            event_cols.append(F.col(f"_e.{field.name}").alias(field.name))

    with_events = parsed.select(
        *event_cols,
        "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "raw_value",
    )
    parse_ok = reduce(
        lambda acc, c: acc & c, [F.col(name).isNotNull() for name in REQUIRED_FIELDS]
    )
    ingest_time = F.current_timestamp()  # one value per micro-batch
    return (
        with_events.withColumn("parse_ok", F.coalesce(parse_ok, F.lit(False)))
        .withColumn("ingest_time", ingest_time)
        .withColumn("ingest_date", F.to_date(F.col("ingest_time")))
    )


def bronze_schema(source_df: DataFrame) -> StructType:
    """Schema of the bronze table: to_raw_rows' output with every column nullable.

    A DataFrame write would also create nullable columns. Explicit NOT NULL
    columns would make later schema changes (Phase 5) and replays stricter
    than Phase 1 for no gain: R0 and parse_ok already cover the key columns.
    """
    return StructType([StructField(f.name, f.dataType, True, f.metadata)
                       for f in to_raw_rows(source_df).schema.fields])


def table_has_data(spark: SparkSession, table_path: str) -> bool:
    """True if a Delta table exists at the path and has at least one row."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        return False
    return spark.read.format("delta").load(table_path).limit(1).count() > 0


def table_txn_version(spark: SparkSession, table_path: str, app_id: str) -> int | None:
    """Newest txn version the table holds for `app_id` (a query id), or None.

    Reads commit JSONs first (stdlib, newest first). Only if log cleanup has
    removed older commits and none of the remaining ones has the txn, it reads
    the newest Delta checkpoint Parquet with Spark (txn actions live there).
    """
    v, complete = latest_txn_in_commits(table_path, app_id)
    if v is not None or complete:
        return v
    files = newest_checkpoint_files(table_path)
    if not files:
        raise BronzeStartupError(f"Delta table {table_path}: commit files before the oldest "
                                 "remaining one are gone and there is no Delta checkpoint")
    rows = (spark.read.parquet(*[str(f) for f in files])
            .where(F.col("txn").isNotNull() & (F.col("txn.appId") == app_id))
            .agg(F.max("txn.version").alias("v")).first())
    return None if rows is None or rows["v"] is None else int(rows["v"])


def check_checkpoint_guard(spark: SparkSession, table_path: str, checkpoint_path: str,
                           allow_rebuild: bool) -> str:
    """Refuse to start when checkpoint and table disagree (both directions).

    One rule (streaming/start_guard.py): the table's newest txn version T for
    the checkpoint's query id must equal the checkpoint's newest committed batch
    C, or be C + 1 after a crash between the Delta commit and commits/C+1.
    Covers:
      * table has data, checkpoint missing/empty: a new checkpoint means a NEW
        query id, so Delta's txn cannot recognise the old batches and every
        message would be appended again;
      * checkpoint has batches, table missing or without a txn for this query
        id: resuming would skip every row before the checkpoint's offsets;
      * table behind (restored from an older copy) or ahead of the checkpoint.

    Returns:
        "fresh", "resume", or "rebuild" (refusal overridden by
        BRONZE_ALLOW_REBUILD=1, logged as a WARNING).

    Raises:
        BronzeStartupError: refused, and allow_rebuild is False.
    """
    state = read_checkpoint_state(checkpoint_path)
    txn = (table_txn_version(spark, table_path, state.query_id)
           if state.query_id and state.last_planned is not None else None)
    decision = decide(state, txn, lambda: table_has_data(spark, table_path))
    fields = {"table_path": table_path, "checkpoint_path": checkpoint_path,
              "query_id": state.query_id, "checkpoint_planned": state.last_planned,
              "checkpoint_committed": state.last_committed, "table_txn": txn}
    if decision.mode != "refuse":
        log.info("start-up state", extra={"fields": {**fields, "mode": decision.mode,
                                                     "note": decision.note}})
        return decision.mode
    message = (f"bronze table {table_path} and checkpoint {checkpoint_path} disagree: "
               f"{decision.problem}. Restore the matching checkpoint/table, or set "
               "BRONZE_ALLOW_REBUILD=1 if that is really intended (DECISIONS D2.2).")
    if not allow_rebuild:
        raise BronzeStartupError(message)
    log.warning("REBUILD: starting although checkpoint and table disagree",
                extra={"fields": {**fields, "problem": decision.problem}})
    return "rebuild"


def ensure_bronze_table(spark: SparkSession, table_path: str, schema: StructType) -> None:
    """Create the bronze table if absent; check partitioning and appendOnly either way.

    Creates only when no table exists, so a table whose schema has grown since
    (Phase 5: added nullable columns) is accepted as it is.

    Raises:
        BronzeStartupError: the existing table is not partitioned by
            ingest_date only, or is not append-only.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        (DeltaTable.createIfNotExists(spark).location(table_path).addColumns(schema)
         .partitionedBy(PARTITION_COLUMN).property(APPEND_ONLY_PROPERTY, "true").execute())
        log.info("bronze table created", extra={"fields": {"table_path": table_path}})
    detail = DeltaTable.forPath(spark, table_path).detail().first()
    problems = []
    if list(detail["partitionColumns"]) != [PARTITION_COLUMN]:
        problems.append(f"partition columns {list(detail['partitionColumns'])} "
                        f"(expected ['{PARTITION_COLUMN}'])")
    if (detail["properties"] or {}).get(APPEND_ONLY_PROPERTY) != "true":
        problems.append(f"{APPEND_ONLY_PROPERTY} is not 'true'")
    if problems:
        raise BronzeStartupError(f"bronze table {table_path}: " + "; ".join(problems))


def prepare_bronze(spark: SparkSession, cfg: BronzeJobConfig, source_df: DataFrame) -> str:
    """Start-up checks: the guard first (it must see the table as it was), then
    create/check the table. Returns the guard's mode. Raises BronzeStartupError."""
    mode = check_checkpoint_guard(spark, cfg.table_path, cfg.checkpoint_path,
                                  cfg.allow_rebuild)
    ensure_bronze_table(spark, cfg.table_path, bronze_schema(source_df))
    return mode


def start_bronze_query(source_df: DataFrame, cfg: BronzeJobConfig,
                       trigger: dict[str, Any] | None = None) -> StreamingQuery:
    """Start the append-only Delta streaming write of to_raw_rows(source_df).

    Args:
        source_df: Kafka-shaped streaming DataFrame (key, value, topic,
            partition, offset, timestamp).
        cfg: job settings (query name, table and checkpoint paths, trigger).
        trigger: Spark trigger kwargs; default processingTime=cfg.trigger_interval.
            Tests pass {"availableNow": True}.
    """
    writer = (
        to_raw_rows(source_df)
        .writeStream.queryName(cfg.query_name)
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint_path)
        .partitionBy(PARTITION_COLUMN)
    )
    writer = writer.trigger(**(trigger or {"processingTime": cfg.trigger_interval}))
    return writer.start(cfg.table_path)


def kafka_source(spark: SparkSession, cfg: BronzeJobConfig) -> DataFrame:
    """Lazy Kafka source: nothing connects to Kafka until the query starts."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
        .option("subscribe", cfg.topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .option("failOnDataLoss", str(cfg.fail_on_data_loss).lower())
        .load()
    )


def run(cfg: BronzeJobConfig, master: str, driver_memory: str) -> int:
    """Start the streaming query and block until it stops. Returns an exit code:
    0 stopped cleanly, 1 failed, 2 start-up refused (BronzeStartupError)."""
    spark = build_spark(cfg.query_name, master, driver_memory)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger({cfg.query_name: cfg.status_path}))
    log.info("starting", extra={"fields": {
        "spark_version": spark.version, "master": master, "driver_memory": driver_memory,
        "topic": cfg.topic, "table_path": cfg.table_path,
        "checkpoint_path": cfg.checkpoint_path, "status_path": cfg.status_path,
        "trigger": cfg.trigger_interval, "allow_rebuild": cfg.allow_rebuild,
        "max_offsets_per_trigger": cfg.max_offsets_per_trigger}})

    try:
        kafka_df = kafka_source(spark, cfg)
        mode = prepare_bronze(spark, cfg, kafka_df)
        log.info("bronze table ready", extra={"fields": {
            "mode": mode, "table_path": cfg.table_path}})
        query = start_bronze_query(kafka_df, cfg)
    except BronzeStartupError as exc:
        log.error("start-up refused", extra={"fields": {"error": str(exc)}})
        spark.stop()
        return EXIT_REFUSED
    except Exception as exc:  # e.g. unwritable output path, bad options
        # Py4J errors carry the full Java stack trace; log only the first line.
        log.error("failed to start query", extra={"fields": {
            "error_type": type(exc).__name__, "error": str(exc).strip().splitlines()[0][:500]}})
        spark.stop()
        return 1

    # Graceful shutdown: `docker stop` sends SIGTERM. We only set a flag here and
    # stop the query from the main loop (calling Spark inside a signal handler
    # could deadlock the Py4J connection).
    stop_requested = {"value": False}

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("stop requested", extra={"fields": {"signal": signum}})
        stop_requested["value"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    while query.isActive and not stop_requested["value"]:
        query.awaitTermination(timeout=2)

    if stop_requested["value"] and query.isActive:
        query.stop()  # an interrupted batch is re-run from the checkpoint on restart
    exc = query.exception()
    spark.stop()
    if exc is not None:
        log.error("query terminated with error", extra={"fields": {"error": str(exc)}})
        return 1
    return 0


def main() -> int:
    """Entry point: read config from env and run the job."""
    cfg = BronzeJobConfig.from_env()
    setup_logging(cfg.log_level, component="bronze")
    logging.getLogger("py4j").setLevel(logging.WARNING)  # Py4J gateway chatter
    master = os.environ.get("SPARK_MASTER", "local[4]")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    return run(cfg, master, driver_memory)


if __name__ == "__main__":
    sys.exit(main())
