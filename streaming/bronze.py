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
    * Checkpoint-reset guard: if the table has data but the checkpoint has no
      batches, the job refuses to start (exit 2) unless BRONZE_ALLOW_REBUILD=1.
      A new checkpoint means a new query id, so Delta's txn cannot recognise
      the old batches and every message would be appended again.

What it does NOT do (slice 3+): deduplicate events, handle late data with
watermarks, or MERGE. Duplicate events in Kafka produce duplicate rows here
(one row per Kafka message, D1.14).

Run (inside the Spark container):
    python -m streaming.bronze
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
from functools import reduce
from pathlib import Path
from types import FrameType
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener
from pyspark.sql.types import StructField, StructType

from common.logging_setup import setup_logging
from streaming.config import BronzeJobConfig
from streaming.schemas import EVENT_JSON_SCHEMA, EVENT_TIME_FORMAT, REQUIRED_FIELDS
from streaming.spark_session import build_spark
from streaming.status import batch_record, read_status, update_status, write_status

log = logging.getLogger("streaming.bronze")

PARTITION_COLUMN = "ingest_date"
APPEND_ONLY_PROPERTY = "delta.appendOnly"
EXIT_REFUSED = 2  # start-up refused (guard or table check); 1 = runtime failure


class BronzeStartupError(Exception):
    """The job must not start (checkpoint-reset guard or unexpected table layout)."""


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


def checkpoint_has_batches(checkpoint_path: str) -> bool:
    """True if the checkpoint recorded at least one batch (a file in `offsets/`).

    Spark writes `offsets/N` before it runs batch N. A checkpoint folder with
    only `metadata` (or nothing) has never planned a batch, so it does not know
    which Kafka offsets the table already holds. Local paths only (/data/...).
    """
    offsets = Path(checkpoint_path) / "offsets"
    return offsets.is_dir() and any(f.name.isdigit() for f in offsets.iterdir())


def table_has_data(spark: SparkSession, table_path: str) -> bool:
    """True if a Delta table exists at the path and has at least one row."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, table_path):
        return False
    return spark.read.format("delta").load(table_path).limit(1).count() > 0


def check_checkpoint_guard(spark: SparkSession, table_path: str, checkpoint_path: str,
                           allow_rebuild: bool) -> str:
    """Refuse to start on a populated table with a missing/empty checkpoint.

    Why Delta's own idempotency does not help here: the sink skips a batch only
    if the table already has txn(appId = query id, version = batch id). A new
    checkpoint gets a NEW query id, so none of the old txn entries match, and
    re-reading the topic from `earliest` appends every message a second time.

    Returns:
        "fresh" (no data, no batches), "resume" (checkpoint has batches) or
        "rebuild" (data + no batches, allowed by BRONZE_ALLOW_REBUILD=1).

    Raises:
        BronzeStartupError: data + no batches, and allow_rebuild is False.
    """
    if checkpoint_has_batches(checkpoint_path):
        return "resume"
    if not table_has_data(spark, table_path):
        return "fresh"
    if not allow_rebuild:
        raise BronzeStartupError(
            f"bronze table {table_path} has data but checkpoint {checkpoint_path} has no "
            "batches (missing or reset). Starting would re-read the topic and append every "
            "message again under a new query id. Restore the checkpoint, or set "
            "BRONZE_ALLOW_REBUILD=1 if re-appending is really intended (DECISIONS D2.2).")
    log.warning("REBUILD: starting with an empty checkpoint on a populated bronze table; "
                "messages still in Kafka will be appended again", extra={"fields": {
                    "table_path": table_path, "checkpoint_path": checkpoint_path}})
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


class ProgressLogger(StreamingQueryListener):
    """Per completed micro-batch: log one structured line and update the status file.

    Spark posts the progress event only after the batch's Delta commit and the
    checkpoint commit (commits/N) are written, so the status file never claims
    a batch that is not durable (D1.15; checked for the Delta sink in D2.1 C12).
    """

    def __init__(self, query_name: str, status_path: str | None) -> None:
        super().__init__()
        self._query_name = query_name
        self._status_path = status_path
        # Keep history across restarts: start from what is on disk.
        self._status = read_status(status_path) if status_path else None

    def onQueryStarted(self, event) -> None:  # noqa: N802 (Spark API name)
        log.info("query started", extra={"fields": {
            "query_id": str(event.id), "run_id": str(event.runId), "name": event.name}})

    def onQueryProgress(self, event) -> None:  # noqa: N802
        p = event.progress
        source = p.sources[0] if p.sources else None
        fields = {
            "batch_id": p.batchId,
            "num_input_rows": p.numInputRows,
            "processed_rows_per_s": round(p.processedRowsPerSecond or 0.0, 1),
            "trigger_ms": p.durationMs.get("triggerExecution"),
            "start_offset": _offsets(source.startOffset) if source else None,
            "end_offset": _offsets(source.endOffset) if source else None,
        }
        level = logging.INFO if p.numInputRows > 0 else logging.DEBUG
        log.log(level, "batch progress", extra={"fields": fields})
        if self._status_path and source is not None:
            self._write_status(p, source)

    def _write_status(self, p, source) -> None:
        """Update the status file. Never raises: status is diagnostics, not data."""
        try:
            record = batch_record(
                batch_id=p.batchId, run_id=str(p.runId), num_input_rows=p.numInputRows,
                timestamp=p.timestamp, trigger_ms=p.durationMs.get("triggerExecution"),
                start_offset=source.startOffset, end_offset=source.endOffset,
            )
            self._status = update_status(
                self._status, record, query_name=self._query_name, query_id=str(p.id)
            )
            write_status(self._status, self._status_path)
        except Exception as exc:  # noqa: BLE001 - must not kill the streaming query
            log.warning("could not write status file", extra={"fields": {
                "path": self._status_path, "error": str(exc)}})

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        if event.exception:
            log.error("query failed", extra={"fields": {"error": event.exception}})
        else:
            log.info("query stopped", extra={"fields": {"query_id": str(event.id)}})


def _offsets(raw: str | None) -> object:
    """Kafka source offsets arrive as a JSON string; decode for readable logs."""
    if raw is None or raw in {"None", "null"}:  # first batch has no start offset
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


def run(cfg: BronzeJobConfig, master: str, driver_memory: str) -> int:
    """Start the streaming query and block until it stops. Returns an exit code:
    0 stopped cleanly, 1 failed, 2 start-up refused (BronzeStartupError)."""
    spark = build_spark(cfg.query_name, master, driver_memory)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger(cfg.query_name, cfg.status_path or None))
    log.info("starting", extra={"fields": {
        "spark_version": spark.version, "master": master, "driver_memory": driver_memory,
        "topic": cfg.topic, "table_path": cfg.table_path,
        "checkpoint_path": cfg.checkpoint_path, "status_path": cfg.status_path,
        "trigger": cfg.trigger_interval, "allow_rebuild": cfg.allow_rebuild,
        "max_offsets_per_trigger": cfg.max_offsets_per_trigger}})

    try:
        kafka_df = (  # lazy: nothing connects to Kafka until the query starts
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
            .option("subscribe", cfg.topic)
            .option("startingOffsets", cfg.starting_offsets)
            .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
            .option("failOnDataLoss", str(cfg.fail_on_data_loss).lower())
            .load()
        )
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
