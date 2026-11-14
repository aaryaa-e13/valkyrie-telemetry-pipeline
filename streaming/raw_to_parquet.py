"""Raw ingestion job: Kafka `game-events` -> Parquet (partitioned by ingest_date).

What it guarantees (Phase 1):
    * No message is skipped: Kafka offsets are tracked in the checkpoint, and
      failOnDataLoss=true stops the job if retention deleted unread data.
    * No half-written or duplicated *files* after a restart (Spark's file sink
      commit log, `_spark_metadata`, for readers that use Spark).
    * Unparseable messages are kept (parse_ok = false, raw_value preserved).

What it does NOT do (Phase 2+): deduplicate events, handle late data with
watermarks, or MERGE idempotently. Duplicate events in Kafka produce
duplicate rows here.

Run (inside the Spark container):
    python -m streaming.raw_to_parquet
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
from functools import reduce
from types import FrameType

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from common.logging_setup import setup_logging
from streaming.config import RawJobConfig
from streaming.schemas import EVENT_JSON_SCHEMA, EVENT_TIME_FORMAT, REQUIRED_FIELDS
from streaming.status import batch_record, read_status, update_status, write_status

log = logging.getLogger("streaming.raw_to_parquet")


def build_spark(app_name: str, master: str, driver_memory: str) -> SparkSession:
    """Create a local-mode SparkSession with laptop-friendly settings.

    spark.driver.memory must be set before the JVM starts; PySpark passes
    builder configs to the JVM launcher, so setting it here works when this
    module is started with plain `python` (not inside an existing JVM).
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")  # ingest_date is a UTC date
        .config("spark.sql.shuffle.partitions", "8")  # default 200 is for clusters
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


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


class ProgressLogger(StreamingQueryListener):
    """Per completed micro-batch: log one structured line and update the status file.

    Spark posts the progress event only after the batch's sink commit
    (_spark_metadata/N) and checkpoint commit (commits/N) are written, so the
    status file never claims a batch that is not durable (see D1.15).
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


def run(cfg: RawJobConfig, master: str, driver_memory: str) -> int:
    """Start the streaming query and block until it stops. Returns an exit code."""
    spark = build_spark(cfg.query_name, master, driver_memory)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger(cfg.query_name, cfg.status_path or None))
    log.info("starting", extra={"fields": {
        "spark_version": spark.version, "master": master, "driver_memory": driver_memory,
        "topic": cfg.topic, "output_path": cfg.output_path,
        "checkpoint_path": cfg.checkpoint_path, "status_path": cfg.status_path,
        "trigger": cfg.trigger_interval,
        "max_offsets_per_trigger": cfg.max_offsets_per_trigger}})

    try:
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
            .option("subscribe", cfg.topic)
            .option("startingOffsets", cfg.starting_offsets)
            .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
            .option("failOnDataLoss", str(cfg.fail_on_data_loss).lower())
            .load()
        )
        query = (
            to_raw_rows(kafka_df)
            .writeStream.queryName(cfg.query_name)
            .format("parquet")
            .outputMode("append")
            .option("path", cfg.output_path)
            .option("checkpointLocation", cfg.checkpoint_path)
            .partitionBy("ingest_date")
            .trigger(processingTime=cfg.trigger_interval)
            .start()
        )
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
    cfg = RawJobConfig.from_env()
    setup_logging(cfg.log_level, component="raw_to_parquet")
    logging.getLogger("py4j").setLevel(logging.WARNING)  # Py4J gateway chatter
    master = os.environ.get("SPARK_MASTER", "local[4]")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    return run(cfg, master, driver_memory)


if __name__ == "__main__":
    sys.exit(main())
