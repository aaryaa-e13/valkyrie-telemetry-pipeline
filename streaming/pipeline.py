"""The Spark container's job: bronze and silver streaming queries in one process.

Start-up (both guards run before any query starts, so a refusal never leaves
one query running alone):
    1. bronze guard + create/check bronze table   (streaming/bronze.py, D2.2)
    2. silver guard + create/check silver/control (streaming/silver.py, D2.3)
    3. start bronze (Kafka -> bronze), then silver (bronze -> silver)

Failure policy (DECISIONS D2.3): if either query stops with an error, the other
is stopped too and the process exits 1. A broken silver therefore also stops
bronze ingestion: it fails loudly instead of letting bronze run on while silver
silently falls behind. Kafka's 7-day retention is the buffer: after a fix,
bronze resumes from its checkpoint and nothing is lost if the outage is shorter.

Exit codes: 0 stopped cleanly (SIGTERM/SIGINT), 1 a query failed or could not
start, 2 start-up refused by a guard or table check.

Run (inside the Spark container):
    python -m streaming.pipeline
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from collections.abc import Callable
from types import FrameType

from pyspark.sql.streaming import StreamingQuery

from common.logging_setup import setup_logging
from streaming.bronze import (
    EXIT_REFUSED,
    kafka_source,
    prepare_bronze,
    start_bronze_query,
)
from streaming.config import (
    BronzeJobConfig,
    SilverJobConfig,
    delta_snapshot_partitions_from_env,
)
from streaming.progress import ProgressLogger
from streaming.silver import prepare_silver, start_silver_query
from streaming.spark_session import build_spark
from streaming.start_guard import BronzeStartupError

log = logging.getLogger("streaming.pipeline")


def _stop_all(queries: list[StreamingQuery]) -> None:
    for q in queries:
        if q.isActive:
            q.stop()  # an interrupted batch is re-run from the checkpoint on restart


def run(bcfg: BronzeJobConfig, scfg: SilverJobConfig, master: str, driver_memory: str) -> int:
    """Start both queries and block until a stop signal or a failure."""
    snapshot_partitions = delta_snapshot_partitions_from_env()
    spark = build_spark("valkyrie-pipeline", master, driver_memory,
                        delta_snapshot_partitions=snapshot_partitions)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger({bcfg.query_name: bcfg.status_path,
                                              scfg.query_name: scfg.status_path}))
    log.info("starting", extra={"fields": {
        "spark_version": spark.version, "master": master, "driver_memory": driver_memory,
        "topic": bcfg.topic, "bronze_path": bcfg.table_path, "silver_path": scfg.table_path,
        "control_path": scfg.control_path, "trigger": bcfg.trigger_interval,
        "dedupe_horizon_days": scfg.dedupe_horizon_days,
        "delta_snapshot_partitions": snapshot_partitions or "default (50)",
        "bronze_allow_rebuild": bcfg.allow_rebuild,
        "silver_allow_rebuild": scfg.allow_rebuild}})

    queries: list[StreamingQuery] = []
    try:
        kafka_df = kafka_source(spark, bcfg)
        bmode = prepare_bronze(spark, bcfg, kafka_df)
        smode = prepare_silver(spark, scfg)
        log.info("tables ready", extra={"fields": {"bronze_mode": bmode, "silver_mode": smode}})
        queries.append(start_bronze_query(kafka_df, bcfg))
        queries.append(start_silver_query(spark, scfg))
    except BronzeStartupError as exc:  # also SilverStartupError (subclass)
        log.error("start-up refused", extra={"fields": {"error": str(exc)}})
        _stop_all(queries)
        spark.stop()
        return EXIT_REFUSED
    except Exception as exc:  # e.g. unwritable path, bad options
        log.error("failed to start", extra={"fields": {
            "error_type": type(exc).__name__, "error": str(exc).strip().splitlines()[0][:500]}})
        _stop_all(queries)
        spark.stop()
        return 1

    # `docker stop` sends SIGTERM. Only set a flag here; Spark is called from the
    # main loop (calling it inside a signal handler could deadlock Py4J).
    stop_requested = {"value": False}

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("stop requested", extra={"fields": {"signal": signum}})
        stop_requested["value"] = True

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    code = supervise(queries, lambda: stop_requested["value"])
    spark.stop()
    return code


def supervise(queries: list[StreamingQuery], should_stop: Callable[[], bool],
              poll_s: float = 2.0) -> int:
    """Block until a stop request or until any query stops; then stop all queries.

    Returns 0 after a requested stop, 1 if a query failed or stopped on its own.
    This is the D2.3 failure policy: one failing query stops the other.
    """
    while all(q.isActive for q in queries) and not should_stop():
        queries[0].awaitTermination(timeout=poll_s)
    requested = should_stop()
    failed = [q for q in queries if not q.isActive and q.exception() is not None]
    for q in failed:
        log.error("query terminated with error; stopping the pipeline (D2.3)",
                  extra={"fields": {"query": q.name,
                                    "error": str(q.exception()).splitlines()[0][:500]}})
    if not failed and not requested:
        log.error("a query stopped without error and without a stop request; "
                  "stopping the pipeline", extra={"fields": {
                      "stopped": [q.name for q in queries if not q.isActive]}})
    _stop_all(queries)
    return 1 if failed or not requested else 0


def main() -> int:
    """Entry point: read both configs from env and run."""
    bcfg = BronzeJobConfig.from_env()
    scfg = SilverJobConfig.from_env()
    setup_logging(bcfg.log_level, component="pipeline")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    master = os.environ.get("SPARK_MASTER", "local[4]")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    return run(bcfg, scfg, master, driver_memory)


if __name__ == "__main__":
    sys.exit(main())
