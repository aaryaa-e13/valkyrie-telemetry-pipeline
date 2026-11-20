"""One place that builds VALKYRIE's local-mode SparkSessions (with Delta Lake).

Delta's SQL extension and catalog are *static* settings: they must be set
before the session starts and cannot be added later. Keeping them here means
the streaming job, verify and the Spark tests cannot drift apart
(tests/spark/conftest.py imports DELTA_CONF). The Delta jars themselves are
baked into the image at build time (DECISIONS D2.1); nothing is downloaded.

The slice 1 gate (streaming/delta_compat_check.py) keeps its own session on
purpose: it is a frozen check of the stack, not of our wrappers.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import SparkSession

SNAPSHOT_PARTITIONS_CONF = "spark.databricks.delta.snapshotPartitions"

DELTA_CONF: dict[str, str] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def build_spark(app_name: str, master: str, driver_memory: str,
                shuffle_partitions: int = 8,
                extra: Mapping[str, str] | None = None,
                delta_snapshot_partitions: int | None = None) -> SparkSession:
    """Create (or reuse) a local-mode SparkSession with Delta enabled.

    spark.driver.memory must be set before the JVM starts; PySpark passes
    builder configs to the JVM launcher, so this works when the caller was
    started with plain `python` (not inside an existing JVM).

    Args:
        app_name: Spark application name (shown in the UI and logs).
        master: e.g. "local[4]".
        driver_memory: JVM heap, e.g. "2g".
        shuffle_partitions: the default 200 is for clusters, not a laptop.
        extra: further settings, applied last.
        delta_snapshot_partitions: tasks Delta uses to rebuild a table snapshot;
            None keeps Delta's default (50). See DECISIONS D2.3.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")  # ingest_date is a UTC date
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
    )
    conf = dict(DELTA_CONF)
    if delta_snapshot_partitions is not None:
        conf[SNAPSHOT_PARTITIONS_CONF] = str(delta_snapshot_partitions)
    for key, value in {**conf, **(extra or {})}.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
