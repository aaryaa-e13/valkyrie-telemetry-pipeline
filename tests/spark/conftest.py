"""Shared fixtures for tests that need Spark (run inside the Spark image)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

pyspark = pytest.importorskip("pyspark")  # skip cleanly outside the Spark image

from pyspark.sql import SparkSession  # noqa: E402

from streaming.spark_session import DELTA_CONF  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One small local SparkSession for the whole test session (JVM start is slow).

    Delta's SQL extension and catalog are static settings (they cannot be added
    once the session exists), so they are set here for every Spark test, from
    the same DELTA_CONF the job uses. The Delta jars are baked into the image
    (DECISIONS D2.1).
    """
    builder = SparkSession.builder.master("local[1]").appName("valkyrie-tests")
    for key, value in DELTA_CONF.items():
        builder = builder.config(key, value)
    session = (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        # Test speed only (slice 3): Delta rebuilds a table snapshot with 50 tasks by
        # default; on local[1] that is ~5 s per Delta operation on tiny tables.
        .config("spark.databricks.delta.snapshotPartitions", "2")
        # UI on (fixed port, inside the test container only) so the MERGE
        # pruning test can read scan metrics from the REST API (slice 3, Q4).
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4050")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
