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


TAIL_OFFSET = ('{"sourceVersion":1,"reservoirId":"test","reservoirVersion":%d,"index":-1,'
               '"isStartingVersion":false}')


@pytest.fixture
def tail_offsets():  # noqa: ANN201 (returns a helper function)
    """Write offsets/0..n-1 as TAIL offsets (isStartingVersion false) into a checkpoint.

    Tests that call process_batch directly for batch N > 0 need offsets/N-1: the
    batch reads its snapshot status there (D2.4, A1). Real shape, Delta 4.4.1.
    """
    from pathlib import Path

    def write(checkpoint_path: str, n: int) -> None:
        d = Path(checkpoint_path) / "offsets"
        d.mkdir(parents=True, exist_ok=True)
        for k in range(n):
            (d / str(k)).write_text(
                'v1\n{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{}}\n'
                + TAIL_OFFSET % (k + 1), encoding="utf-8")

    return write
