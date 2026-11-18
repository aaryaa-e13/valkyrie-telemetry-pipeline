"""Shared fixtures for tests that need Spark (run inside the Spark image)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

pyspark = pytest.importorskip("pyspark")  # skip cleanly outside the Spark image

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One small local SparkSession for the whole test session (JVM start is slow).

    Delta's SQL extension and catalog are static settings (they cannot be added
    once the session exists), so they are set here for every Spark test. The
    Delta jars are baked into the image (DECISIONS D2.1).
    """
    session = (
        SparkSession.builder.master("local[1]").appName("valkyrie-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
