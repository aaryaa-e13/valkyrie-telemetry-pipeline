"""Shared fixtures for tests that need Spark (run inside the Spark image)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

pyspark = pytest.importorskip("pyspark")  # skip cleanly outside the Spark image

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One small local SparkSession for the whole test session (JVM start is slow)."""
    session = (
        SparkSession.builder.master("local[1]").appName("valkyrie-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
