"""Delta smoke test that keeps guarding the image in every `make test-spark`.

Create a path table, insert-only MERGE, the same MERGE again (idempotent). The
full gate with streaming and replay checks is `make delta-compat` (D2.1).

Run with: make test-spark
"""

from __future__ import annotations

from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")  # skip cleanly outside the Spark image
pytest.importorskip("delta")

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

MERGE = "MERGE INTO delta.`{t}` t USING src s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"


def test_create_merge_merge_again(spark: SparkSession, tmp_path: Path) -> None:
    table = str(tmp_path / "t")
    spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, v STRING") \
        .write.format("delta").save(table)
    spark.createDataFrame([(3, "c-new"), (4, "d"), (5, "e")], "id INT, v STRING") \
        .createOrReplaceTempView("src")

    spark.sql(MERGE.format(t=table))
    after_first = DeltaTable.forPath(spark, table).history(1).collect()[0]
    spark.sql(MERGE.format(t=table))  # replay of the same batch: must change nothing

    rows = {r["id"]: r["v"] for r in spark.read.format("delta").load(table).collect()}
    assert rows == {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}  # id 3 not overwritten
    assert after_first["operationMetrics"]["numTargetRowsInserted"] == "2"
    # An insert-only MERGE that inserts nothing writes no new commit (C4 in D2.1).
    assert DeltaTable.forPath(spark, table).history(1).collect()[0]["version"] == \
        after_first["version"]
