"""Silver: D9 key check, in-batch dedupe, insert-only MERGE, control rows, pruning.

Runs inside the Spark image (make test-spark; tests marked spark_fast also run in
make test-spark-fast). Every table and checkpoint lives in pytest's tmp_path. Shared
builders: silver_helpers.py. SPARK_TESTS=tests/spark/test_silver.py runs this file alone.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common import spark_rest  # noqa: E402
from common.delta_log import current_version, live_files_in_range, read_commits  # noqa: E402
from streaming.silver import (  # noqa: E402
    LATE_FLAG,
    PARTITION_COLUMN,
    batch_counts,
    dedupe_in_batch,
    is_late,
    process_batch,
    split_counts,
)
from streaming.silver_merge import MergeBoundsError  # noqa: E402
from tests.spark.silver_helpers import (  # noqa: E402
    T0,
    _bronze_rows,
    _control,
    _ready,
    _silver,
    fast,
)

# --- pure parts (fast) ------------------------------------------------------------------

@fast
def test_batch_counts_per_reason_and_bounds_from_keyed_rows_only(spark: SparkSession) -> None:
    # ingest_time: bronze always sets it (bronze.py); m_k reads it for the D3.7 S1 skip.
    schema = ("parse_ok boolean, event_id string, event_time timestamp, "
              "kafka_timestamp timestamp, ingest_time timestamp")
    late = datetime(2027, 1, 1, tzinfo=UTC)
    k = T0 + timedelta(seconds=1)
    df = spark.createDataFrame([
        (True, "a", T0, k, k), (True, "a", T0 + timedelta(seconds=64), k, k),
        (True, "b", T0, k, k),
        (False, None, None, k, k),         # unparsed + null id + null time
        (True, None, late, late, late),    # null id only (would widen the bounds if counted)
        (True, "c", None, late, late),     # null time only
        (None, "d", late, late, late),     # parse_ok NULL counts as unparsed
    ], schema)
    c = batch_counts(df)
    assert (c.rows_in, c.excluded_unparsed, c.excluded_null_event_id,
            c.excluded_null_event_time, c.excluded_total) == (7, 2, 2, 2, 4)
    assert (c.keyed_rows, c.keyed_unique, c.dup_in_batch) == (3, 2, 1)
    assert c.min_date == c.max_date == date(2026, 11, 19)
    # m_N from keyed rows only, clamped to Kafka time: the +64 s copy counts as k.
    assert c.clamped_max_us == int(k.timestamp()) * 1_000_000 and c.clamped_rows == 1


@fast
def test_non_keyed_copy_with_lower_offset_does_not_hide_the_keyed_copy(
        spark: SparkSession) -> None:
    """Same event_id twice: offset 0 has a NULL event_time (not keyed), offset 1 is keyed.

    The keyed copy is kept, and the counts are: one excluded row (null time), one keyed
    row, one unique id, one row deduped. The split runs like plan_batch runs it: on the
    deduped rows, flagged with is_late, with the DATE partition column.
    """
    schema = ("parse_ok boolean, event_id string, event_time timestamp, "
              "kafka_timestamp timestamp, kafka_topic string, kafka_partition int, "
              "kafka_offset long, ingest_time timestamp")
    k = T0 + timedelta(seconds=1)
    df = spark.createDataFrame([(True, "x", None, k, "game-events", 0, 0, k),
                                (True, "x", T0, k, "game-events", 0, 1, k)], schema)
    kept = dedupe_in_batch(df)
    assert [(row.event_id, row.kafka_offset) for row in kept.collect()] == [("x", 1)]
    c = batch_counts(df)
    assert (c.rows_in, c.excluded_null_event_time, c.excluded_total) == (2, 1, 1)
    assert (c.keyed_rows, c.keyed_unique, c.dup_in_batch) == (1, 1, 0)
    assert c.min_date == c.max_date == date(2026, 11, 19)
    for wm in (None, int((T0 - timedelta(minutes=5)).timestamp()) * 1_000_000):
        s = split_counts(kept.withColumn(PARTITION_COLUMN, F.to_date("event_time"))
                         .withColumn(LATE_FLAG, is_late(wm)))
        assert (s.deduped, s.late, s.ontime) == (1, 0, 1)
        assert s.ontime_min_date == s.ontime_max_date == date(2026, 11, 19)


@fast
def test_dedupe_keeps_lowest_offset_copy(spark: SparkSession) -> None:
    df = _bronze_rows(spark, [(14, "e-1", T0 + timedelta(seconds=64)), (8, "e-1", T0),
                              (9, "e-2", T0), (10, None, T0)])
    rows = {r.event_id: r for r in dedupe_in_batch(df).collect()}
    assert set(rows) == {"e-1", "e-2"}
    assert rows["e-1"].kafka_offset == 8 and rows["e-1"].event_time == T0.replace(tzinfo=None)


@fast
def test_process_batch_inserts_once_and_replay_writes_nothing(spark: SparkSession,
                                                              tmp_path: Path) -> None:
    cfg = _ready(spark, tmp_path)
    batch = _bronze_rows(spark, [(8, "e-1", T0), (9, "e-2", T0),
                                 (14, "e-1", T0 + timedelta(seconds=64)), (15, None, T0)])
    row = process_batch(batch, 0, cfg, "q-1")
    assert (row["rows_in"], row["excluded_total"], row["dup_in_batch"],
            row["keyed_unique"], row["inserted_this_attempt"]) == (4, 1, 1, 2, 2)
    assert (row["merge_lo"], row["merge_hi"], row["merge_window_days"]) == (
        date(2026, 11, 12), date(2026, 11, 26), 15)
    assert row["merge_ms"] >= 0
    silver = {r.event_id: r for r in _silver(spark, cfg).collect()}
    assert set(silver) == {"e-1", "e-2"} and silver["e-1"].kafka_offset == 8
    assert silver["e-1"].event_date == date(2026, 11, 19)
    assert {"raw_value", "parse_ok"}.isdisjoint(_silver(spark, cfg).columns)
    vs, vc = current_version(cfg.table_path), current_version(cfg.control_path)
    replay = process_batch(batch, 0, cfg, "q-1")      # same batch again (crash replay)
    assert replay["inserted_this_attempt"] == 0
    assert (current_version(cfg.table_path), current_version(cfg.control_path)) == (vs, vc)
    assert [(r["batch_id"], r["inserted_this_attempt"]) for r in _control(spark, cfg)] == [(0, 2)]


@fast
def test_cross_batch_duplicate_is_not_inserted_again(spark: SparkSession, tmp_path: Path,
                                                     tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path)
    tail_offsets(cfg.checkpoint_path, 1)
    process_batch(_bronze_rows(spark, [(8, "e-1", T0)]), 0, cfg, "q-1")
    later = _bronze_rows(spark, [(14, "e-1", T0 + timedelta(seconds=64)), (15, "e-2", T0)])
    assert process_batch(later, 1, cfg, "q-1")["inserted_this_attempt"] == 1
    silver = {r.event_id: r for r in _silver(spark, cfg).collect()}
    assert set(silver) == {"e-1", "e-2"} and silver["e-1"].kafka_offset == 8
    assert silver["e-1"].silver_batch_id == 0


@fast
def test_all_keyless_batch_skips_merge_but_writes_control_row(spark: SparkSession,
                                                               tmp_path: Path) -> None:
    cfg = _ready(spark, tmp_path)
    vs = current_version(cfg.table_path)
    row = process_batch(_bronze_rows(spark, [(1, None, T0), (2, None, T0)]), 0, cfg, "q-1")
    assert row["merge_skipped"] is True and row["merge_lo"] is None
    assert (row["excluded_unparsed"], row["excluded_total"]) == (2, 2)
    assert current_version(cfg.table_path) == vs   # no MERGE, no silver commit
    assert len(_control(spark, cfg)) == 1


@fast
def test_zero_row_batch_writes_control_row(spark: SparkSession, tmp_path: Path) -> None:
    cfg = _ready(spark, tmp_path)
    row = process_batch(_bronze_rows(spark, []), 0, cfg, "q-1")
    assert (row["rows_in"], row["merge_skipped"]) == (0, True)
    assert [(r["batch_id"], r["rows_in"]) for r in _control(spark, cfg)] == [(0, 0)]


@fast
def test_missing_bound_with_keyed_rows_fails_the_batch(spark: SparkSession, tmp_path: Path,
                                                       monkeypatch: pytest.MonkeyPatch) -> None:
    """If a bound were ever None despite on-time rows, the batch fails and writes nothing."""
    from streaming import silver

    cfg = _ready(spark, tmp_path)
    real = silver.split_counts
    monkeypatch.setattr(silver, "split_counts",
                        lambda rows: replace(real(rows), ontime_min_date=None))
    vs, vc = current_version(cfg.table_path), current_version(cfg.control_path)
    with pytest.raises(MergeBoundsError, match="NULL bound"):
        process_batch(_bronze_rows(spark, [(1, "e-1", T0)]), 0, cfg, "q-1")
    assert (current_version(cfg.table_path), current_version(cfg.control_path)) == (vs, vc)


@fast
def test_null_bound_hazard_really_duplicates(spark: SparkSession, tmp_path: Path) -> None:
    """Why the D9 rule exists: a NULL bound makes the ON clause NULL and re-inserts
    an id that is already there. If a future Delta changes this, this test says so."""
    cfg = _ready(spark, tmp_path)
    process_batch(_bronze_rows(spark, [(1, "e-1", T0)]), 0, cfg, "q-1")
    _silver(spark, cfg).limit(1).createOrReplaceTempView("hazard_src")
    spark.sql(f"MERGE INTO delta.`{cfg.table_path}` t USING hazard_src s "
              "ON t.event_id = s.event_id AND t.event_date BETWEEN NULL AND NULL "
              "WHEN NOT MATCHED THEN INSERT *")
    assert _silver(spark, cfg).where("event_id = 'e-1'").count() == 2


@fast
def test_restamped_duplicate_beyond_horizon_is_inserted_twice(spark: SparkSession,
                                                              tmp_path: Path,
                                                              tail_offsets: Any) -> None:
    """Documented limit (parent plan D2): copies more than H days apart both land."""
    cfg = _ready(spark, tmp_path, dedupe_horizon_days=0)
    tail_offsets(cfg.checkpoint_path, 1)
    process_batch(_bronze_rows(spark, [(1, "e-1", T0)]), 0, cfg, "q-1")
    process_batch(_bronze_rows(spark, [(2, "e-1", T0 + timedelta(days=1))]), 1, cfg, "q-1")
    assert _silver(spark, cfg).where("event_id = 'e-1'").count() == 2


@fast
def test_merge_prunes_target_partitions(spark: SparkSession, tmp_path: Path,
                                       tail_offsets: Any) -> None:
    """Pruning evidence (D2.3): Spark's scan metrics of the MERGE read exactly the
    files the Delta log says are in [lo, hi], and fewer than all files.

    W is 30 days here so nothing is late: this test is about silver's MERGE only.
    """
    cfg = _ready(spark, tmp_path, dedupe_horizon_days=0, watermark_delay_s=30 * 86400)
    tail_offsets(cfg.checkpoint_path, 11)
    base = datetime(2026, 11, 1, 12, tzinfo=UTC)
    for d in range(10):                     # 10 event_date partitions, 1 file each
        process_batch(_bronze_rows(spark, [(d, f"old-{d}", base + timedelta(days=d))]),
                      d, cfg, "q-1")
    ui = spark.sparkContext.uiWebUrl
    assert ui, "the test session must run with the Spark UI on (conftest, Q4)"
    before = spark_rest.newest_execution_id(ui)
    row = process_batch(_bronze_rows(spark, [(20, "old-8", base + timedelta(days=8)),
                                             (21, "new-1", base + timedelta(days=9))]),
                        10, cfg, "q-1")
    assert row["inserted_this_attempt"] == 1
    scan = spark_rest.newest_merge_scan(ui, after_id=before)
    assert scan is not None, "no MERGE scan metrics found in the REST API"
    # Expectation from the Delta log, at the version BEFORE this MERGE committed.
    commits = [c for c in read_commits(Path(cfg.table_path))
               if c[0] < current_version(cfg.table_path)]
    inside, total = live_files_in_range(commits, "event_date", row["merge_lo"].isoformat(),
                                        row["merge_hi"].isoformat())
    assert (inside, total) == (2, 10)
    assert scan.files_read == inside < total
    assert scan.partitions_read == 2

