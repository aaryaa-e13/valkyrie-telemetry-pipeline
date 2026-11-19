"""Bronze Delta table: creation, append-only guard, checkpoint-reset guard, the sink.

Runs inside the Spark image (make test-spark). Every table and checkpoint lives
in pytest's tmp_path, never in ./data.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common.delta_log import (  # noqa: E402
    appended_between,
    current_version,
    latest_txn_in_commits,
)
from simulator.events import EventType, GameEvent  # noqa: E402
from streaming import bronze  # noqa: E402
from streaming.bronze import (  # noqa: E402
    BronzeStartupError,
    bronze_schema,
    check_checkpoint_guard,
    ensure_bronze_table,
    prepare_bronze,
    start_bronze_query,
    to_raw_rows,
)
from streaming.config import BronzeJobConfig  # noqa: E402
from streaming.start_guard import read_checkpoint_state  # noqa: E402

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)


def _value(i: int) -> bytes:
    return GameEvent(
        event_id=f"e-{i}", event_type=EventType.ITEM_PURCHASE,
        event_time=datetime(2026, 11, 19, 12, 0, 0, tzinfo=UTC), match_id="m-1", match_seq=i + 1,
        patch_version="14.3", region="EUW", player_id="p_000001",
        champion_id="ch_quillon", item_id="it_stormglass", gold=100 + i,
    ).to_json_bytes()


def _kafka_rows(spark: SparkSession, offsets: range, partition: int = 0) -> DataFrame:
    """Kafka-shaped static rows (same column types as the real Kafka source)."""
    rows = [(b"m-1", _value(o), "game-events", partition, o, datetime(2026, 11, 19, 12, 0, 1))
            for o in offsets]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def _cfg(tmp_path: Path, **kw: Any) -> BronzeJobConfig:
    return BronzeJobConfig(table_path=str(tmp_path / "bronze"),
                           checkpoint_path=str(tmp_path / "cp"), status_path="", **kw)


def _detail(spark: SparkSession, path: str) -> Any:
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, path).detail().first()


def _populated(spark: SparkSession, tmp_path: Path) -> BronzeJobConfig:
    """A bronze table with 3 rows and no checkpoint at all."""
    cfg = _cfg(tmp_path)
    ensure_bronze_table(spark, cfg.table_path, bronze_schema(_kafka_rows(spark, range(0))))
    to_raw_rows(_kafka_rows(spark, range(3))).write.format("delta").mode("append") \
        .save(cfg.table_path)
    return cfg


# --- table creation and the append-only guard ------------------------------------------

def test_create_is_idempotent_append_only_and_partitioned(spark: SparkSession,
                                                          tmp_path: Path) -> None:
    path = str(tmp_path / "bronze")
    schema = bronze_schema(_kafka_rows(spark, range(0)))
    ensure_bronze_table(spark, path, schema)
    v = current_version(path)
    ensure_bronze_table(spark, path, schema)  # second start: no new commit
    assert current_version(path) == v == 0
    d = _detail(spark, path)
    assert list(d["partitionColumns"]) == ["ingest_date"]
    assert d["properties"]["delta.appendOnly"] == "true"
    cols = spark.read.format("delta").load(path).schema
    assert [f.name for f in cols] == [f.name for f in to_raw_rows(_kafka_rows(spark,
                                                                              range(0))).schema]
    assert all(f.nullable for f in cols)


def test_existing_table_with_wrong_layout_is_refused(spark: SparkSession,
                                                     tmp_path: Path) -> None:
    path = str(tmp_path / "other")
    to_raw_rows(_kafka_rows(spark, range(2))).write.format("delta") \
        .partitionBy("kafka_partition").save(path)  # wrong partition, no appendOnly
    with pytest.raises(BronzeStartupError) as exc:
        ensure_bronze_table(spark, path, bronze_schema(_kafka_rows(spark, range(0))))
    assert "partition columns ['kafka_partition']" in str(exc.value)
    assert "delta.appendOnly is not 'true'" in str(exc.value)


def test_delete_and_update_are_refused(spark: SparkSession, tmp_path: Path) -> None:
    cfg = _populated(spark, tmp_path)
    v = current_version(cfg.table_path)
    for stmt in (f"DELETE FROM delta.`{cfg.table_path}` WHERE kafka_offset = 0",
                 f"UPDATE delta.`{cfg.table_path}` SET gold = 0"):
        with pytest.raises(Exception, match="(?i)append.?only"):
            spark.sql(stmt)
    assert current_version(cfg.table_path) == v
    assert spark.read.format("delta").load(cfg.table_path).count() == 3


def test_optimize_works_on_append_only_table(spark: SparkSession, tmp_path: Path) -> None:
    cfg = _populated(spark, tmp_path)
    to_raw_rows(_kafka_rows(spark, range(3, 6))).write.format("delta").mode("append") \
        .save(cfg.table_path)
    v_before = current_version(cfg.table_path)
    metrics = spark.sql(f"OPTIMIZE delta.`{cfg.table_path}`").first()["metrics"]
    v_after = current_version(cfg.table_path)
    assert v_after == v_before + 1
    assert metrics["numFilesRemoved"] >= 2 and metrics["numFilesAdded"] == 1
    assert spark.read.format("delta").load(cfg.table_path).count() == 6
    # The log counter treats the rewrite as "no new rows" (dataChange=false).
    assert appended_between(cfg.table_path, v_before, v_after).rows == 0


def test_adding_a_nullable_column_still_works(spark: SparkSession, tmp_path: Path) -> None:
    """Phase 5 schema evolution must not be blocked by appendOnly or by our start-up check."""
    cfg = _populated(spark, tmp_path)
    spark.sql(f"ALTER TABLE delta.`{cfg.table_path}` ADD COLUMNS (client_build STRING)")
    ensure_bronze_table(spark, cfg.table_path, bronze_schema(_kafka_rows(spark, range(0))))
    # The old job (without the new column) can still append ...
    to_raw_rows(_kafka_rows(spark, range(3, 5))).write.format("delta").mode("append") \
        .save(cfg.table_path)
    # ... and a writer that has the column can fill it.
    from pyspark.sql import functions as F
    to_raw_rows(_kafka_rows(spark, range(5, 6))).withColumn("client_build", F.lit("b42")) \
        .write.format("delta").mode("append").save(cfg.table_path)
    # mergeSchema append with yet another new column also works.
    to_raw_rows(_kafka_rows(spark, range(6, 7))).withColumn("device", F.lit("pc")) \
        .write.format("delta").mode("append").option("mergeSchema", "true") \
        .save(cfg.table_path)
    df = spark.read.format("delta").load(cfg.table_path)
    assert df.count() == 7
    assert df.filter("client_build = 'b42'").count() == 1
    assert df.filter("client_build IS NULL").count() == 6
    assert "device" in df.columns
    assert _detail(spark, cfg.table_path)["properties"]["delta.appendOnly"] == "true"


# --- checkpoint-reset guard ---------------------------------------------------------------

def test_guard_fresh_when_nothing_exists(spark: SparkSession, tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    assert check_checkpoint_guard(spark, cfg.table_path, cfg.checkpoint_path, False) == "fresh"


def test_guard_fresh_for_empty_table(spark: SparkSession, tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    ensure_bronze_table(spark, cfg.table_path, bronze_schema(_kafka_rows(spark, range(0))))
    assert check_checkpoint_guard(spark, cfg.table_path, cfg.checkpoint_path, False) == "fresh"


@pytest.mark.parametrize("checkpoint_state", ["missing", "metadata only", "empty offsets"])
def test_guard_refuses_data_without_checkpoint_batches(spark: SparkSession, tmp_path: Path,
                                                       checkpoint_state: str) -> None:
    cfg = _populated(spark, tmp_path)
    cp = Path(cfg.checkpoint_path)
    if checkpoint_state != "missing":
        cp.mkdir()
        (cp / "metadata").write_text('{"id":"new-query"}')
    if checkpoint_state == "empty offsets":
        (cp / "offsets").mkdir()
    v = current_version(cfg.table_path)
    with pytest.raises(BronzeStartupError, match="BRONZE_ALLOW_REBUILD=1"):
        prepare_bronze(spark, cfg, _kafka_rows(spark, range(0)))
    assert current_version(cfg.table_path) == v  # refused before touching the table


def test_guard_allows_rebuild_when_asked(spark: SparkSession, tmp_path: Path,
                                         caplog: pytest.LogCaptureFixture) -> None:
    cfg = _populated(spark, tmp_path)
    rebuild = replace(cfg, allow_rebuild=True)
    with caplog.at_level(logging.WARNING, logger="streaming.bronze"):
        assert prepare_bronze(spark, rebuild, _kafka_rows(spark, range(0))) == "rebuild"
    assert any("REBUILD" in r.getMessage() for r in caplog.records)


class _NoStopSession:
    """The shared test session, but stop()/listeners/log level are no-ops."""

    class _Streams:
        def addListener(self, _listener: object) -> None:  # noqa: N802 (Spark API name)
            pass

    class _Context:
        def setLogLevel(self, _level: str) -> None:  # noqa: N802
            pass

    def __init__(self, session: SparkSession) -> None:
        self._s = session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._s, name)

    @property
    def streams(self) -> _Streams:
        return self._Streams()

    @property
    def sparkContext(self) -> _Context:  # noqa: N802
        return self._Context()

    def stop(self) -> None:
        pass


def test_run_exits_2_when_guard_refuses(spark: SparkSession, tmp_path: Path,
                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """End to end through run(): non-zero exit (2), no query started, table unchanged."""
    cfg = _populated(spark, tmp_path)
    v = current_version(cfg.table_path)
    monkeypatch.setattr(bronze, "build_spark", lambda *a, **k: _NoStopSession(spark))
    monkeypatch.setattr(bronze, "start_bronze_query",
                        lambda *a, **k: pytest.fail("query must not start"))
    assert bronze.run(cfg, "local[1]", "1g") == bronze.EXIT_REFUSED == 2
    assert current_version(cfg.table_path) == v
    assert not Path(cfg.checkpoint_path).exists()


# --- the streaming sink ---------------------------------------------------------------------

def _run_stream(spark: SparkSession, src: Path, cfg: BronzeJobConfig,
                max_files: int | None = None, where: str | None = None) -> str:
    """One availableNow run of the real bronze writer over Kafka-shaped Parquet files."""
    reader = spark.readStream.schema(KAFKA_SCHEMA)
    if max_files:
        reader = reader.option("maxFilesPerTrigger", max_files)
    stream = reader.parquet(str(src))
    if where:
        stream = stream.where(where)
    assert prepare_bronze(spark, cfg, stream) in {"fresh", "resume"}
    q = start_bronze_query(stream, cfg, trigger={"availableNow": True})
    q.awaitTermination(180)
    assert q.exception() is None
    return str(q.id)


def _add_src(spark: SparkSession, src: Path, offsets: range, partition: int = 0) -> None:
    _kafka_rows(spark, offsets, partition).coalesce(1).write.mode("append").parquet(str(src))


def _two_runs(spark: SparkSession, tmp_path: Path) -> tuple[BronzeJobConfig, str]:
    """Batches 0 (5 rows) and 1 (3 rows) from one checkpoint. Returns (cfg, query id)."""
    src, cfg = tmp_path / "src", _cfg(tmp_path)
    _add_src(spark, src, range(0, 5))
    q1 = _run_stream(spark, src, cfg)
    _add_src(spark, src, range(5, 8), partition=1)
    q2 = _run_stream(spark, src, cfg)  # restart from the same checkpoint
    assert q1 == q2  # query id is stored in the checkpoint
    return cfg, q1


def _guard(spark: SparkSession, cfg: BronzeJobConfig, allow_rebuild: bool = False) -> str:
    return check_checkpoint_guard(spark, cfg.table_path, cfg.checkpoint_path, allow_rebuild)


def test_sink_appends_once_per_message_with_stable_txn_app_id(spark: SparkSession,
                                                               tmp_path: Path) -> None:
    cfg, q = _two_runs(spark, tmp_path)
    df = spark.read.format("delta").load(cfg.table_path)
    assert df.count() == 8
    assert df.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count() == 8
    log = appended_between(cfg.table_path, -1, current_version(cfg.table_path))
    assert log.rows == 8
    assert [t["appId"] for t in log.txns] == [q, q]
    assert [t["version"] for t in log.txns] == [0, 1]
    assert _guard(spark, cfg) == "resume"  # txn 1 == checkpoint's last commit 1


# --- start-up guard, checkpoint has batches (the other direction) -------------------------

def test_txn_one_ahead_after_crash_before_commit_file_is_legal(spark: SparkSession,
                                                               tmp_path: Path) -> None:
    """Delta committed batch 1, commits/1 is missing: the one legal mismatch (T = C + 1)."""
    cfg, _ = _two_runs(spark, tmp_path)
    commits = Path(cfg.checkpoint_path) / "commits"
    (commits / "1").unlink()
    (commits / ".1.crc").unlink(missing_ok=True)
    v = current_version(cfg.table_path)
    assert _guard(spark, cfg) == "resume"
    _run_stream(spark, tmp_path / "src", cfg)  # Spark re-runs batch 1, the sink skips it
    assert (commits / "1").exists()
    assert current_version(cfg.table_path) == v
    assert spark.read.format("delta").load(cfg.table_path).count() == 8


def test_table_missing_with_checkpoint_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    cfg, _ = _two_runs(spark, tmp_path)
    shutil.rmtree(cfg.table_path)
    with pytest.raises(BronzeStartupError, match="missing or has no txn for this query id"):
        _guard(spark, cfg)
    assert not Path(cfg.table_path).exists()  # refused before creating anything
    assert _guard(spark, cfg, allow_rebuild=True) == "rebuild"


def test_table_without_txn_for_this_query_is_refused(spark: SparkSession,
                                                     tmp_path: Path) -> None:
    """The table was replaced by one written by something else (here: plain appends)."""
    cfg, _ = _two_runs(spark, tmp_path)
    other = _populated(spark, tmp_path / "other")
    with pytest.raises(BronzeStartupError, match="no txn for this query id"):
        check_checkpoint_guard(spark, other.table_path, cfg.checkpoint_path, False)


def test_table_behind_checkpoint_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    """Table restored from a copy taken after batch 0; the checkpoint has batch 1."""
    src, cfg = tmp_path / "src", _cfg(tmp_path)
    _add_src(spark, src, range(0, 5))
    _run_stream(spark, src, cfg)
    backup = tmp_path / "backup"
    shutil.copytree(cfg.table_path, backup)
    _add_src(spark, src, range(5, 8))
    _run_stream(spark, src, cfg)
    shutil.rmtree(cfg.table_path)
    shutil.copytree(backup, cfg.table_path)
    with pytest.raises(BronzeStartupError, match="behind the checkpoint.*lose batches 1..1"):
        _guard(spark, cfg)


def test_table_ahead_of_checkpoint_is_refused(spark: SparkSession, tmp_path: Path) -> None:
    """Another writer used this query id as its txnAppId, with a higher version."""
    cfg, q = _two_runs(spark, tmp_path)
    to_raw_rows(_kafka_rows(spark, range(100, 101))).write.format("delta").mode("append") \
        .option("txnAppId", q).option("txnVersion", 5).save(cfg.table_path)
    with pytest.raises(BronzeStartupError, match="ahead of the checkpoint"):
        _guard(spark, cfg)


def test_batch_with_zero_output_rows_still_commits_txn(spark: SparkSession,
                                                       tmp_path: Path) -> None:
    """Why T can never lag C: the sink commits a txn even when a batch writes no rows."""
    src, cfg = tmp_path / "src", _cfg(tmp_path)
    _add_src(spark, src, range(0, 5))
    q = _run_stream(spark, src, cfg, where="offset < 3")
    _add_src(spark, src, range(10, 13))  # new input, but every row is filtered out
    _run_stream(spark, src, cfg, where="offset < 3")
    log = appended_between(cfg.table_path, -1, current_version(cfg.table_path))
    assert log.rows == 3
    assert [(t["appId"], t["version"]) for t in log.txns] == [(q, 0), (q, 1)]
    assert sorted(p.name for p in (Path(cfg.checkpoint_path) / "commits").iterdir()
                  if p.name.isdigit()) == ["0", "1"]
    assert _guard(spark, cfg) == "resume"


def test_txn_is_found_in_delta_checkpoint_after_log_cleanup(spark: SparkSession,
                                                            tmp_path: Path) -> None:
    """Old commit JSONs gone (log cleanup): the txn is read from the checkpoint Parquet."""
    src, cfg = tmp_path / "src", _cfg(tmp_path)
    for i in range(10):
        _add_src(spark, src, range(i * 2, i * 2 + 2))
    _run_stream(spark, src, cfg, max_files=1)  # batches 0..9 -> versions 1..10
    log_dir = Path(cfg.table_path) / "_delta_log"
    assert (log_dir / f"{10:020d}.checkpoint.parquet").exists()
    spark.sql(f"OPTIMIZE delta.`{cfg.table_path}`")  # version 11, no txn
    assert current_version(cfg.table_path) == 11
    for v in range(11):  # what cleanup does: drop commits up to the checkpoint
        (log_dir / f"{v:020d}.json").unlink()
        (log_dir / f"{v:020d}.crc").unlink(missing_ok=True)
    assert latest_txn_in_commits(cfg.table_path, read_checkpoint_state(
        cfg.checkpoint_path).query_id) == (None, False)
    assert _guard(spark, cfg) == "resume"  # txn 9 from the checkpoint == commits/9
    assert spark.read.format("delta").load(cfg.table_path).count() == 20
