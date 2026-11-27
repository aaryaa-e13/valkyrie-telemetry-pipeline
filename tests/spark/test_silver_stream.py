"""Silver streaming: control row per batch, checkpoint reset, guard, failure policy, verify.

Runs inside the Spark image (make test-spark; tests marked spark_fast also run in
make test-spark-fast). Every table and checkpoint lives in pytest's tmp_path. Shared
builders: silver_helpers.py. SPARK_TESTS=tests/spark/test_silver_stream.py runs this file alone.
"""

from __future__ import annotations

import shutil
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common.delta_log import current_version  # noqa: E402
from e2e import catchup  # noqa: E402
from streaming.progress import ProgressLogger  # noqa: E402
from streaming.silver import (  # noqa: E402
    SilverStartupError,
    check_silver_guard,
)
from streaming.start_guard import read_checkpoint_state  # noqa: E402
from streaming.status import read_status  # noqa: E402
from tests.spark.silver_helpers import (  # noqa: E402
    T0,
    _bronze,
    _cfg,
    _committed_batches,
    _control,
    _manifest,
    _run,
    _seed_like,
    _silver,
    fast,
)

# --- streaming: control row per batch, restart, guard (slower) ------------------------------


def test_stream_dedupes_and_writes_a_control_row_for_every_batch(spark: SparkSession,
                                                                 tmp_path: Path) -> None:
    cfg = replace(_cfg(tmp_path), status_path=str(tmp_path / "silver.json"))
    _seed_like(spark, cfg)
    listener = ProgressLogger({cfg.query_name: cfg.status_path})
    spark.streams.addListener(listener)
    try:
        q1 = _run(spark, cfg)
        silver = {r.event_id: r.kafka_offset for r in _silver(spark, cfg).collect()}
        assert silver == {"s-1": 8, "s-2": 9, "s-3": 10}   # 7 bronze rows -> 3 silver rows
        # Cross-batch: a third copy of s-1 and a new id arrive after the first run.
        _bronze(spark, cfg, [(30, "s-1", T0 + timedelta(seconds=128)), (31, "s-4", T0)])
        # A bronze commit without data change (OPTIMIZE): must not duplicate anything.
        spark.sql(f"OPTIMIZE delta.`{cfg.bronze_path}`")
        q2 = _run(spark, cfg)
        assert q1 == q2
        spark.streams.awaitAnyTermination(1)
    finally:
        spark.streams.removeListener(listener)
    silver = {r.event_id: r.kafka_offset for r in _silver(spark, cfg).collect()}
    assert silver == {"s-1": 8, "s-2": 9, "s-3": 10, "s-4": 31}
    control = _control(spark, cfg)
    assert {r["silver_query_id"] for r in control} == {q1}       # id from checkpoint metadata
    assert [r["batch_id"] for r in control] == _committed_batches(cfg)   # condition 1
    assert sum(r["excluded_total"] for r in control) == 1
    assert check_silver_guard(spark, cfg) == "resume"
    # Status v2 + catch-up rule against REAL Delta source offsets.
    status = read_status(cfg.status_path)
    assert status is not None and status["source"] == "delta"
    snap = catchup.delta_snapshot_from_status(status)
    bronze_v = current_version(cfg.bronze_path)
    from common.delta_log import table_id
    result = catchup.check_silver_caught_up(snap, table_id(cfg.bronze_path), bronze_v)
    assert result.caught_up, (status["last_batch"], bronze_v, result.reasons)


def test_checkpoint_reset_rebuilds_silver_without_duplicates(spark: SparkSession,
                                                             tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    _seed_like(spark, cfg)
    q1 = _run(spark, cfg)
    before = sorted((r.event_id, r.kafka_offset) for r in _silver(spark, cfg).collect())
    shutil.rmtree(cfg.checkpoint_path)                       # reset the silver checkpoint
    assert check_silver_guard(spark, cfg) == "rebuild-from-bronze"   # allowed, WARNING
    q2 = _run(spark, cfg)
    assert q2 != q1
    assert sorted((r.event_id, r.kafka_offset) for r in _silver(spark, cfg).collect()) == before
    assert {r["silver_query_id"] for r in _control(spark, cfg)} == {q1, q2}


def test_silver_guard_refusals_and_crash_state(spark: SparkSession, tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    _seed_like(spark, cfg)
    _run(spark, cfg)
    _bronze(spark, cfg, [(40, "s-9", T0)])
    backup = tmp_path / "control_backup"
    shutil.copytree(cfg.control_path, backup)                 # control at batch C-1
    _run(spark, cfg)
    state = read_checkpoint_state(cfg.checkpoint_path)
    c = state.last_committed
    assert c is not None and c >= 1

    # Legal: crash after the control row, before commits/C  ->  T == C' + 1.
    (Path(cfg.checkpoint_path) / "commits" / str(c)).unlink()
    # Also its checksum file: Spark's re-write of commits/C would collide with it.
    (Path(cfg.checkpoint_path) / "commits" / f".{c}.crc").unlink(missing_ok=True)
    assert check_silver_guard(spark, cfg) == "resume"
    vs, vc = current_version(cfg.table_path), current_version(cfg.control_path)
    _run(spark, cfg)                                          # re-runs batch C: writes nothing
    assert (current_version(cfg.table_path), current_version(cfg.control_path)) == (vs, vc)
    assert _committed_batches(cfg)[-1] == c
    # The replay really ran (commits/C is back) and left exactly one control row for C.
    assert (Path(cfg.checkpoint_path) / "commits" / str(c)).exists()
    ctl = [r["batch_id"] for r in _control(spark, cfg) if r["silver_query_id"] == state.query_id]
    assert ctl.count(c) == 1
    assert sorted(ctl) == list(range(c + 1))
    assert _silver(spark, cfg).where("event_id = 's-9'").count() == 1   # batch C's row, once

    # Control table behind (restored from an older copy): refused.
    live = tmp_path / "control_live"
    shutil.move(cfg.control_path, live)
    shutil.copytree(backup, cfg.control_path)
    with pytest.raises(SilverStartupError, match="behind"):
        check_silver_guard(spark, cfg)
    assert check_silver_guard(spark, replace(cfg, allow_rebuild=True)) == "rebuild"
    shutil.rmtree(cfg.control_path)
    shutil.move(live, cfg.control_path)
    assert check_silver_guard(spark, cfg) == "resume"

    # Control table missing: refused.
    shutil.move(cfg.control_path, live)
    with pytest.raises(SilverStartupError, match="no txn for this query id"):
        check_silver_guard(spark, cfg)
    shutil.move(live, cfg.control_path)

    # Silver table missing while the checkpoint has batches: refused.
    shutil.move(cfg.table_path, tmp_path / "silver_live")
    with pytest.raises(SilverStartupError, match="silver table .* is missing"):
        check_silver_guard(spark, cfg)


@fast
def test_one_failing_query_stops_the_other(spark: SparkSession, tmp_path: Path) -> None:
    """D2.3 failure policy: supervise() stops every query when one fails, exit 1."""
    from streaming.pipeline import supervise

    def boom(df: DataFrame, batch_id: int) -> None:
        raise RuntimeError("silver broke")

    rate = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    ok = (rate.writeStream.queryName("ok").format("noop")
          .option("checkpointLocation", str(tmp_path / "cp_ok")).start())
    bad = (rate.writeStream.queryName("bad").foreachBatch(boom)
           .option("checkpointLocation", str(tmp_path / "cp_bad")).start())
    assert supervise([ok, bad], lambda: False, poll_s=0.5) == 1
    assert not ok.isActive and not bad.isActive
    assert "silver broke" in str(bad.exception())


# --- verify --layer silver ------------------------------------------------------------------

def test_verify_silver_passes_then_catches_duplicates_and_missing(spark: SparkSession,
                                                                  tmp_path: Path) -> None:
    from streaming.verify_manifest import evaluate_silver, verify_silver

    cfg = _cfg(tmp_path)
    _seed_like(spark, cfg)
    _run(spark, cfg)

    def check(ids: list[str]) -> tuple[bool, list[str], dict[str, Any]]:
        rep = verify_silver(spark, _manifest(tmp_path, ids), cfg.table_path, cfg.late_path,
                            cfg.control_path, cfg.checkpoint_path, cfg.bronze_path, 7)
        failed, notes = evaluate_silver(rep)
        return failed, notes, rep

    failed, notes, rep = check(["s-1", "s-2", "s-3"])
    assert not failed, notes
    cons = rep["consistency"]
    assert cons["checkable"] and cons["rows_checkable"]
    assert (cons["bronze_rows"], cons["bronze_keyed_distinct_ids"], cons["silver_rows_upto_batch"],
            cons["bronze_keyless_rows"], cons["control_excluded_total"]) == (7, 3, 3, 1, 1)
    assert (cons["late_rows_upto_batch"], rep["late_found_ids"], rep["in_both_ids"]) == (0, 0, 0)

    failed, notes, _ = check(["s-1", "s-404"])
    assert failed and any("R2" in n and "neither silver nor late" in n for n in notes)

    # A second copy of s-1 sneaks into silver (appendOnly allows appends).
    _silver(spark, cfg).where("event_id = 's-1'").write.format("delta").mode("append") \
        .save(cfg.table_path)
    failed, notes, rep = check(["s-1", "s-2", "s-3"])
    assert failed
    assert rep["duplicate_event_ids"] == 1 and rep["duplicate_rows"] == 1
    assert any("R3" in n for n in notes) and any("!= distinct keyed" in n for n in notes)


@fast
def test_verify_silver_reports_missing_table(spark: SparkSession, tmp_path: Path) -> None:
    from streaming.verify_manifest import evaluate_silver, verify_silver

    cfg = _cfg(tmp_path)
    rep = verify_silver(spark, _manifest(tmp_path, ["a"]), cfg.table_path, cfg.late_path,
                        cfg.control_path, cfg.checkpoint_path, cfg.bronze_path, 7)
    assert evaluate_silver(rep) == (True, [f"silver table {cfg.table_path} does not exist"])
