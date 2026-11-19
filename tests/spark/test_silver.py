"""Silver: D9 key check, in-batch dedupe, insert-only MERGE, control rows, guard, pruning.

Runs inside the Spark image (make test-spark; tests marked spark_fast also run in
make test-spark-fast). Every table and checkpoint lives in pytest's tmp_path.
"""

from __future__ import annotations

import shutil
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common import spark_rest  # noqa: E402
from common.delta_log import current_version, live_files_in_range, read_commits  # noqa: E402
from e2e import catchup  # noqa: E402
from simulator.events import EventType, GameEvent  # noqa: E402
from streaming.bronze import bronze_schema, ensure_bronze_table, to_raw_rows  # noqa: E402
from streaming.config import SilverJobConfig  # noqa: E402
from streaming.progress import ProgressLogger  # noqa: E402
from streaming.silver import (  # noqa: E402
    SilverStartupError,
    batch_counts,
    check_silver_guard,
    dedupe_in_batch,
    ensure_silver_tables,
    prepare_silver,
    process_batch,
    start_silver_query,
)
from streaming.silver_merge import MergeBoundsError  # noqa: E402
from streaming.start_guard import read_checkpoint_state  # noqa: E402
from streaming.status import read_status  # noqa: E402

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)
T0 = datetime(2026, 11, 19, 12, 0, 0, tzinfo=UTC)
fast = pytest.mark.spark_fast


# --- helpers ----------------------------------------------------------------------------

def _value(event_id: str, when: datetime, seq: int = 1) -> bytes:
    return GameEvent(
        event_id=event_id, event_type=EventType.ITEM_PURCHASE, event_time=when,
        match_id="m-1", match_seq=seq, patch_version="14.3", region="EUW",
        player_id="p_000001", champion_id="ch_quillon", item_id="it_stormglass", gold=100,
    ).to_json_bytes()


def _bronze_rows(spark: SparkSession, spec: list[tuple[int, str | None, datetime]],
                 partition: int = 0) -> DataFrame:
    """Bronze-shaped rows. spec: (offset, event_id or None = malformed message, event_time)."""
    rows = [(b"m-1", _value(eid, ts, i + 1) if eid else b"{not json", "game-events",
             partition, off, datetime(2026, 11, 19, 12, 0, 1))
            for i, (off, eid, ts) in enumerate(spec)]
    return to_raw_rows(spark.createDataFrame(rows, KAFKA_SCHEMA))


def _cfg(tmp_path: Path, **kw: Any) -> SilverJobConfig:
    return SilverJobConfig(
        bronze_path=str(tmp_path / "bronze"), table_path=str(tmp_path / "silver"),
        control_path=str(tmp_path / "control"), checkpoint_path=str(tmp_path / "cp_silver"),
        status_path="", **kw)


def _bronze(spark: SparkSession, cfg: SilverJobConfig,
            spec: list[tuple[int, str | None, datetime]], partition: int = 0) -> None:
    """Create the bronze table if needed and append one commit of rows."""
    empty = spark.createDataFrame([], KAFKA_SCHEMA)
    ensure_bronze_table(spark, cfg.bronze_path, bronze_schema(empty))
    if spec:
        _bronze_rows(spark, spec, partition).coalesce(1).write.format("delta") \
            .mode("append").save(cfg.bronze_path)


def _ready(spark: SparkSession, tmp_path: Path, **kw: Any) -> SilverJobConfig:
    cfg = _cfg(tmp_path, **kw)
    _bronze(spark, cfg, [])
    ensure_silver_tables(spark, cfg)
    return cfg


def _silver(spark: SparkSession, cfg: SilverJobConfig) -> DataFrame:
    return spark.read.format("delta").load(cfg.table_path)


def _control(spark: SparkSession, cfg: SilverJobConfig) -> list[dict[str, Any]]:
    return [r.asDict() for r in spark.read.format("delta").load(cfg.control_path)
            .orderBy("silver_query_id", "batch_id").collect()]


def _run(spark: SparkSession, cfg: SilverJobConfig) -> str:
    """One availableNow run of the real silver query. Returns the query id."""
    assert prepare_silver(spark, cfg) in {"fresh", "resume", "rebuild-from-bronze"}
    q = start_silver_query(spark, cfg, trigger={"availableNow": True})
    q.awaitTermination(180)
    assert q.exception() is None, q.exception()
    return str(q.id)


def _committed_batches(cfg: SilverJobConfig) -> list[int]:
    commits = Path(cfg.checkpoint_path) / "commits"
    return sorted(int(f.name) for f in commits.iterdir() if f.name.isdigit())


# --- pure parts (fast) ------------------------------------------------------------------

@fast
def test_batch_counts_per_reason_and_bounds_from_keyed_rows_only(spark: SparkSession) -> None:
    schema = "parse_ok boolean, event_id string, event_time timestamp"
    late = datetime(2027, 1, 1, tzinfo=UTC)
    df = spark.createDataFrame([
        (True, "a", T0), (True, "a", T0 + timedelta(seconds=64)), (True, "b", T0),
        (False, None, None),        # unparsed + null id + null time
        (True, None, late),         # null id only (would widen the bounds if counted)
        (True, "c", None),          # null time only
        (None, "d", late),          # parse_ok NULL counts as unparsed
    ], schema)
    c = batch_counts(df)
    assert (c.rows_in, c.excluded_unparsed, c.excluded_null_event_id,
            c.excluded_null_event_time, c.excluded_total) == (7, 2, 2, 2, 4)
    assert (c.keyed_rows, c.keyed_unique, c.dup_in_batch) == (3, 2, 1)
    assert c.min_date == c.max_date == date(2026, 11, 19)


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
def test_cross_batch_duplicate_is_not_inserted_again(spark: SparkSession,
                                                     tmp_path: Path) -> None:
    cfg = _ready(spark, tmp_path)
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
    """If min/max were ever None despite keyed rows, the batch fails and writes nothing."""
    from streaming import silver

    cfg = _ready(spark, tmp_path)
    real = silver.batch_counts
    monkeypatch.setattr(silver, "batch_counts",
                        lambda df: replace(real(df), min_date=None))
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
                                                              tmp_path: Path) -> None:
    """Documented limit (parent plan D2): copies more than H days apart both land."""
    cfg = _ready(spark, tmp_path, dedupe_horizon_days=0)
    process_batch(_bronze_rows(spark, [(1, "e-1", T0)]), 0, cfg, "q-1")
    process_batch(_bronze_rows(spark, [(2, "e-1", T0 + timedelta(days=1))]), 1, cfg, "q-1")
    assert _silver(spark, cfg).where("event_id = 'e-1'").count() == 2


@fast
def test_merge_prunes_target_partitions(spark: SparkSession, tmp_path: Path) -> None:
    """Pruning evidence (D2.3): Spark's scan metrics of the MERGE read exactly the
    files the Delta log says are in [lo, hi], and fewer than all files."""
    cfg = _ready(spark, tmp_path, dedupe_horizon_days=0)
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


# --- streaming: control row per batch, restart, guard (slower) ------------------------------

def _seed_like(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """Two bronze commits: 3 ids sent twice (second copy +64 s, later offsets), 1 keyless."""
    _bronze(spark, cfg, [(8, "s-1", T0), (9, "s-2", T0), (10, "s-3", T0), (11, None, T0)])
    _bronze(spark, cfg, [(14, "s-1", T0 + timedelta(seconds=64)),
                         (15, "s-2", T0 + timedelta(seconds=64)),
                         (16, "s-3", T0 + timedelta(seconds=64))])


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

def _manifest(tmp_path: Path, ids: list[str]) -> Path:
    import json

    path = tmp_path / "run.json"
    path.write_text(json.dumps({
        "manifest_version": 1, "run_id": "r1", "seed": 501, "topic": "game-events",
        "totals": {"matches": 1, "events_generated": len(ids), "events_delivered": len(ids),
                   "events_failed": 0},
        "matches": {"m-1": {"events_generated": len(ids), "events_delivered": len(ids),
                            "max_match_seq": len(ids), "partitions": [0], "event_ids": ids}},
        "failed_event_ids": []}))
    return path


def test_verify_silver_passes_then_catches_duplicates_and_missing(spark: SparkSession,
                                                                  tmp_path: Path) -> None:
    from streaming.verify_manifest import evaluate_silver, verify_silver

    cfg = _cfg(tmp_path)
    _seed_like(spark, cfg)
    _run(spark, cfg)

    def check(ids: list[str]) -> tuple[bool, list[str], dict[str, Any]]:
        rep = verify_silver(spark, _manifest(tmp_path, ids), cfg.table_path, cfg.control_path,
                            cfg.checkpoint_path, cfg.bronze_path, 7)
        failed, notes = evaluate_silver(rep)
        return failed, notes, rep

    failed, notes, rep = check(["s-1", "s-2", "s-3"])
    assert not failed, notes
    cons = rep["consistency"]
    assert cons["checkable"] and cons["rows_checkable"]
    assert (cons["bronze_rows"], cons["bronze_keyed_distinct_ids"], cons["silver_rows_upto_batch"],
            cons["bronze_keyless_rows"], cons["control_excluded_total"]) == (7, 3, 3, 1, 1)

    failed, notes, _ = check(["s-1", "s-404"])
    assert failed and any("missing in silver" in n for n in notes)

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
    rep = verify_silver(spark, _manifest(tmp_path, ["a"]), cfg.table_path, cfg.control_path,
                        cfg.checkpoint_path, cfg.bronze_path, 7)
    assert evaluate_silver(rep) == (True, [f"silver table {cfg.table_path} does not exist"])
