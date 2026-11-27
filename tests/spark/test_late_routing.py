"""Silver late routing (slice 4, DECISIONS D2.4): watermark, late table, control columns.

Runs inside the Spark image (make test-spark; tests marked spark_fast also run in
make test-spark-fast). Every table and checkpoint lives in pytest's tmp_path.
Every test asserts exact sets and counts; none asserts a fraction.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common.delta_log import current_version  # noqa: E402
from simulator.events import EventType, GameEvent  # noqa: E402
from streaming.bronze import bronze_schema, ensure_bronze_table, to_raw_rows  # noqa: E402
from streaming.config import SilverJobConfig  # noqa: E402
from streaming.silver import (  # noqa: E402
    CONTROL_SCHEMA,
    SLICE3_CONTROL_FIELDS,
    SilverStartupError,
    check_silver_guard,
    ensure_silver_tables,
    plan_batch,
    prepare_silver,
    process_batch,
    start_silver_query,
    write_control_row,
    write_late,
    write_silver,
)
from streaming.watermark import SnapshotStatusError, SplitError  # noqa: E402

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)
T0 = datetime(2026, 11, 19, 12, 0, 0, tzinfo=UTC)
W = timedelta(seconds=60)
MS = timedelta(milliseconds=1)
fast = pytest.mark.spark_fast
# Control fields that legitimately differ per attempt (C2, plus processed_at: see report;
# Phase 3 plan 2.5: the *_already_present terms and the DLQ, D_open and cut timings).
ATTEMPT_FIELDS = {"inserted_this_attempt", "late_inserted_this_attempt", "merge_ms", "late_ms",
                  "wm_read_ms", "processed_at", "dlq_keyless_inserted_this_attempt",
                  "dlq_keyless_already_present", "dlq_keyed_inserted_this_attempt",
                  "dlq_keyed_already_present", "late_already_present",
                  "ontime_already_present", "dlq_ms", "d_open_read_ms", "cut_ms"}
Row = tuple[int, str | None, datetime, datetime | None]   # offset, id (None = bad), time, kafka


# --- helpers ----------------------------------------------------------------------------

def _value(event_id: str, when: datetime) -> bytes:
    return GameEvent(
        event_id=event_id, event_type=EventType.ITEM_PURCHASE, event_time=when,
        match_id="m-1", match_seq=1, patch_version="14.3", region="EUW",
        player_id="p_000001", champion_id="ch_quillon", item_id="it_stormglass", gold=100,
    ).to_json_bytes()


def r(offset: int, event_id: str | None, when: datetime, kafka: Any = "auto") -> Row:
    """A bronze row; the Kafka time defaults to event_time + 1 s (no clamp)."""
    return (offset, event_id, when, when + timedelta(seconds=1) if kafka == "auto" else kafka)


def _batch(spark: SparkSession, rows: list[Row]) -> DataFrame:
    data = [(b"m-1", _value(e, t) if e else b"{not json", "game-events", 0, off, k)
            for off, e, t, k in rows]
    return to_raw_rows(spark.createDataFrame(data, KAFKA_SCHEMA))


def _cfg(tmp_path: Path, **kw: Any) -> SilverJobConfig:
    return SilverJobConfig(
        bronze_path=str(tmp_path / "bronze"), table_path=str(tmp_path / "silver"),
        control_path=str(tmp_path / "control"), checkpoint_path=str(tmp_path / "cp"),
        late_path=str(tmp_path / "late"), status_path="", **kw)


def _empty_bronze(spark: SparkSession, cfg: SilverJobConfig) -> None:
    ensure_bronze_table(spark, cfg.bronze_path,
                        bronze_schema(spark.createDataFrame([], KAFKA_SCHEMA)))


def _ready(spark: SparkSession, tmp_path: Path, tail_offsets: Any, n_tail: int = 10,
           **kw: Any) -> SilverJobConfig:
    """Tables created; offsets/0..n_tail-1 are tail offsets, so batches 1..n_tail are
    not snapshot batches (batch 0 always is)."""
    cfg = _cfg(tmp_path, **kw)
    _empty_bronze(spark, cfg)
    ensure_silver_tables(spark, cfg)
    tail_offsets(cfg.checkpoint_path, n_tail)
    return cfg


def _commit(spark: SparkSession, cfg: SilverJobConfig, rows: list[Row]) -> None:
    """One bronze commit, one file."""
    _empty_bronze(spark, cfg)
    _batch(spark, rows).coalesce(1).write.format("delta").mode("append").save(cfg.bronze_path)


def _rows(spark: SparkSession, path: str) -> dict[str, Any]:
    return {x.event_id: x for x in spark.read.format("delta").load(path).collect()}


def _control(spark: SparkSession, cfg: SilverJobConfig) -> list[dict[str, Any]]:
    return [x.asDict() for x in spark.read.format("delta").load(cfg.control_path)
            .orderBy("silver_query_id", "batch_id").collect()]


def _naive(t: datetime) -> datetime:
    return t.astimezone(UTC).replace(tzinfo=None)   # how collect() returns timestamps


def _check_disjoint_union(spark: SparkSession, cfg: SilverJobConfig, sent: set[str]) -> None:
    """R1-R3 over the whole tables: S ∩ L = ∅, S ∪ L == sent, no id twice."""
    s = [x.event_id for x in spark.read.format("delta").load(cfg.table_path).collect()]
    lt = [x.event_id for x in spark.read.format("delta").load(cfg.late_path).collect()]
    assert len(s) == len(set(s)) and len(lt) == len(set(lt)), (s, lt)
    assert set(s) & set(lt) == set(), (s, lt)
    assert set(s) | set(lt) == sent


def _versions(cfg: SilverJobConfig) -> tuple[int | None, ...]:
    return (current_version(cfg.table_path), current_version(cfg.late_path),
            current_version(cfg.control_path))


# --- pure-ish rules (fast) ------------------------------------------------------------------

@fast
def test_least_skips_null_which_is_why_the_clamp_checks_kafka_time(spark: SparkSession) -> None:
    """Pins Spark 4.1.4: least() skips NULL arguments (plan section 13)."""
    got = spark.sql("SELECT least(CAST(NULL AS TIMESTAMP), "
                    "TIMESTAMP'2026-11-19 12:00:00') AS v").first()["v"]
    assert got == datetime(2026, 11, 19, 12, 0, 0)


@fast
def test_first_batch_routes_nothing_and_records_why(spark: SparkSession, tmp_path: Path,
                                                    tail_offsets: Any) -> None:
    """NULL trap: wm = -infinity in batch 0; every keyed row reaches silver (A3)."""
    cfg = _ready(spark, tmp_path, tail_offsets)
    row = process_batch(_batch(spark, [r(1, "a", T0), r(2, "b", T0 - timedelta(days=1))]),
                        0, cfg, "q-1")
    assert (row["in_initial_snapshot"], row["watermark_used"]) == (True, None)
    assert (row["late_rows"], row["ontime_rows"], row["inserted_this_attempt"]) == (0, 2, 2)
    assert row["watermark_delay_s"] == 60 and row["late_ms"] == 0
    assert set(_rows(spark, cfg.table_path)) == {"a", "b"} and not _rows(spark, cfg.late_path)
    assert current_version(cfg.late_path) == 0        # no late rows: no late commit


@fast
def test_boundary_wm_minus_1ms_and_wm_are_late_wm_plus_1ms_is_on_time(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(_batch(spark, [r(1, "m", T0)]), 0, cfg, "q-1")
    wm = T0 - W
    row = process_batch(_batch(spark, [r(2, "a", wm - MS), r(3, "b", wm), r(4, "c", wm + MS)]),
                        1, cfg, "q-1")
    assert row["in_initial_snapshot"] is False and row["watermark_used"] == wm
    assert (row["late_rows"], row["ontime_rows"], row["late_inserted_this_attempt"],
            row["inserted_this_attempt"]) == (2, 1, 2, 1)
    late = _rows(spark, cfg.late_path)
    assert set(late) == {"a", "b"} and set(_rows(spark, cfg.table_path)) == {"m", "c"}
    assert (late["a"].lag_behind_wm_ms, late["b"].lag_behind_wm_ms) == (1, 0)
    assert late["a"].watermark_used == _naive(wm) and late["a"].silver_batch_id == 1


@fast
def test_future_dated_row_moves_the_wm_only_to_its_kafka_time(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    row0 = process_batch(_batch(spark, [r(1, "f", T0 + timedelta(hours=1), kafka=T0)]),
                         0, cfg, "q-1")
    assert (row0["clamped_rows"], row0["batch_max_clamped_event_time"]) == (1, T0)
    # Unclamped, wm would be T0 + 59 min and x would be late.
    row1 = process_batch(_batch(spark, [r(2, "x", T0 - timedelta(seconds=30))]), 1, cfg, "q-1")
    assert row1["watermark_used"] == T0 - W and row1["late_rows"] == 0
    assert set(_rows(spark, cfg.table_path)) == {"f", "x"}


@fast
def test_rows_without_kafka_time_or_key_do_not_move_the_wm(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    row0 = process_batch(_batch(spark, [
        r(1, "k", T0),
        r(2, "n", T0 + timedelta(hours=1), kafka=None),          # keyed, no Kafka time
        r(3, None, T0, kafka=T0 + timedelta(days=1)),            # keyless, far-future Kafka
    ]), 0, cfg, "q-1")
    assert row0["batch_max_clamped_event_time"] == T0
    assert (row0["excluded_total"], row0["keyed_unique"], row0["clamped_rows"]) == (1, 2, 0)
    row1 = process_batch(_batch(spark, [r(4, "x", T0 - timedelta(seconds=30))]), 1, cfg, "q-1")
    assert row1["watermark_used"] == T0 - W and row1["late_rows"] == 0


@fast
def test_late_and_silver_merge_bounds_are_separate(spark: SparkSession, tmp_path: Path,
                                                   tail_offsets: Any) -> None:
    """One old late row never widens silver's MERGE window (D9, amendment A)."""
    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(_batch(spark, [r(1, "m", T0)]), 0, cfg, "q-1")
    old = T0 - timedelta(days=30)
    row = process_batch(_batch(spark, [r(2, "old", old), r(3, "n", T0)]), 1, cfg, "q-1")
    assert (row["merge_lo"], row["merge_hi"], row["merge_window_days"]) == (
        date(2026, 11, 12), date(2026, 11, 26), 15)
    assert (row["late_merge_lo"], row["late_merge_hi"]) == (date(2026, 10, 13),
                                                            date(2026, 10, 27))
    assert set(_rows(spark, cfg.late_path)) == {"old"}


# --- silver and late stay disjoint (section 4) ------------------------------------------------

LATE_T, ON_T = T0 - timedelta(hours=1), T0 + timedelta(seconds=10)
DISJOINT_CASES = {
    # name: (batch 1 rows, batch 2 rows, expected late ids, checks on the batch rows)
    "late first, on-time copy later": (
        [r(5, "x", LATE_T)], [r(9, "x", ON_T)], {"x"},
        lambda b1, b2: b2["dup_of_late"] == 1 and b2["inserted_this_attempt"] == 0),
    "on-time first, late copy later": (
        [r(5, "x", T0)], [r(9, "x", LATE_T)], set(),
        lambda b1, b2: b2["late_dup_of_silver"] == 1 and b2["late_inserted_this_attempt"] == 0),
    "one batch, late copy has the lower offset": (
        [r(5, "x", LATE_T), r(6, "x", ON_T)], [], {"x"},
        lambda b1, b2: b1["dup_in_batch"] == 1 and b1["late_rows"] == 1),
    "one batch, on-time copy has the lower offset": (
        [r(5, "x", ON_T), r(6, "x", LATE_T)], [], set(),
        lambda b1, b2: b1["dup_in_batch"] == 1 and b1["ontime_rows"] == 1),
    "late copy in two batches": (
        [r(5, "x", LATE_T)], [r(9, "x", LATE_T)], {"x"},
        lambda b1, b2: (b2["late_rows"], b2["late_inserted_this_attempt"]) == (1, 0)),
    "on-time copy in two batches": (
        [r(5, "x", ON_T)], [r(9, "x", ON_T)], set(),
        lambda b1, b2: b2["inserted_this_attempt"] == 0),
}


@fast
@pytest.mark.parametrize("case", list(DISJOINT_CASES))
def test_silver_and_late_stay_disjoint(spark: SparkSession, tmp_path: Path,
                                       tail_offsets: Any, case: str) -> None:
    b1, b2, expected_late, check = DISJOINT_CASES[case]
    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(_batch(spark, [r(1, "m", T0)]), 0, cfg, "q-1")
    row1 = process_batch(_batch(spark, b1), 1, cfg, "q-1")
    row2 = process_batch(_batch(spark, b2), 2, cfg, "q-1")
    assert set(_rows(spark, cfg.late_path)) == expected_late
    assert check(row1, row2), (row1, row2)
    _check_disjoint_union(spark, cfg, {"m", "x"})


# --- failures before any write ------------------------------------------------------------------

@fast
def test_split_check_fails_the_batch_before_any_write(spark: SparkSession, tmp_path: Path,
                                                      tail_offsets: Any,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    """C1: late + on-time != keyed_unique -> SplitError; no table gets a commit."""
    from streaming import silver

    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(_batch(spark, [r(1, "m", T0)]), 0, cfg, "q-1")
    real = silver.split_counts
    monkeypatch.setattr(silver, "split_counts",
                        lambda rows: replace(real(rows), ontime=real(rows).ontime - 1))
    before = _versions(cfg)
    with pytest.raises(SplitError, match="no table"):
        process_batch(_batch(spark, [r(2, "a", T0 - timedelta(hours=1)), r(3, "b", T0)]),
                      1, cfg, "q-1")
    assert _versions(cfg) == before


@fast
def test_unreadable_snapshot_status_fails_the_batch_before_any_write(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    """A2 inside a batch: offsets/0 without isStartingVersion -> SnapshotStatusError."""
    cfg = _ready(spark, tmp_path, tail_offsets, n_tail=0)
    process_batch(_batch(spark, [r(1, "m", T0)]), 0, cfg, "q-1")
    off = Path(cfg.checkpoint_path) / "offsets"
    off.mkdir(parents=True, exist_ok=True)
    (off / "0").write_text('v1\n{}\n{"reservoirVersion":1,"index":-1}', encoding="utf-8")
    before = _versions(cfg)
    with pytest.raises(SnapshotStatusError, match="isStartingVersion is missing"):
        process_batch(_batch(spark, [r(2, "a", T0)]), 1, cfg, "q-1")
    assert _versions(cfg) == before


@fast
def test_snapshot_status_error_makes_the_pipeline_exit_2(spark: SparkSession,
                                                         tmp_path: Path) -> None:
    from streaming.pipeline import supervise

    def boom(df: DataFrame, batch_id: int) -> None:
        raise SnapshotStatusError("offsets/4 is gone")

    rate = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    ok = (rate.writeStream.queryName("ok2").format("noop")
          .option("checkpointLocation", str(tmp_path / "cp_ok")).start())
    bad = (rate.writeStream.queryName("bad2").foreachBatch(boom)
           .option("checkpointLocation", str(tmp_path / "cp_bad")).start())
    try:
        assert supervise([ok, bad], lambda: False, poll_s=0.5) == 2
        assert not ok.isActive and not bad.isActive
    finally:
        # The session is shared: forget the failed query, or a later test's
        # awaitAnyTermination() re-raises its exception.
        spark.streams.resetTerminated()


# --- crash points (section 3, C2) -----------------------------------------------------------

CRASH_B0 = [r(1, "m", T0), r(2, "y", T0)]
CRASH_B1 = [r(3, "z", T0 - timedelta(hours=2))]                      # z -> late
CRASH_B2 = [r(4, "l1", T0 - timedelta(hours=1)),                     # new late
            r(5, "y", T0 - timedelta(hours=1)),                      # late copy of silver's y
            r(6, "o1", ON_T), r(7, "o1", ON_T + timedelta(seconds=5)),   # on-time + in-batch dup
            r(8, "z", ON_T),                                         # on-time copy of late z
            r(9, None, T0)]                                          # keyless
CRASH_IDS = {"m", "y", "z", "l1", "o1"}


def _frozen(spark: SparkSession, rows: list[Row], path: Path) -> DataFrame:
    """Bronze rows written once and read back: every read is the same data (as a
    replay re-reads the same bronze range); _batch() would stamp a new ingest_time."""
    if not path.exists():
        _batch(spark, rows).write.format("delta").save(str(path))
    return spark.read.format("delta").load(str(path))


def _crash_setup(spark: SparkSession, tmp_path: Path, frozen: Path,
                 tail_offsets: Any) -> SilverJobConfig:
    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(_frozen(spark, CRASH_B0, frozen / "b0"), 0, cfg, "q-1")
    process_batch(_frozen(spark, CRASH_B1, frozen / "b1"), 1, cfg, "q-1")
    return cfg


def _table_content(spark: SparkSession, path: str) -> list[tuple[Any, ...]]:
    df = spark.read.format("delta").load(path).drop("silver_inserted_at")
    return sorted(tuple(x) for x in df.collect())


@pytest.mark.parametrize("crash_after", ["plan", "late write", "silver write", "control row"])
def test_any_crash_point_then_replay_equals_a_crash_free_run(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any, crash_after: str) -> None:
    fz = tmp_path / "frozen"
    b2 = lambda: _frozen(spark, CRASH_B2, fz / "b2")  # noqa: E731
    ref = _crash_setup(spark, tmp_path / "ref", fz, tail_offsets)
    ref_row = process_batch(b2(), 2, ref, "q-1")
    assert (ref_row["late_rows"], ref_row["late_dup_of_silver"], ref_row["ontime_rows"],
            ref_row["dup_of_late"], ref_row["dup_in_batch"], ref_row["excluded_total"]) == (
        2, 1, 2, 1, 1, 1)

    cfg = _crash_setup(spark, tmp_path / "crash", fz, tail_offsets)
    started = datetime.now(UTC)
    if crash_after == "control row":
        process_batch(b2(), 2, cfg, "q-1")
    else:
        plan = plan_batch(b2(), 2, cfg, "q-1", started)
        if crash_after in ("late write", "silver write"):
            write_late(spark, cfg, plan)
        if crash_after == "silver write":
            write_silver(spark, cfg, plan)
        plan.release()
    replay = process_batch(b2(), 2, cfg, "q-1")                        # Spark re-runs batch 2

    rows = [x for x in _control(spark, cfg) if x["batch_id"] == 2]
    assert len(rows) == 1                                              # exactly one row for N
    stored = rows[0]
    ref_stored = next(x for x in _control(spark, ref) if x["batch_id"] == 2)
    keys = set(stored) - ATTEMPT_FIELDS
    assert {k: stored[k] for k in keys} == {k: ref_stored[k] for k in keys}
    assert _table_content(spark, cfg.table_path) == _table_content(spark, ref.table_path)
    assert _table_content(spark, cfg.late_path) == _table_content(spark, ref.late_path)
    _check_disjoint_union(spark, cfg, CRASH_IDS)
    # What the replay attempt itself inserted depends on where the crash was.
    expected_attempt = {"plan": (1, 1), "late write": (1, 0), "silver write": (0, 0),
                        "control row": (0, 0)}[crash_after]
    assert (replay["inserted_this_attempt"], replay["late_inserted_this_attempt"]) == \
        expected_attempt


# --- the real streaming query: snapshot batches (A1, A4), reset, restart ---------------------

def _start(spark: SparkSession, cfg: SilverJobConfig, trigger: dict[str, Any]) -> Any:
    assert prepare_silver(spark, cfg) in {"fresh", "resume", "rebuild-from-bronze"}
    return start_silver_query(spark, cfg, trigger=trigger)


def _run(spark: SparkSession, cfg: SilverJobConfig) -> str:
    q = _start(spark, cfg, {"availableNow": True})
    q.awaitTermination(300)
    assert q.exception() is None, q.exception()
    return str(q.id)


def _run_one_batch_then_stop(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """processingTime 1 hour: batch 0 runs at once, the next one would wait an hour."""
    q = _start(spark, cfg, {"processingTime": "1 hour"})
    commit0 = Path(cfg.checkpoint_path) / "commits" / "0"
    deadline = time.monotonic() + 180
    while not commit0.exists():
        assert q.exception() is None and time.monotonic() < deadline
        time.sleep(0.2)
    q.stop()


def _offset(cfg: SilverJobConfig, n: int) -> dict[str, Any]:
    text = (Path(cfg.checkpoint_path) / "offsets" / str(n)).read_text(encoding="utf-8")
    return json.loads(text.splitlines()[2])


def _uncommit(cfg: SilverJobConfig, n: int) -> None:
    """Crash after the sink, before commits/n (and its checksum)."""
    (Path(cfg.checkpoint_path) / "commits" / str(n)).unlink()
    (Path(cfg.checkpoint_path) / "commits" / f".{n}.crc").unlink(missing_ok=True)


def _drop_newest_control_commit(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """Undo the newest control commit of a TEST table: crash before the control row."""
    log = Path(cfg.control_path) / "_delta_log"
    v = current_version(cfg.control_path)
    assert v is not None and v % 10 != 0       # no Delta checkpoint at that version
    for name in (f"{v:020d}.json", f".{v:020d}.json.crc", f"{v:020d}.crc"):
        (log / name).unlink(missing_ok=True)
    # Delta caches table snapshots per path in the JVM; forget the dropped version.
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()  # type: ignore[union-attr]


def test_snapshot_batches_route_nothing_and_a_replayed_one_is_identical(
        spark: SparkSession, tmp_path: Path) -> None:
    """A1 (5 files, cap 2) and A4 (re-run a snapshot batch after deleting commits/N).

    Each file holds a recent row and a day-old row, so ANY batch after the first
    would route rows late if the snapshot rule did not hold.
    """
    cfg = _cfg(tmp_path, max_files_per_trigger=2)
    for i in range(5):
        _commit(spark, cfg, [r(2 * i, f"r-{i}", T0 + timedelta(seconds=i)),
                             r(2 * i + 1, f"a-{i}", T0 - timedelta(days=1, seconds=i))])
    _run(spark, cfg)
    ctl = _control(spark, cfg)
    assert [(x["batch_id"], x["rows_in"]) for x in ctl] == [(0, 4), (1, 4), (2, 2)]
    assert all(x["in_initial_snapshot"] and x["watermark_used"] is None
               and x["late_rows"] == 0 for x in ctl)
    # A1, the real shape: the last snapshot batch ENDS outside the snapshot.
    assert [_offset(cfg, n)["isStartingVersion"] for n in range(3)] == [True, True, False]
    assert not _rows(spark, cfg.late_path)
    sent = {f"r-{i}" for i in range(5)} | {f"a-{i}" for i in range(5)}
    assert set(_rows(spark, cfg.table_path)) == sent

    # A4: crash after batch 2's silver write, before its control row and commits/2.
    original = next(x for x in ctl if x["batch_id"] == 2)
    _uncommit(cfg, 2)
    _drop_newest_control_commit(spark, cfg)
    assert [x["batch_id"] for x in _control(spark, cfg)] == [0, 1]
    assert check_silver_guard(spark, cfg) == "resume"
    _run(spark, cfg)
    rows2 = [x for x in _control(spark, cfg) if x["batch_id"] == 2]
    assert len(rows2) == 1
    keys = set(original) - ATTEMPT_FIELDS
    assert {k: rows2[0][k] for k in keys} == {k: original[k] for k in keys}
    assert rows2[0]["watermark_used"] is None and rows2[0]["inserted_this_attempt"] == 0
    _check_disjoint_union(spark, cfg, sent)


def test_snapshot_ending_mid_batch_with_a_tail_file(spark: SparkSession, tmp_path: Path) -> None:
    """A1: batch 1 holds the last snapshot file AND a tail file: no wm for it (cost:
    the day-old tail row t-0 stays in silver). Batch 2 is tail only: normal wm."""
    cfg = _cfg(tmp_path, max_files_per_trigger=2)
    for i in range(3):
        _commit(spark, cfg, [r(i, f"r-{i}", T0 + timedelta(seconds=i))])
    _run_one_batch_then_stop(spark, cfg)                     # batch 0: 2 snapshot files
    _commit(spark, cfg, [r(10, "t-0", T0 - timedelta(days=1))])
    _commit(spark, cfg, [r(11, "t-1", T0 - timedelta(days=1))])
    _run(spark, cfg)
    ctl = {x["batch_id"]: x for x in _control(spark, cfg)}
    assert [(n, ctl[n]["rows_in"], ctl[n]["in_initial_snapshot"]) for n in sorted(ctl)] == [
        (0, 2, True), (1, 2, True), (2, 1, False)]
    assert (_offset(cfg, 0)["isStartingVersion"], _offset(cfg, 1)["isStartingVersion"]) == (
        True, False)
    assert ctl[1]["watermark_used"] is None and ctl[1]["late_rows"] == 0
    assert ctl[2]["watermark_used"] == _naive(T0 + timedelta(seconds=2) - W)
    assert set(_rows(spark, cfg.late_path)) == {"t-1"}
    assert set(_rows(spark, cfg.table_path)) == {"r-0", "r-1", "r-2", "t-0"}


def test_snapshot_ending_exactly_at_a_batch_boundary(spark: SparkSession,
                                                     tmp_path: Path) -> None:
    """A1: 4 snapshot files, cap 2: batch 1 ends exactly at the snapshot end."""
    cfg = _cfg(tmp_path, max_files_per_trigger=2)
    for i in range(4):
        _commit(spark, cfg, [r(i, f"r-{i}", T0 + timedelta(seconds=i))])
    _run(spark, cfg)
    _commit(spark, cfg, [r(10, "t-0", T0 - timedelta(days=1))])
    _run(spark, cfg)
    ctl = {x["batch_id"]: x for x in _control(spark, cfg)}
    assert [(n, ctl[n]["rows_in"], ctl[n]["in_initial_snapshot"]) for n in sorted(ctl)] == [
        (0, 2, True), (1, 2, True), (2, 1, False)]
    assert _offset(cfg, 1)["isStartingVersion"] is False
    assert set(_rows(spark, cfg.late_path)) == {"t-0"}


def test_late_rows_replay_restart_recompute_and_checkpoint_reset(spark: SparkSession,
                                                                 tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    _commit(spark, cfg, [r(0, "r-0", T0), r(1, "r-1", T0)])
    q1 = _run(spark, cfg)                                         # batch 0 (snapshot)
    _commit(spark, cfg, [r(2, "a-0", T0 - timedelta(days=1)), r(3, "n-0", ON_T),
                         r(4, None, T0)])
    _run(spark, cfg)                                              # batch 1: a-0 late
    sent = {"r-0", "r-1", "a-0", "n-0"}
    assert set(_rows(spark, cfg.late_path)) == {"a-0"}
    b1 = next(x for x in _control(spark, cfg) if x["batch_id"] == 1)
    assert (b1["in_initial_snapshot"], b1["watermark_used"], b1["late_rows"]) == (
        False, _naive(T0 - W), 1)

    # Streaming replay of a batch WITH late rows (delete commits/1): nothing new.
    before = _versions(cfg)
    _uncommit(cfg, 1)
    _run(spark, cfg)
    assert _versions(cfg) == before
    assert [x["batch_id"] for x in _control(spark, cfg)] == [0, 1]

    # A new reader recomputes every recorded watermark_used from the control table.
    ctl = _control(spark, cfg)
    for x in ctl:
        prior = [y["batch_max_clamped_event_time"] for y in ctl
                 if y["batch_id"] < x["batch_id"] and y["batch_max_clamped_event_time"]]
        expected = None if x["in_initial_snapshot"] or not prior else max(prior) - W
        assert x["watermark_used"] == expected, x

    # Checkpoint reset: new query id; its snapshot routes nothing; a-0 STAYS in late.
    silver_before = _table_content(spark, cfg.table_path)
    late_before = _table_content(spark, cfg.late_path)
    shutil.rmtree(cfg.checkpoint_path)
    assert check_silver_guard(spark, cfg) == "rebuild-from-bronze"
    q2 = _run(spark, cfg)
    assert q2 != q1
    new = [x for x in _control(spark, cfg) if x["silver_query_id"] == q2]
    assert [(x["in_initial_snapshot"], x["watermark_used"], x["late_rows"], x["dup_of_late"],
             x["inserted_this_attempt"]) for x in new] == [(True, None, 0, 1, 0)]
    assert _table_content(spark, cfg.table_path) == silver_before
    assert _table_content(spark, cfg.late_path) == late_before
    _check_disjoint_union(spark, cfg, sent)


# --- upgrade from slice 3 and the new guards (Q1, Q2, C5, A2) ------------------------------

def _slice3_state(spark: SparkSession, cfg: SilverJobConfig, tail_offsets: Any) -> None:
    """What the live system looked like before slice 4: control with the 16 slice 3
    columns and one row (q-old, batch 0), silver, a checkpoint at batch 0, no late."""
    from delta.tables import DeltaTable
    from pyspark.sql.types import StructType

    _commit(spark, cfg, [r(0, "r-0", T0)])
    ensure_silver_tables(spark, replace(cfg, late_path=cfg.late_path + "_tmp"))
    shutil.rmtree(cfg.control_path)
    shutil.rmtree(cfg.late_path + "_tmp")
    old = StructType(CONTROL_SCHEMA.fields[:SLICE3_CONTROL_FIELDS])
    (DeltaTable.createIfNotExists(spark).location(cfg.control_path).addColumns(old)
     .property("delta.appendOnly", "true").execute())
    row: dict[str, Any] = {f.name: None for f in old.fields}
    row.update(silver_query_id="q-old", batch_id=0, rows_in=1, excluded_total=0,
               keyed_unique=1, inserted_this_attempt=1, processed_at=T0)
    (spark.createDataFrame([row], old).write.format("delta").mode("append")
     .option("txnAppId", "q-old").option("txnVersion", 0).save(cfg.control_path))
    cp = Path(cfg.checkpoint_path)
    (cp / "commits").mkdir(parents=True)
    (cp / "metadata").write_text('{"id":"q-old"}\n', encoding="utf-8")
    (cp / "commits" / "0").write_text('v1\n{"nextBatchWatermarkMs":0}\n', encoding="utf-8")
    tail_offsets(cfg.checkpoint_path, 1)


def _pipeline_exit(cfg: SilverJobConfig, tmp_path: Path, **env: str) -> tuple[int, str]:
    """Run the real `python -m streaming.pipeline` against the test tables."""
    full = {**os.environ, "BRONZE_PATH": cfg.bronze_path, "SILVER_PATH": cfg.table_path,
            "SILVER_CONTROL_PATH": cfg.control_path, "SILVER_LATE_PATH": cfg.late_path,
            "SILVER_CHECKPOINT_PATH": cfg.checkpoint_path, "SILVER_STATUS_PATH": "",
            "BRONZE_CHECKPOINT_PATH": str(tmp_path / "cp_bronze"), "STATUS_PATH": "",
            "BRONZE_ALLOW_REBUILD": "1",   # the test bronze has no bronze checkpoint
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:1", "SPARK_MASTER": "local[1]",
            "SPARK_DRIVER_MEMORY": "512m", "DELTA_SNAPSHOT_PARTITIONS": "2", **env}
    res = subprocess.run([sys.executable, "-m", "streaming.pipeline"], env=full,
                         capture_output=True, text=True, timeout=240)
    return res.returncode, res.stdout + res.stderr


def test_upgrade_from_slice3_then_each_new_guard_refuses_with_exit_2(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _cfg(tmp_path)
    _slice3_state(spark, cfg, tail_offsets)
    assert prepare_silver(spark, cfg) == "resume"            # upgrade: ALTER + create late
    cols = spark.read.format("delta").load(cfg.control_path).columns
    assert cols == [f.name for f in CONTROL_SCHEMA.fields]   # ALTER worked on appendOnly
    assert current_version(cfg.late_path) == 0
    # First upgraded batch: the old row has no m_k, so the wm is still -infinity.
    row = process_batch(_batch(spark, [r(1, "x", T0 - timedelta(days=1))]), 1, cfg, "q-old")
    assert (row["in_initial_snapshot"], row["watermark_used"], row["late_rows"]) == (
        False, None, 0)
    (Path(cfg.checkpoint_path) / "commits" / "1").write_text("v1\n{}\n", encoding="utf-8")
    tail_offsets(cfg.checkpoint_path, 2)
    assert check_silver_guard(spark, cfg) == "resume"

    # C5 (1): W = 61 vs recorded 60. Not overridable.
    msg = "WATERMARK_DELAY_S=61 but query q-old recorded watermark_delay_s=60"
    with pytest.raises(SilverStartupError, match=msg):
        check_silver_guard(spark, replace(cfg, watermark_delay_s=61, allow_rebuild=True))
    code, out = _pipeline_exit(cfg, tmp_path, WATERMARK_DELAY_S="61")
    assert code == 2 and msg in out, out[-2000:]

    # A2 at start-up: the next batch's start offset is unreadable. Not overridable.
    good = (Path(cfg.checkpoint_path) / "offsets" / "1").read_text(encoding="utf-8")
    (Path(cfg.checkpoint_path) / "offsets" / "1").write_text("v1\n{}\n", encoding="utf-8")
    with pytest.raises(SilverStartupError, match="SnapshotStatusError"):
        check_silver_guard(spark, replace(cfg, allow_rebuild=True))
    code, out = _pipeline_exit(cfg, tmp_path)
    assert code == 2 and "exactly one source line" in out, out[-2000:]
    (Path(cfg.checkpoint_path) / "offsets" / "1").write_text(good, encoding="utf-8")

    # C5 (2): late table missing after the upgrade.
    shutil.move(cfg.late_path, tmp_path / "late_aside")
    msg = f"late table {cfg.late_path} is missing after the upgrade"
    with pytest.raises(SilverStartupError, match=msg):
        check_silver_guard(spark, cfg)
    code, out = _pipeline_exit(cfg, tmp_path)
    assert code == 2 and msg in out, out[-2000:]

    # C5 (3): an "upgrade" (create an empty late table) although a control row already
    # has watermark_delay_s. Shown with a reset checkpoint, where only this rule applies.
    shutil.move(cfg.checkpoint_path, tmp_path / "cp_aside")
    msg = "control rows of query q-old already have watermark_delay_s"
    with pytest.raises(SilverStartupError, match=msg):
        check_silver_guard(spark, cfg)
    code, out = _pipeline_exit(cfg, tmp_path)
    assert code == 2 and msg in out, out[-2000:]

    # Put everything back: the guard accepts it again.
    shutil.move(tmp_path / "cp_aside", cfg.checkpoint_path)
    shutil.move(tmp_path / "late_aside", cfg.late_path)
    assert check_silver_guard(spark, cfg) == "resume"


@fast
def test_write_control_row_is_skipped_for_a_batch_already_recorded(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    row = process_batch(_batch(spark, [r(1, "m", T0)]), 0, cfg, "q-1")
    write_control_row(spark, cfg.control_path, row)   # same txn (q-1, 0): Delta skips it
    assert len(_control(spark, cfg)) == 1


# --- verify: R1-R3 over silver ∪ late, whole-table union (slice 4, Q3) ----------------------

def _manifest(tmp_path: Path, ids: list[str]) -> Path:
    path = tmp_path / "run.json"
    path.write_text(json.dumps({
        "manifest_version": 1, "run_id": "r1", "seed": 501, "topic": "game-events",
        "totals": {"matches": 1, "events_generated": len(ids), "events_delivered": len(ids),
                   "events_failed": 0},
        "matches": {"m-1": {"events_generated": len(ids), "events_delivered": len(ids),
                            "max_match_seq": len(ids), "partitions": [0], "event_ids": ids}},
        "failed_event_ids": []}))
    return path


def test_verify_counts_late_rows_and_catches_an_id_in_both_tables(spark: SparkSession,
                                                                  tmp_path: Path) -> None:
    from streaming.verify_manifest import evaluate_silver, verify_silver

    cfg = _cfg(tmp_path)
    _commit(spark, cfg, [r(0, "r-0", T0), r(1, "r-1", T0)])
    _run(spark, cfg)
    _commit(spark, cfg, [r(2, "a-0", T0 - timedelta(days=1)), r(3, "n-0", ON_T),
                         r(4, None, T0)])
    _run(spark, cfg)

    def check() -> tuple[bool, list[str], dict[str, Any]]:
        rep = verify_silver(spark, _manifest(tmp_path, ["r-0", "r-1", "a-0", "n-0"]),
                            cfg.table_path, cfg.late_path, cfg.control_path,
                            cfg.checkpoint_path, cfg.bronze_path, 7)
        failed, notes = evaluate_silver(rep)
        return failed, notes, rep

    failed, notes, rep = check()
    assert not failed, notes
    assert (rep["found_unique_ids"], rep["silver_found_ids"], rep["late_found_ids"],
            rep["missing_ids"], rep["in_both_ids"]) == (4, 3, 1, 0, 0)
    cons = rep["consistency"]
    assert (cons["bronze_keyed_distinct_ids"], cons["silver_rows_upto_batch"],
            cons["late_rows_upto_batch"], cons["bronze_keyless_rows"],
            cons["control_excluded_total"]) == (4, 3, 1, 1, 1)

    # a-0 also sneaks into silver (appendOnly allows appends): R1 fails, and the
    # whole-table count no longer adds up.
    (spark.read.format("delta").load(cfg.late_path).where("event_id = 'a-0'")
     .drop("watermark_used", "lag_behind_wm_ms")
     .write.format("delta").mode("append").save(cfg.table_path))
    failed, notes, rep = check()
    assert failed and (rep["in_both_ids"], rep["ids_in_both_tables"]) == (1, 1)
    assert any(n.startswith("1 R1:") for n in notes)
    assert any("+ late rows 1 + D_open keyed rows 0 != distinct keyed bronze ids 4" in n
               for n in notes)


@fast
def test_verify_reports_a_missing_late_table(spark: SparkSession, tmp_path: Path,
                                             tail_offsets: Any) -> None:
    from streaming.verify_manifest import evaluate_silver, verify_silver

    cfg = _ready(spark, tmp_path, tail_offsets)
    shutil.rmtree(cfg.late_path)
    rep = verify_silver(spark, _manifest(tmp_path, ["a"]), cfg.table_path, cfg.late_path,
                        cfg.control_path, cfg.checkpoint_path, cfg.bronze_path, 7)
    assert evaluate_silver(rep) == (True, [f"late table {cfg.late_path} does not exist"])
