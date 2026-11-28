"""Oracle rules R4-R6 and their gates (slice 5, DECISIONS D2.5): every rule shown to FAIL.

One passing lake is built once by the REAL silver job (process_batch, 3 batches,
query "q-1"), then each test tampers with its own copy of it and asserts the exact
set of failing rules, or the exact "not checkable" reason. Everything lives in
pytest's tmp_path; the live lake is never touched.

The lake (T0 = 2026-11-19 12:00 UTC, W = 60 s; Kafka time = event_time + 1 s):

    batch 0 (snapshot, wm -inf)  a0 T0-120s, a1 T0                 -> silver; m_0 = T0
    batch 1 (wm T0-60s)          b0 T0+10s, s1 T0-50s (slight)     -> silver
                                 v1 T0-90s (very)                  -> late;   m_1 = T0+10s
    batch 2 (wm T0-50s)          c0 T0+20s, v2 T0-40s (very, kept) -> silver; m_2 = T0+20s
    bronze only (foreign)        f-old T0-1 day; one unparsable row with a far-future Kafka time

R5 cannot fail alone: it follows from R2 + R4 (D6), so each R5 tamper also trips R2
or R4, and the test asserts both.
"""

from __future__ import annotations

import copy
import json
import re
import shutil
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from simulator.events import EventType, GameEvent  # noqa: E402
from streaming import dlq  # noqa: E402
from streaming.bronze import bronze_schema, ensure_bronze_table, to_raw_rows  # noqa: E402
from streaming.config import SilverJobConfig  # noqa: E402
from streaming.silver import ensure_silver_tables, process_batch  # noqa: E402
from streaming.verify_manifest import evaluate_silver, verify_silver  # noqa: E402

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)
T0 = datetime(2026, 11, 19, 12, 0, 0, tzinfo=UTC)
S = timedelta(seconds=1)
MS = timedelta(milliseconds=1)
_NOW = datetime.now(UTC)          # bronze ingest_time is the write time
TAIL = ('v1\n{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{}}\n'
        '{"sourceVersion":1,"reservoirId":"test","reservoirVersion":%d,"index":-1,'
        '"isStartingVersion":false}')
Row = tuple[int, str | None, datetime, Any]

BATCHES: list[list[Row]] = [
    [(0, "a0", T0 - 120 * S, "auto"), (1, "a1", T0, "auto")],
    [(2, "b0", T0 + 10 * S, "auto"), (3, "s1", T0 - 50 * S, "auto"),
     (4, "v1", T0 - 90 * S, "auto")],
    [(5, "c0", T0 + 20 * S, "auto"), (6, "v2", T0 - 40 * S, "auto")],
]
FOREIGN: list[Row] = [(7, "f-old", T0 - timedelta(days=1), "auto"),
                      (8, None, T0, T0 + timedelta(days=1))]
RUN_IDS = [e for b in BATCHES for _, e, _, _ in b]
BANDS = {"slight": {"min_x_watermark": 0.1, "max_x_watermark": 0.5, "min_delay_ms": 6000,
                    "max_delay_ms": 30000, "expected": "keep"},
         "very": {"min_x_watermark": 2.0, "max_x_watermark": 3.0, "min_delay_ms": 120000,
                  "max_delay_ms": 180000, "expected": "may_drop"}}
LATE = [{"event_id": "s1", "match_id": "m-1", "delay_ms": 20000, "band": "slight",
         "expected": "keep", "stream_lag_ms": 1000},
        {"event_id": "v1", "match_id": "m-1", "delay_ms": 130000, "band": "very",
         "expected": "may_drop", "stream_lag_ms": 1000},
        {"event_id": "v2", "match_id": "m-1", "delay_ms": 125000, "band": "very",
         "expected": "may_drop", "stream_lag_ms": 1000}]


def manifest_v3(ids: list[str] | None = None, late: list[dict[str, Any]] | None = None,
                w: float = 60.0) -> dict[str, Any]:
    ids = ids or RUN_IDS
    late = copy.deepcopy(LATE if late is None else late)
    counts = {b: sum(1 for x in late if x["band"] == b) for b in ("slight", "very")}
    return {
        "manifest_version": 3, "run_id": "r1", "seed": 7, "topic": "game-events",
        "totals": {"matches": 1, "events_generated": len(ids), "events_delivered": len(ids),
                   "events_failed": 0},
        "matches": {"m-1": {"events_generated": len(ids), "events_delivered": len(ids),
                            "max_match_seq": len(ids), "partitions": [0], "event_ids": ids}},
        "failed_event_ids": [],
        "kafka_offsets": {"0": {"min": 0, "max": 6, "count": 7}},
        "pacing": {"mode": "realtime", "time_scale": 10.0, "assumed_watermark_s": w,
                   "late_fraction": 0.3, "late_too_late_share": 0.2, "bands": BANDS,
                   "measured": {"sent": len(ids), "future_events": 0, "max_ahead_ms": 0,
                                "max_behind_ms": 0}},
        "late_counts": counts, "late_events_omitted_reason": None, "late_events": late,
    }


def _value(event_id: str, when: datetime) -> bytes:
    return GameEvent(
        event_id=event_id, event_type=EventType.ITEM_PURCHASE, event_time=when,
        match_id="m-1", match_seq=1, patch_version="14.3", region="EUW",
        player_id="p_000001", champion_id="ch_quillon", item_id="it_stormglass", gold=100,
    ).to_json_bytes()


def _df(spark: SparkSession, rows: list[Row]) -> DataFrame:
    data = [(b"m-1", _value(e, t) if e else b"{not json", "game-events", 0, off,
             t + S if k == "auto" else k) for off, e, t, k in rows]
    return to_raw_rows(spark.createDataFrame(data, KAFKA_SCHEMA))


def _cfg(root: Path) -> SilverJobConfig:
    return SilverJobConfig(
        bronze_path=str(root / "bronze"), table_path=str(root / "silver"),
        control_path=str(root / "control"), checkpoint_path=str(root / "cp"),
        late_path=str(root / "late"), status_path="")


@pytest.fixture(scope="module")
def base(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The passing lake, built once by the job (see the module docstring)."""
    root = tmp_path_factory.mktemp("oracle") / "lake"
    cfg = _cfg(root)
    ensure_bronze_table(spark, cfg.bronze_path,
                        bronze_schema(spark.createDataFrame([], KAFKA_SCHEMA)))
    ensure_silver_tables(spark, cfg)
    offsets = Path(cfg.checkpoint_path) / "offsets"
    offsets.mkdir(parents=True)
    for k in range(len(BATCHES)):          # batches 1, 2 are not snapshot batches
        (offsets / str(k)).write_text(TAIL % (k + 1), encoding="utf-8")
    all_rows = [x for b in BATCHES for x in b] + FOREIGN
    _df(spark, all_rows).coalesce(1).write.format("delta").mode("append") \
        .save(cfg.bronze_path)
    for n, rows in enumerate(BATCHES):
        process_batch(_df(spark, rows), n, cfg, "q-1")
    return root


@pytest.fixture
def lake(base: Path, tmp_path: Path) -> Iterator[SilverJobConfig]:
    shutil.copytree(base, tmp_path / "lake")
    yield _cfg(tmp_path / "lake")


def _unlock(spark: SparkSession, path: str) -> None:
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.appendOnly' = 'false')")


def _sql(spark: SparkSession, path: str, stmt: str) -> None:
    """UPDATE/DELETE on one table of the copy: stmt uses {t} for the table."""
    _unlock(spark, path)
    spark.sql(stmt.format(t=f"delta.`{path}`"))


def ts(t: datetime) -> str:
    return f"TIMESTAMP'{t.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def check(spark: SparkSession, tmp_path: Path, cfg: SilverJobConfig,
          manifest: dict[str, Any] | None = None) -> tuple[set[str], dict[str, Any], list[str]]:
    """(failing rule keys, report, notes). Every FAIL note must name its rule."""
    path = tmp_path / "run.json"
    path.write_text(json.dumps(manifest or manifest_v3()), encoding="utf-8")
    rep = verify_silver(spark, path, cfg.table_path, cfg.late_path, cfg.control_path,
                        cfg.checkpoint_path, cfg.bronze_path, 7)
    failed, notes = evaluate_silver(rep)
    rules = {m.group(1) for n in notes if (m := re.match(r"(?:\d+ )?(R\d+[a-d]?):", n))}
    assert failed == bool(rules), notes
    return rules, rep, notes


def gate_reason(rep: dict[str, Any]) -> str:
    v = rep["oracle"]["verdicts"]
    assert v["R4"] == v["R5"], v
    return v["R4"]


# --- the passing lake ------------------------------------------------------------------

@pytest.mark.spark_fast
def test_baseline_passes_every_rule_with_both_gates_checkable(spark, tmp_path, lake) -> None:
    rules, rep, notes = check(spark, tmp_path, lake)
    o = rep["oracle"]
    assert rules == set(), notes
    assert o["verdicts"] == dict.fromkeys(("R4", "R5", "R6a", "R6b", "R6c", "R6d"), "PASS")
    assert (o["w_check"]["checkable"], o["w_check"]["w_s"], o["w_check"]["manifest_w_s"]) \
        == (True, 60, 60.0)
    # foreign_max = f-old (T0 - 1 day); the unparsable far-future row does not count.
    assert o["isolation"]["checkable"] is True
    assert o["isolation"]["margin_ms"] == (timedelta(days=1) - 60 * S) // MS
    assert o["bands"] == {"ontime": {"expected": 4, "silver": 4, "late": 0},
                          "slight": {"expected": 1, "silver": 1, "late": 0},
                          "very": {"expected": 2, "silver": 1, "late": 1}}
    assert (o["r4_r5"]["r4_violations"], o["r4_r5"]["r5_violations"]) == (0, 0)
    r6 = o["r6"]
    assert (r6["r6a"]["late_rows"], r6["r6b"]["silver_rows_checked"],
            r6["r6c"]["batches_checked"], r6["r6d"]["batches_checked"]) == (1, 6, 3, 3)
    t = o["timing"]
    assert [(v["event_id"], v["batch_id"], v["event_time_minus_wm_ms"], v["routed"])
            for v in t["very_events"]] == [("v2", 2, 10_000, False), ("v1", 1, -30_000, True)]
    assert (t["slight_margin_ms"], t["slight_with_wm_null"]) == (10_000, 0)
    assert [(b["batch_id"], b["run_silver"], b["run_late"], b["late_inserted_this_attempt"])
            for b in t["run_batches"]] == [(0, 2, 0, 0), (1, 2, 1, 1), (2, 2, 0, 0)]
    # R10 (good data never dead-lettered): the DLQ is empty and offsets 0-6 are the
    # run's own range (7 messages); the broken message at offset 8 is outside it.
    dl = rep["dlq"]
    assert {k: dl["verdicts"][k] for k in ("R10a", "R10b")} == {"R10a": "PASS",
                                                                 "R10b": "PASS"}
    assert dl["r10"]["ranges"] == {"0": {"min": 0, "max": 6, "count": 7, "own": True}}
    assert (dl["dlq_rows_for_run"], dl["r10"]["own_rows"], dl["r10"]["shared_rows"]) \
        == (0, 0, 0)


# --- R1-R3 still fail on their own (with the new rules quiet) --------------------------

def test_r1_an_id_in_both_tables(spark, tmp_path, lake) -> None:
    # v1 also in silver, recorded as batch 0 (wm -inf) so R6b has nothing to say.
    (spark.read.format("delta").load(lake.late_path).where("event_id = 'v1'")
     .drop("watermark_used", "lag_behind_wm_ms")
     .withColumn("silver_batch_id", F.lit(0).cast("long"))
     .write.format("delta").mode("append").save(lake.table_path))
    assert check(spark, tmp_path, lake)[0] == {"R1"}


def test_r2_an_id_in_neither_table_also_breaks_r5(spark, tmp_path, lake) -> None:
    assert check(spark, tmp_path, lake, manifest_v3(RUN_IDS + ["x9"]))[0] == {"R2", "R5"}


@pytest.mark.parametrize("table", ["silver", "late"])
def test_r3_a_second_copy_of_one_row(spark, tmp_path, lake, table: str) -> None:
    path, eid = (lake.table_path, "c0") if table == "silver" else (lake.late_path, "v1")
    spark.read.format("delta").load(path).where(f"event_id = '{eid}'") \
        .write.format("delta").mode("append").save(path)
    assert check(spark, tmp_path, lake)[0] == {"R3"}


# --- R4, R5 -------------------------------------------------------------------------------

def test_r4_a_routed_event_the_manifest_calls_keep(spark, tmp_path, lake) -> None:
    late = copy.deepcopy(LATE)
    late[1].update(band="slight", expected="keep", delay_ms=20000)   # v1 relabelled slight
    rules, rep, _ = check(spark, tmp_path, lake, manifest_v3(late=late))
    assert rules == {"R4", "R5"}
    assert rep["oracle"]["r4_r5"]["r4_examples"] == [
        {"event_id": "v1", "band": "slight", "delay_ms": 20000}]


def test_r5_an_on_time_id_moved_from_silver_to_late(spark, tmp_path, lake) -> None:
    # a0 (T0-120s) placed in late as if batch 1 (wm T0-60s) had routed it: R6a stays quiet.
    row = (spark.read.format("delta").load(lake.table_path).where("event_id = 'a0'")
           .withColumn("silver_batch_id", F.lit(1).cast("long"))
           .withColumn("watermark_used", F.expr(ts(T0 - 60 * S)))
           .withColumn("lag_behind_wm_ms", F.lit(60_000).cast("long")))
    row = row.select(*spark.read.format("delta").load(lake.late_path).columns)
    # Collected first: a cached DataFrame is recomputed after the DELETE rewrites silver.
    kept = spark.createDataFrame(row.collect(), row.schema)
    assert kept.count() == 1
    _sql(spark, lake.table_path, "DELETE FROM {t} WHERE event_id = 'a0'")
    kept.write.format("delta").mode("append").save(lake.late_path)
    rules, rep, _ = check(spark, tmp_path, lake)
    assert rules == {"R4", "R5"}
    assert rep["oracle"]["r4_r5"]["r5_examples"] == [
        {"event_id": "a0", "band": "ontime", "delay_ms": 0}]


# --- R6 -----------------------------------------------------------------------------------

def test_r6a_late_row_after_its_watermark(spark, tmp_path, lake) -> None:
    _sql(spark, lake.late_path,
         f"UPDATE {{t}} SET watermark_used = {ts(T0 - 91 * S)} WHERE event_id = 'v1'")
    rules, rep, _ = check(spark, tmp_path, lake)
    assert rules == {"R6a"}
    assert rep["oracle"]["r6"]["r6a"]["detail"] == {
        "without_slice4_control_row": 0, "event_time_after_wm": 1, "wm_differs_from_control": 1}


def test_r6b_silver_row_at_its_batch_watermark(spark, tmp_path, lake) -> None:
    _sql(spark, lake.table_path,
         f"UPDATE {{t}} SET event_time = {ts(T0 - 50 * S)} WHERE event_id = 'c0'")  # = wm(2)
    rules, rep, _ = check(spark, tmp_path, lake)
    assert rules == {"R6b"} and rep["oracle"]["r6"]["r6b"]["examples"] == ["c0"]


def test_r6c_wm_not_from_the_formula(spark, tmp_path, lake) -> None:
    _sql(spark, lake.control_path,
         f"UPDATE {{t}} SET batch_max_clamped_event_time = {ts(T0 + 5 * S)} WHERE batch_id = 0")
    rules, rep, _ = check(spark, tmp_path, lake)
    r6c = rep["oracle"]["r6"]["r6c"]
    assert rules == {"R6c"} and (r6c["formula_mismatches"], r6c["goes_down"]) == (1, 0)
    assert r6c["examples"][0]["batch_id"] == 1


def test_r6c_wm_goes_down(spark, tmp_path, lake) -> None:
    # Batch 2 with W = 120 and the matching formula value: the formula holds, the order not.
    _sql(spark, lake.control_path,
         f"UPDATE {{t}} SET watermark_delay_s = 120, watermark_used = {ts(T0 - 110 * S)} "
         "WHERE batch_id = 2")
    rules, rep, _ = check(spark, tmp_path, lake)
    r6c = rep["oracle"]["r6"]["r6c"]
    assert rules == {"R6c"} and (r6c["formula_mismatches"], r6c["goes_down"]) == (0, 1)
    assert gate_reason(rep) == ("not checkable: W check: W differs across the run's "
                                "batches: [60, 120]")


def test_r6d_recorded_max_below_an_inserted_row(spark, tmp_path, lake) -> None:
    # The last batch, so no later wm depends on it (R6c stays quiet). c0 is T0+20s.
    _sql(spark, lake.control_path,
         f"UPDATE {{t}} SET batch_max_clamped_event_time = {ts(T0 + 19 * S)} WHERE batch_id = 2")
    rules, rep, _ = check(spark, tmp_path, lake)
    assert rules == {"R6d"}
    ex = rep["oracle"]["r6"]["r6d"]["examples"]
    assert [(x["batch_id"], x["rows_max_us"] - x["recorded_us"]) for x in ex] == [(2, 1_000_000)]


# --- the gates: "not checkable", never PASS, never FAIL -------------------------------------

def _v2() -> dict[str, Any]:
    m = manifest_v3()
    for k in ("pacing", "late_counts", "late_events", "late_events_omitted_reason"):
        m.pop(k)
    return {**m, "manifest_version": 2}


@pytest.mark.parametrize(("manifest", "reason"), [
    (manifest_v3(w=30.0), "W check: W differs: manifest 30 s, control 60 s"),
    (_v2(), "W check: manifest v2 has no lateness data"),
    ({**manifest_v3(), "late_events": None, "late_events_omitted_reason": "too many"},
     "W check: late_events omitted: too many"),
], ids=["w-differs", "v2", "list-omitted"])
def test_w_check_not_checkable(spark, tmp_path, lake, manifest, reason: str) -> None:
    rules, rep, notes = check(spark, tmp_path, lake, manifest)
    assert rules == set() and gate_reason(rep) == f"not checkable: {reason}"
    assert rep["oracle"]["r4_r5"] is None
    assert f"R4/R5 not checkable: {reason}" in notes


@pytest.mark.parametrize(("row", "isolated", "margin_ms"), [
    # boundary: clamped time == min_t + W (T0-60s) is NOT isolated; 1 ms less is.
    (("f-near", T0 - 60 * S, T0 - 59 * S), False, 0),
    (("f-near", T0 - 60 * S - MS, T0), True, 1),
    # V1: the job's clamp exactly. No Kafka time: never counts, however far in the future.
    (("f-fut", T0 + timedelta(days=1), None), True, (timedelta(days=1) - 60 * S) // MS),
    # A future event_time counts only up to its Kafka time ...
    (("f-fut", T0 + timedelta(days=1), T0 - 61 * S), True, 1000),
    (("f-fut", T0 + timedelta(days=1), T0), False, -60_000),
    # ... and an unparsable (keyless) row never counts.
    ((None, T0, T0 + timedelta(days=1)), True, (timedelta(days=1) - 60 * S) // MS),
    # D3.7: event_time > ingest_time (now) + 5 min never counts, whatever its Kafka time.
    (("f-s1", _NOW + timedelta(days=1), _NOW), True, (timedelta(days=1) - 60 * S) // MS),
], ids=["at-boundary", "1ms-below", "null-kafka-time", "clamped-below", "clamped-above",
        "keyless", "past-s1-tolerance"])
def test_isolation_uses_the_jobs_clamp(spark, tmp_path, lake, row, isolated: bool,
                                       margin_ms: int) -> None:
    eid, t, k = row
    _df(spark, [(20, eid, t, k)]).write.format("delta").mode("append").save(lake.bronze_path)
    rules, rep, _ = check(spark, tmp_path, lake)
    iso = rep["oracle"]["isolation"]
    assert rules == set()
    assert (iso["checkable"], iso["margin_ms"]) == (isolated, margin_ms)
    if isolated:
        assert gate_reason(rep) == "PASS"
    else:
        assert gate_reason(rep).startswith("not checkable: isolation: not isolated")


# --- R10: good data is never dead-lettered ----------------------------------------------

def _dead_letter(spark: SparkSession, cfg: SilverJobConfig, event_id: str | None,
                 offset: int) -> None:
    """Append one DLQ row as the job would write it for the message at `offset`.

    Stamped batch 3 (after the last committed batch 2): R9 and the consistency balance
    read up to that cut, so the tamper reaches R10 only (one fault -> one rule)."""
    keyed = event_id is not None
    row = dict.fromkeys(dlq.DLQ_COLUMNS)
    row.update(dlq_key=f"id:{event_id}" if keyed else f"off:game-events/0/{offset}",
               key_kind=dlq.KEY_KIND_ID if keyed else dlq.KEY_KIND_OFFSET,
               reason_code="H1" if not keyed else "H6", reasons=["test tamper"],
               rules_version=1, event_id=event_id, kafka_topic="game-events",
               kafka_partition=0, kafka_offset=offset, silver_query_id="q-1",
               silver_batch_id=3)
    spark.createDataFrame([row], dlq.DLQ_SCHEMA).write.format("delta").mode("append") \
        .save(cfg.dlq_path)


def test_r10a_a_good_event_pushed_to_the_dlq(spark, tmp_path, lake) -> None:
    _dead_letter(spark, lake, "b0", 2)       # b0 is one of the run's good events
    rules, rep, notes = check(spark, tmp_path, lake)
    assert rules == {"R10a"}, notes          # caught by id, so not counted again by R10b
    r10 = rep["dlq"]["r10"]
    assert (r10["by_id_rows"], r10["by_id_examples"], r10["own_rows"]) == (1, ["id:b0"], 0)
    assert rep["dlq"]["verdicts"]["R10b"] == "PASS"


def test_r10b_a_keyless_row_at_the_runs_own_offset(spark, tmp_path, lake) -> None:
    _dead_letter(spark, lake, None, 3)       # offset 3 is the run's s1 message
    rules, rep, notes = check(spark, tmp_path, lake)
    assert rules == {"R10b"}, notes
    r10 = rep["dlq"]["r10"]
    assert (r10["own_rows"], r10["own_keyless"], r10["own_examples"]) \
        == (1, 1, ["off:game-events/0/3"])
    assert rep["dlq"]["dlq_rows_for_run"] == 1


def test_r10_the_broken_test_message_outside_the_range_is_not_the_runs(
        spark, tmp_path, lake) -> None:
    # The lake's deliberately unparsable bronze row (offset 8) dead-lettered as the
    # job would: outside the run's offsets 0-6, so it is not counted as good data.
    _dead_letter(spark, lake, None, 8)
    rules, rep, notes = check(spark, tmp_path, lake)
    assert rules == set(), notes
    r10 = rep["dlq"]["r10"]
    assert (rep["dlq"]["dlq_total_rows"], rep["dlq"]["dlq_rows_for_run"], r10["own_rows"],
            r10["shared_rows"]) == (1, 0, 0, 0)
    assert {k: rep["dlq"]["verdicts"][k] for k in ("R10a", "R10b")} == {
        "R10a": "PASS", "R10b": "PASS"}
