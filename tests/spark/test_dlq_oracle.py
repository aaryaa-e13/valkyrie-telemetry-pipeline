"""Phase 3 slice 2: the three-way test (part 2), the watermark ignores bad rows (D3.7
condition 1), R10c/R10d/R11/R14 on a v4 lake built by the real stream, the 6 tamper
tests and R14's 3 tests.

Runs inside the Spark image (make test-spark SPARK_TESTS=tests/spark/test_dlq_oracle.py).
Every table and checkpoint lives in pytest's tmp_path. Tamper tests assert the EXACT set
of failing rules ("balance" = verify's consistency balance, which names no rule).
Plan: docs/plans/phase3-slice2-plan.md sections 4 and 6.
"""

from __future__ import annotations

import base64
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

from common.manifest_reader import load_manifest  # noqa: E402
from simulator import corrupt  # noqa: E402
from streaming import oracle_gates as og  # noqa: E402
from streaming.bronze import bronze_schema, ensure_bronze_table, to_raw_rows  # noqa: E402
from streaming.config import SilverJobConfig  # noqa: E402
from streaming.silver import (  # noqa: E402
    REASON,
    REASONS,
    key_ok,
    prepare_silver,
    process_batch,
    start_silver_query,
    with_gates,
)
from streaming.verify_manifest import evaluate_silver, verify, verify_silver  # noqa: E402
from tests.gates_corpus import corrupt_cases, corrupt_events, ev  # noqa: E402
from tests.spark.dlq_helpers import KAFKA_SCHEMA, TOPIC, _cfg, _load, _ready, bronze  # noqa: E402

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
# One real bad-data run built on the host by tests/fixtures/build_bad_run.py (the Spark
# image cannot import the simulator's generator); drift-checked by a unit test.
FIXTURE = json.loads(Path("tests/fixtures/bad_run.json").read_text(encoding="utf-8"))
MESSAGES: list[dict[str, Any]] = FIXTURE["messages"]


def _t(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def kafka_df(spark: SparkSession, msgs: list[tuple[int, bytes, datetime, bytes]]) -> DataFrame:
    """Bronze rows (to_raw_rows) for (offset, value, kafka_timestamp, key), partition 0."""
    return to_raw_rows(spark.createDataFrame(
        [(key, value, TOPIC, 0, off, kts) for off, value, kts, key in msgs], KAFKA_SCHEMA))


def fixture_df(spark: SparkSession, msgs: list[dict[str, Any]],
               pace: str = "backfill") -> DataFrame:
    """Bronze rows for fixture messages, with the ingest_time a real send gives them.

    backfill: the fixture's times (every send after the timeline's end, ingest 200 ms
    later). realtime: each message 0.5 s after its (logical) event_time, ingest 200 ms
    later."""
    rows, ingest = [], []
    for m in msgs:
        if pace == "backfill":
            kts, ing = _t(m["kafka_timestamp"]), _t(m["ingest_time"])
        else:
            kts = _t(m["event_time"]) + timedelta(milliseconds=500)
            ing = kts + timedelta(milliseconds=200)
        rows.append((m["key"].encode(), base64.b64decode(m["value_b64"]), TOPIC, 0,
                     m["offset"], kts))
        ingest.append((m["offset"], ing))
    raw = to_raw_rows(spark.createDataFrame(rows, KAFKA_SCHEMA))
    cols = raw.columns
    ing_df = spark.createDataFrame(ingest, "kafka_offset long, _ing timestamp")
    return (raw.join(ing_df, "kafka_offset").withColumn("ingest_time", F.col("_ing"))
            .withColumn("ingest_date", F.to_date("_ing")).select(*cols))


def _run_stream(spark: SparkSession, cfg: SilverJobConfig) -> None:
    assert prepare_silver(spark, cfg) in {"fresh", "resume", "rebuild-from-bronze"}
    q = start_silver_query(spark, cfg, trigger={"availableNow": True})
    q.awaitTermination(300)
    assert q.exception() is None, q.exception()


def _append(spark: SparkSession, cfg: SilverJobConfig, df: DataFrame) -> None:
    ensure_bronze_table(spark, cfg.bronze_path,
                        bronze_schema(spark.createDataFrame([], KAFKA_SCHEMA)))
    df.coalesce(1).write.format("delta").mode("append").save(cfg.bronze_path)


def check(spark: SparkSession, cfg: SilverJobConfig, manifest: Path
          ) -> tuple[set[str], dict[str, Any], list[str]]:
    """(failing rules, report, notes); "balance" stands for the consistency balance."""
    rep = verify_silver(spark, manifest, cfg.table_path, cfg.late_path, cfg.control_path,
                        cfg.checkpoint_path, cfg.bronze_path, 7)
    failed, notes = evaluate_silver(rep)
    rules = {m.group(1) for n in notes if (m := re.match(r"(?:\d+ )?(R\d+[a-d]?):", n))}
    if any("!= distinct keyed bronze ids" in n or "!= sum keyless_rows" in n for n in notes):
        rules.add("balance")
    assert failed == bool(rules), notes
    return rules, rep, notes


# --- 1. three-way test, part 2: the job's gates == corrupt.py == verify's gates ----------

def test_three_way_part2_the_jobs_gates_on_corrupted_bytes(spark: SparkSession) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    t = now - timedelta(minutes=1)
    cases = corrupt_cases(corrupt_events(t))
    start = t - timedelta(seconds=30)
    msgs, by_off = [], {}
    for i, (e, c) in enumerate(cases):
        value, _ = corrupt.corrupt_value(e, c, start)
        msgs.append((i, value, now, e.key))
        by_off[i] = (e, c, value)
    df = with_gates(kafka_df(spark, msgs), corrupt.RULES_VERSION_EXPECTED) \
        .withColumn("_keyed", key_ok())
    rows = df.select("kafka_offset", "_keyed", REASON, REASONS, "quality_flags",
                     F.unix_micros("ingest_time").alias("ing")).collect()
    assert len(rows) == len(cases) and {c.kind for _, c in cases} == set(corrupt.KINDS)
    for r in rows:
        e, c, value = by_off[r["kafka_offset"]]
        label = (e.event_type.value, c)
        x = corrupt.expected_for(c, e.event_type.value)
        job = (r["_keyed"], r[REASON], tuple(r[REASONS]), tuple(r["quality_flags"]))
        ov = og.classify(value.decode(), topic=TOPIC, partition=0, offset=r["kafka_offset"],
                         ingest_time=EPOCH + timedelta(microseconds=r["ing"]),
                         rules_version=corrupt.RULES_VERSION_EXPECTED)
        assert job == (ov.keyed, ov.reason_code, ov.reasons, ov.flags), (label, job, ov)
        if x.cls == "dlq":
            assert (r[REASON], r["_keyed"]) == (x.code, x.key_kind == "id"), (label, job)
        else:
            assert (r[REASON], tuple(r["quality_flags"])) == (None, (x.code,)), (label, job)


# --- 2. D3.7 condition 1: bad rows must not move m_k or the watermark ---------------------

@pytest.mark.parametrize("pace", ["backfill", "realtime"])
def test_dlq_and_flagged_rows_do_not_change_m_k_or_the_watermark(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any, pace: str) -> None:
    """The fixture run's good messages in 3 batches, twice: alone, and with the run's
    epoch_time (DLQ'd, keyed) and future_time (kept, flagged S1) messages at their own
    offsets. m_k and wm must be equal batch by batch (D3.7 condition 1)."""
    kinds = {"epoch_time", "future_time"}
    msgs = [m for m in MESSAGES if m["bad_kind"] is None or m["bad_kind"] in kinds]
    assert {m["bad_kind"] for m in msgs} == {None, *kinds}
    n = len(msgs)
    chunks = [msgs[k * n // 3:(k + 1) * n // 3] for k in range(3)]

    def run(name: str, with_bad: bool) -> list[dict[str, Any]]:
        scfg = _ready(spark, tmp_path / name, tail_offsets)
        return [process_batch(fixture_df(spark, [m for m in c if with_bad or not m["bad_kind"]],
                                         pace), b, scfg, "q")
                for b, c in enumerate(chunks, start=1)]         # batches 1-3: not snapshot

    good, mixed = run("good", False), run("mixed", True)
    per_batch = [{"batch": g["batch_id"],
                  "bad": sorted(m["bad_kind"] for m in c if m["bad_kind"]),
                  "m_k good": g["batch_max_clamped_event_time"],
                  "m_k mixed": x["batch_max_clamped_event_time"],
                  "wm good": g["watermark_used"], "wm mixed": x["watermark_used"]}
                 for g, x, c in zip(good, mixed, chunks, strict=True)]
    print(json.dumps(per_batch, default=str, indent=1))          # the evidence, either way
    assert sum(x["rejected_rows"] for x in mixed) == sum(
        1 for m in msgs if m["bad_kind"] == "epoch_time")
    assert [(x["batch_max_clamped_event_time"], x["watermark_used"]) for x in mixed] == [
        (g["batch_max_clamped_event_time"], g["watermark_used"]) for g in good], per_batch


# --- 3. a v4 lake built by the real stream: every rule PASS ---------------------------------

@pytest.fixture(scope="module")
def v4base(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The fixture run (config.bad.yaml, a covering seed) through the real stream."""
    root = tmp_path_factory.mktemp("v4") / "lake"
    scfg = _cfg(root)
    _append(spark, scfg, fixture_df(spark, MESSAGES))
    _run_stream(spark, scfg)
    (root / "run.json").write_text(json.dumps(FIXTURE["manifest"]), encoding="utf-8")
    (root / "goods.json").write_text(json.dumps(
        {m["event_id"]: m["good"] for m in MESSAGES if m["good"]}), encoding="utf-8")
    return root


@pytest.fixture
def v4(v4base: Path, tmp_path: Path) -> Iterator[tuple[SilverJobConfig, Path]]:
    shutil.copytree(v4base, tmp_path / "lake")
    yield _cfg(tmp_path / "lake"), tmp_path / "lake" / "run.json"


def test_the_v4_lake_passes_every_rule_with_exact_evidence(
        spark: SparkSession, v4: tuple[SilverJobConfig, Path]) -> None:
    cfg, path = v4
    rules, rep, notes = check(spark, cfg, path)
    assert rules == set(), notes
    m = load_manifest(path)
    bd = m.bad_data
    assert bd is not None and bd.bad_events is not None
    dl = rep["dlq"]
    for r in ("R8", "R9", "R10a", "R10b", "R10c", "R10d", "R11", "R14"):
        assert dl["verdicts"][r] == "PASS", (r, dl["verdicts"][r])
    assert set(bd.expected_rejections) == set(og.HARD_CODES)          # every hard gate
    assert set(bd.expected_flags) == set(og.SOFT_CODES)               # every soft gate
    assert dl["r10c"]["found_by_reason"] == bd.expected_rejections
    assert dl["r10c"]["extra_by_reason"] == {}
    assert dl["r10d"]["found_by_flag"] == bd.expected_flags
    assert set(dl["r10d"]["others_by_flag"].values()) == {0}
    assert (dl["r11"]["by_reason"], dl["r11"]["by_flag"]) == (bd.expected_rejections,
                                                              bd.expected_flags)
    ev_ = dl["bad_evidence"]
    assert not ev_["batches_shared"]
    assert ev_["control_reject_counts"] == bd.expected_rejections
    assert ev_["control_flag_counts"] == bd.expected_flags
    assert (ev_["dlq_expected_in_accepted"], ev_["flag_expected_in_dlq"]) == ({}, {})
    # R2 per C7: S ∪ L == C - expected DLQ ids
    assert rep["expected_unique_ids"] == len(m.event_ids) - bd.expected_dlq_count
    assert (rep["missing_ids"], rep["dlq_ids_in_accepted"]) == (0, 0)
    # R14 negative: every D_open id has only its bad copy
    r14 = dl["r14"]
    assert (r14["stranded_ids"], r14["counter_dup_of_dlq"]) == (0, 0)
    assert r14["copies_checked"] == r14["d_open_ids"] == bd.expected_by_key_kind["id"]
    # bronze R1 on v4: unreadable ids found by offset
    b = verify(spark, path, cfg.bronze_path)
    unreadable = sum(1 for x in bd.bad_events.values() if not x.id_readable)
    assert (b["missing_ids"], b["duplicate_rows"], b["expected_by_offset"],
            b["found_by_offset"]) == (0, 0, unreadable, unreadable) and unreadable > 0


def test_bronze_r1_misses_a_bad_message_that_is_not_at_its_offset(
        spark: SparkSession, v4: tuple[SilverJobConfig, Path]) -> None:
    cfg, path = v4
    data = json.loads(path.read_text())
    entry = next(e for e in data["bad_data"]["bad_events"] if e["kind"] == "malformed_json")
    entry["kafka_offset"] = 10_000
    path.write_text(json.dumps(data))
    b = verify(spark, path, cfg.bronze_path)
    assert b["missing_ids"] == 1 and b["missing_examples"] == [f"{TOPIC}/0/10000"]


# --- 4. the 6 tamper tests (plan section 4; sets derived, not run, before the run) --------

def _unlock(spark: SparkSession, path: str) -> None:
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('delta.appendOnly' = 'false')")


def _bad(path: Path, pred: Any) -> Any:
    bd = load_manifest(path).bad_data
    assert bd is not None and bd.bad_events is not None
    return next(b for b in bd.bad_events.values() if pred(b))


def _good_ids(path: Path) -> list[str]:
    m = load_manifest(path)
    assert m.bad_data is not None and m.bad_data.bad_events is not None
    return sorted(m.event_ids - set(m.bad_data.bad_events))


def _move_the_cut(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """One more (foreign) good bronze row + a stream run: the newest cut then includes
    every table version written before it (R9 reads at the cut)."""
    t = datetime.now(UTC) - timedelta(seconds=5)
    msg = json.dumps(ev("foreign-1", when=t)).encode()
    _append(spark, cfg, kafka_df(spark, [(50_000, msg, t + timedelta(seconds=1), b"m-1")]))
    _run_stream(spark, cfg)


def test_tamper_delete_one_keyed_dlq_row(spark: SparkSession, v4: Any) -> None:
    cfg, path = v4
    b = _bad(path, lambda b: b.expected_key_kind == "id")
    _unlock(spark, cfg.dlq_path)
    spark.sql(f"DELETE FROM delta.`{cfg.dlq_path}` WHERE dlq_key = 'id:{b.event_id}'")
    rules, rep, notes = check(spark, cfg, path)
    assert rules == {"R10c", "R11", "balance"}, notes
    assert rep["dlq"]["r10c"]["missing"] == 1


def test_tamper_change_one_reason_code(spark: SparkSession, v4: Any) -> None:
    cfg, path = v4
    b = _bad(path, lambda b: b.expected_class == "dlq" and b.expected_code != "bad_patch_format")
    _unlock(spark, cfg.dlq_path)
    spark.sql(f"UPDATE delta.`{cfg.dlq_path}` SET reason_code = 'bad_patch_format' "
              f"WHERE dlq_key = '{b.dlq_key(TOPIC)}'")
    rules, rep, notes = check(spark, cfg, path)
    assert rules == {"R10c", "R11"}, notes
    assert rep["dlq"]["r10c"]["wrong_reason"] == 1


def test_tamper_a_dlq_row_for_a_good_event_in_silver(spark: SparkSession, v4: Any) -> None:
    cfg, path = v4
    gid = _good_ids(path)[0]
    s_row = spark.read.format("delta").load(cfg.table_path).where(f"event_id = '{gid}'").first()
    d = spark.read.format("delta").load(cfg.dlq_path)
    row = d.where("key_kind = 'event_id'").first().asDict()
    row.update(dlq_key=f"id:{gid}", event_id=gid, reason_code="bad_enum", reasons=["bad_enum"],
               kafka_partition=s_row["kafka_partition"], kafka_offset=s_row["kafka_offset"])
    spark.createDataFrame([row], d.schema).write.format("delta").mode("append").save(
        cfg.dlq_path)
    _move_the_cut(spark, cfg)
    rules, rep, notes = check(spark, cfg, path)
    assert rules == {"R10a", "R9", "R11", "balance"}, notes


def test_tamper_a_silver_row_for_a_dlq_id(spark: SparkSession, v4: Any) -> None:
    cfg, path = v4
    b = _bad(path, lambda b: b.kind == "bad_team")
    events = json.loads((path.parent / "goods.json").read_text())
    good = events[b.event_id]
    s = spark.read.format("delta").load(cfg.table_path)
    row = s.where("size(quality_flags) = 0").first().asDict()
    row.update({k: good.get(k) for k in ("event_id", "event_type", "match_id", "match_seq",
                                          "player_id", "champion_id", "team",
                                          "winning_team")})
    row.update(kafka_partition=b.kafka_partition, kafka_offset=b.kafka_offset)
    spark.createDataFrame([row], s.schema).write.format("delta").mode("append").save(
        cfg.table_path)
    _move_the_cut(spark, cfg)
    rules, rep, notes = check(spark, cfg, path)
    # R11 too: verify's gates reject this message, yet a row for it is in silver. (The plan
    # listed {R2, R9, balance}; R11 sees the same fault from the bronze side.)
    assert rules == {"R2", "R9", "R11", "balance"}, notes
    assert rep["dlq_ids_in_accepted"] == 1


def test_tamper_break_one_control_count(spark: SparkSession, v4: Any) -> None:
    cfg, path = v4
    c = spark.read.format("delta").load(cfg.control_path)
    row = c.first().asDict()
    row.update(silver_query_id="q-fake", rows_in=row["rows_in"] + 1)
    spark.createDataFrame([row], c.schema).write.format("delta").mode("append").save(
        cfg.control_path)
    rules, rep, notes = check(spark, cfg, path)
    assert rules == {"R8"}, notes


def test_tamper_drop_one_flag(spark: SparkSession, v4: Any) -> None:
    cfg, path = v4
    b = _bad(path, lambda b: b.expected_class == "flag")
    _unlock(spark, cfg.table_path)
    spark.sql(f"UPDATE delta.`{cfg.table_path}` SET quality_flags = CAST(array() AS ARRAY<STRING>) "
              f"WHERE event_id = '{b.event_id}'")
    rules, rep, notes = check(spark, cfg, path)
    assert rules == {"R10d", "R11"}, notes


# --- 5. R14: stranded valid copy (addendum A) ---------------------------------------------

def _v1(tmp_path: Path, ids: list[str]) -> Path:
    path = tmp_path / "v1.json"
    path.write_text(json.dumps({
        "manifest_version": 1, "run_id": "r1", "seed": 1, "topic": TOPIC,
        "totals": {"matches": 1, "events_generated": len(ids), "events_delivered": len(ids),
                   "events_failed": 0},
        "matches": {"m-1": {"events_generated": len(ids), "events_delivered": len(ids),
                            "max_match_seq": len(ids), "partitions": [0], "event_ids": ids}},
        "failed_event_ids": []}), encoding="utf-8")
    return path


def test_r14_real_path_a_valid_copy_after_a_dlq_copy(spark: SparkSession,
                                                     tmp_path: Path) -> None:
    """Batch 0: a bad copy of x (bad_team) -> DLQ. Batch 1: the valid copy of x ->
    counted (dup_of_dlq or late_dup_of_dlq) and not inserted. R14 FAILs with x."""
    cfg = _cfg(tmp_path)
    t = datetime.now(UTC) - timedelta(seconds=30)
    _append(spark, cfg, bronze(spark, [(1, ev("x", when=t, kind="champion_pick", team="green")),
                                       (2, ev("anchor", when=t))]))
    _run_stream(spark, cfg)
    rules, rep, notes = check(spark, cfg, _v1(tmp_path, ["anchor"]))
    assert rules == set() and rep["dlq"]["verdicts"]["R14"] == "PASS", notes   # negative
    _append(spark, cfg, bronze(spark, [(3, ev("x", when=t, kind="champion_pick"))]))
    _run_stream(spark, cfg)
    ctl = _load(spark, cfg.control_path)
    assert sum(int(r["dup_of_dlq"]) + int(r["late_dup_of_dlq"]) for r in ctl) == 1
    rules, rep, notes = check(spark, cfg, _v1(tmp_path, ["anchor"]))
    assert rules == {"R14"}, notes
    r14 = rep["dlq"]["r14"]
    assert (r14["stranded_ids"], r14["stranded_copies"], r14["counter_dup_of_dlq"],
            r14["examples"]) == (1, 1, 1, ["x"])


def test_r14_tamper_a_valid_copy_of_a_dlq_id_appended_to_bronze(
        spark: SparkSession, v4: Any) -> None:
    """On the clean v4 lake: the valid copy of a dead-lettered (keyed) id appended to
    bronze, then consumed by the stream. Exactly R14 fails; R9 stays PASS."""
    cfg, path = v4
    b = _bad(path, lambda b: b.kind == "bad_patch")
    events = json.loads((path.parent / "goods.json").read_text())
    good = json.dumps(events[b.event_id]).encode()
    _append(spark, cfg, kafka_df(spark, [(60_000, good, datetime.now(UTC), b"m-x")]))
    _run_stream(spark, cfg)
    rules, rep, notes = check(spark, cfg, path)
    assert rules == {"R14"}, notes
    dl = rep["dlq"]
    assert dl["verdicts"]["R9"] == "PASS"
    assert (dl["r14"]["stranded_ids"], dl["r14"]["examples"]) == (1, [b.event_id])
    assert dl["verdicts"]["R11"].startswith("not checkable: 1 ids have more than one")
