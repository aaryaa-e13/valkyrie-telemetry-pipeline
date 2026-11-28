"""Phase 3 slice 1: gates, DLQ content, precedence dedupe, first decision final.

Runs inside the Spark image (make test-spark SPARK_TESTS=tests/spark/test_dlq.py).
Every table and checkpoint lives in pytest's tmp_path. Every test asserts exact sets
and counts. Plan: docs/plans/phase3-plan.md sections 1-2, addenda A and B. Shared
builders: dlq_helpers.py.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from common.delta_log import current_version  # noqa: E402
from streaming import dlq  # noqa: E402
from streaming.silver import (  # noqa: E402
    REASON,
    REASONS,
    key_ok,
    process_batch,
    with_gates,
)
from tests.gates_corpus import CORPUS  # noqa: E402 (moved there in slice 2)
from tests.spark.dlq_helpers import (  # noqa: E402
    T0,
    _check_disjoint,
    _check_r8,
    _dlq,
    _ids,
    _load,
    _ready,
    bronze,
    ev,
    fast,
    frozen,
    off_key,
    raw,
)

# --- 1. the gate corpus (fast, no table; the corpus is tests/gates_corpus.py) -----------

@fast
@pytest.mark.parametrize("version", [1, 2])
def test_every_rule_fires_only_on_its_own_condition(spark: SparkSession, version: int) -> None:
    df = with_gates(bronze(spark, [(o, m) for o, m, *_ in CORPUS]), version).withColumn(
        "_keyed", key_ok())
    got = {x.kafka_offset: (x._keyed, x[REASON], list(x[REASONS]), list(x.quality_flags))
           for x in df.collect()}
    for off, _, keyed, reason, reasons, flags in CORPUS:
        if version == 2 and off == 26:          # v2 accepts region SEA (replay demo)
            reason, reasons = None, []
        assert got[off] == (keyed, reason, reasons, flags), (off, got[off])


# --- 2. one batch with every kind: DLQ content, equations, flags ------------------------

def test_one_batch_routes_every_row_to_exactly_one_place(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    msgs = [(o, m) for o, m, *_ in CORPUS]
    row = process_batch(frozen(spark, msgs, tmp_path / "b0"), 0, cfg, "q-1")
    _check_r8(row)
    keyless = [c for c in CORPUS if not c[2]]
    rejected = [c for c in CORPUS if c[2] and c[3]]
    accepted = [c for c in CORPUS if c[2] and not c[3]]
    assert (row["keyless_rows"], row["rejected_rows"], row["ontime_rows"], row["late_rows"]) \
        == (len(keyless), len(rejected), len(accepted), 0)     # batch 0: snapshot, no late
    assert row["excluded_blank_event_id"] == 2
    got = _dlq(spark, cfg)
    expected_keys = {off_key(o) for o, *_ in keyless} | {
        f"id:{json.loads(raw(m))['event_id']}" for _, m, *_ in rejected}
    assert set(got) == expected_keys
    for o, m, keyed, reason, reasons, _ in keyless + rejected:
        k = f"id:{json.loads(raw(m))['event_id']}" if keyed else off_key(o)
        assert (got[k]["key_kind"], got[k]["reason_code"], list(got[k]["reasons"]),
                got[k]["rules_version"], got[k]["kafka_offset"], got[k]["silver_batch_id"]) \
            == ("event_id" if keyed else "offset", reason, reasons, 1, o, 0)
        assert got[k]["raw_value"] == raw(m).decode()
    assert row["reject_counts"] == {
        r: sum(1 for c in keyless + rejected if c[3] == r) for r in {c[3] for c in keyless +
                                                                    rejected}}
    silver = {x["event_id"]: x for x in _load(spark, cfg.table_path)}
    assert set(silver) == {json.loads(raw(m))["event_id"] for _, m, *_ in accepted}
    for _, m, _, _, _, flags in accepted:
        assert list(silver[json.loads(raw(m))["event_id"]]["quality_flags"]) == flags
    assert row["flag_counts"] == {"future_event_time": 1, "self_kill": 1,
                                  "player_id_sentinel": 2, "unexpected_field": 2}
    assert (row["dlq_keyless_inserted_this_attempt"], row["dlq_keyed_inserted_this_attempt"]) \
        == (len(keyless), len(rejected))
    _check_disjoint(spark, cfg)
    # The cut: the versions the control row recorded are the tables' versions now.
    assert (row["silver_version"], row["late_version"], row["dlq_version"],
            row["resolutions_version"]) == (current_version(cfg.table_path),
                                            current_version(cfg.late_path),
                                            current_version(cfg.dlq_path),
                                            current_version(cfg.resolutions_path))
    assert row["cut_ms"] is not None and row["cut_ms"] >= 0


@fast
def test_good_data_writes_no_dlq_commit(spark: SparkSession, tmp_path: Path,
                                        tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    v0 = current_version(cfg.dlq_path)
    row = process_batch(bronze(spark, [(1, ev("a")), (2, ev("b")), (3, ev("a"))]), 0, cfg, "q")
    _check_r8(row)
    assert current_version(cfg.dlq_path) == v0 == 0          # created, never written
    assert _load(spark, cfg.dlq_path) == []
    assert (row["keyless_rows"], row["rejected_rows"], row["reject_counts"], row["dlq_ms"]) \
        == (0, 0, {}, 0)
    assert (row["inserted_this_attempt"], row["dup_in_batch"]) == (2, 1)


# --- 3. precedence dedupe (plan 1.2.3) ---------------------------------------------------

@fast
@pytest.mark.parametrize("bad_first", [True, False])
def test_dedupe_prefers_a_valid_copy_then_the_lowest_offset(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any, bad_first: bool) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    bad, good = ev("x", region="XX"), ev("x")
    msgs = [(1, bad), (2, good), (3, ev("x"))] if bad_first else \
        [(1, good), (2, bad), (3, ev("x"))]
    row = process_batch(bronze(spark, msgs), 0, cfg, "q")
    _check_r8(row)
    kept = _load(spark, cfg.table_path)
    assert [(x["event_id"], x["kafka_offset"]) for x in kept] == [("x", 2 if bad_first else 1)]
    assert (_load(spark, cfg.dlq_path), row["dup_in_batch"], row["rejected_rows"]) == ([], 2, 0)


# --- 4. the first decision is final, across batches (plan 2.6; addendum A) --------------

def test_a_valid_copy_after_a_dlq_copy_is_stranded_and_counted(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any,
        caplog: pytest.LogCaptureFixture) -> None:
    """Addendum A, real path: batch 0 DLQs a bad copy; batch 1's valid copies (one on
    time, one late) are dup_of_dlq / late_dup_of_dlq and not inserted; WARNING logged."""
    cfg = _ready(spark, tmp_path, tail_offsets)
    b0 = [(1, ev("s-on", kind="champion_pick", team="green")),
          (2, ev("s-late", when=T0 - timedelta(hours=2), region="XX")), (3, ev("anchor"))]
    r0 = process_batch(bronze(spark, b0), 1, cfg, "q")         # batch 1: not a snapshot
    assert (r0["rejected_rows"], r0["dlq_keyed_inserted_this_attempt"]) == (2, 2)
    b1 = [(4, ev("s-on", kind="champion_pick", when=T0 + timedelta(seconds=5))),
          (5, ev("s-late", when=T0 - timedelta(hours=2))), (6, ev("new", when=T0))]
    with caplog.at_level(logging.WARNING, logger="streaming.silver"):
        r1 = process_batch(bronze(spark, b1), 2, cfg, "q")
    _check_r8(r1)
    assert (r1["dup_of_dlq"], r1["late_dup_of_dlq"], r1["inserted_this_attempt"],
            r1["late_inserted_this_attempt"]) == (1, 1, 1, 0)
    assert _ids(spark, cfg.table_path) == {"anchor", "new"}
    assert _ids(spark, cfg.late_path) == set()
    assert set(_dlq(spark, cfg)) == {"id:s-on", "id:s-late"}
    assert any("valid copy stranded in DLQ" in r.getMessage() for r in caplog.records)
    _check_disjoint(spark, cfg)


@fast
def test_a_bad_copy_of_an_accepted_id_is_not_dead_lettered(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(bronze(spark, [(1, ev("a")), (2, ev("b", when=T0 - timedelta(hours=3)))]),
                  1, cfg, "q")
    r1 = process_batch(bronze(spark, [(3, ev("a", region="XX")), (4, ev("c", gold=-1))]),
                       2, cfg, "q")
    _check_r8(r1)
    assert (r1["rejected_rows"], r1["rejected_dup_of_accepted"],
            r1["dlq_keyed_inserted_this_attempt"]) == (2, 1, 1)
    assert set(_dlq(spark, cfg)) == {"id:c"}


@fast
def test_a_keyless_entry_with_an_event_id_does_not_block_that_id(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    """Addendum A.3: an H3 row is keyed by offset; a later valid copy of its id is
    accepted (the keyless entry is 'superseded', not stranded)."""
    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(bronze(spark, [(1, ev("k", event_time="later")), (2, ev("z"))]), 1, cfg, "q")
    r1 = process_batch(bronze(spark, [(3, ev("k"))]), 2, cfg, "q")
    assert (r1["dup_of_dlq"], r1["inserted_this_attempt"]) == (0, 1)
    assert set(_dlq(spark, cfg)) == {off_key(1)}
    assert _ids(spark, cfg.table_path) == {"k", "z"}


# --- 5. no-op MERGEs commit nothing (addendum B.2) ----------------------------------------

@fast
def test_an_insert_if_absent_merge_with_nothing_new_commits_nothing(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets)
    process_batch(bronze(spark, [(1, "{bad"), (2, ev("r", gold=-1))]), 0, cfg, "q")
    v = current_version(cfg.dlq_path)
    assert v == 1
    same = spark.read.format("delta").load(cfg.dlq_path)
    same.createOrReplaceTempView("again")
    r = spark.sql(f"MERGE INTO delta.`{cfg.dlq_path}` t USING again s ON t.dlq_key = s.dlq_key "
                  "WHEN NOT MATCHED THEN INSERT *").first()
    assert int(r["num_inserted_rows"]) == 0
    assert current_version(cfg.dlq_path) == v                 # no commit
    res = spark.createDataFrame([("id:r", "r", "silver", "replay", "t1", 2, "q", 0, T0)],
                                dlq.RESOLUTIONS_SCHEMA)
    assert dlq.insert_if_absent(spark, cfg.resolutions_path, res, tuple(
        f.name for f in dlq.RESOLUTIONS_SCHEMA.fields), "dlq_key", "res_view", 1) == 1
    vr = current_version(cfg.resolutions_path)
    res.createOrReplaceTempView("res_again")
    spark.sql(f"MERGE INTO delta.`{cfg.resolutions_path}` t USING res_again s "
              "ON t.dlq_key = s.dlq_key WHEN NOT MATCHED THEN INSERT *").collect()
    assert current_version(cfg.resolutions_path) == vr        # no commit
    with pytest.raises(dlq.DlqWriteError, match="inserted 0 rows, planned 1"):
        dlq.insert_if_absent(spark, cfg.resolutions_path, res, tuple(
            f.name for f in dlq.RESOLUTIONS_SCHEMA.fields), "dlq_key", "res_view", 1)

