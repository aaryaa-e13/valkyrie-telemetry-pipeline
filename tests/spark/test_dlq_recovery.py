"""Phase 3 slice 1: crash at every write boundary, rules-version and DLQ-table guard.

Runs inside the Spark image (make test-spark SPARK_TESTS=tests/spark/test_dlq_recovery.py).
Every table and checkpoint lives in pytest's tmp_path. Every test asserts exact sets
and counts. Plan: docs/plans/phase3-plan.md sections 1-2, addenda A and B. Shared
builders: dlq_helpers.py.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from streaming.config import SilverJobConfig  # noqa: E402
from streaming.silver import (  # noqa: E402
    SilverStartupError,
    check_silver_guard,
    plan_batch,
    process_batch,
    read_d_open,
    write_dlq,
    write_late,
    write_silver,
)
from tests.spark.dlq_helpers import (  # noqa: E402
    ATTEMPT_FIELDS,
    T0,
    _check_disjoint,
    _check_r8,
    _dlq,
    _ids,
    _ready,
    bronze,
    ev,
    frozen,
    off_key,
)

# --- 6. crash at every write boundary, then the re-run == a crash-free run --------------

B0 = [(1, ev("m")), (2, ev("y")), (3, ev("d0", region="XX"))]
B1 = [(4, ev("z", when=T0 - timedelta(hours=2)))]                        # z -> late
B2 = [(5, ev("l1", when=T0 - timedelta(hours=1))),                       # new late
      (6, ev("y", when=T0 - timedelta(hours=1))),                        # late copy of silver y
      (7, ev("o1", when=T0 + timedelta(seconds=1))),
      (8, ev("o1", when=T0 + timedelta(seconds=6))),                     # in-batch dup
      (9, ev("z", when=T0 + timedelta(seconds=2))),                      # on-time copy of late z
      (10, "{not json"),                                                 # keyless
      (11, ev("r1", patch_version="x")),                                 # new keyed reject
      (12, ev("d0", when=T0 + timedelta(seconds=3))),                    # stranded valid copy
      (13, ev("m", gold=-1))]                                            # bad copy of accepted
POINTS = ["plan", "dlq write", "late write", "silver write", "control row"]


def _content(spark: SparkSession, path: str) -> list[tuple[Any, ...]]:
    df = spark.read.format("delta").load(path)
    drop = [c for c in ("silver_inserted_at", "dlq_inserted_at") if c in df.columns]
    return sorted(tuple(x) for x in df.drop(*drop).collect())


def _setup(spark: SparkSession, root: Path, fz: Path, tail_offsets: Any) -> SilverJobConfig:
    cfg = _ready(spark, root, tail_offsets)
    process_batch(frozen(spark, B0, fz / "b0"), 0, cfg, "q-1")
    process_batch(frozen(spark, B1, fz / "b1"), 1, cfg, "q-1")
    return cfg


@pytest.mark.parametrize("crash_after", POINTS)
def test_any_crash_point_then_rerun_equals_a_crash_free_run(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any, crash_after: str) -> None:
    fz = tmp_path / "frozen"
    b2 = lambda: frozen(spark, B2, fz / "b2")  # noqa: E731
    ref = _setup(spark, tmp_path / "ref", fz, tail_offsets)
    ref_row = process_batch(b2(), 2, ref, "q-1")
    _check_r8(ref_row)
    assert (ref_row["late_rows"], ref_row["late_dup_of_silver"], ref_row["ontime_rows"],
            ref_row["dup_of_late"], ref_row["dup_of_dlq"], ref_row["dup_in_batch"],
            ref_row["keyless_rows"], ref_row["rejected_rows"],
            ref_row["rejected_dup_of_accepted"]) == (2, 1, 3, 1, 1, 1, 1, 2, 1)

    cfg = _setup(spark, tmp_path / "crash", fz, tail_offsets)
    if crash_after == "control row":
        process_batch(b2(), 2, cfg, "q-1")
    else:
        plan = plan_batch(b2(), 2, cfg, "q-1", datetime.now(UTC))
        d_open = None
        if crash_after != "plan":
            write_dlq(spark, cfg, plan)
        if crash_after in ("late write", "silver write"):
            d_open, _ = read_d_open(spark, cfg, plan)
            write_late(spark, cfg, plan, d_open)
        if crash_after == "silver write":
            write_silver(spark, cfg, plan, d_open)
        plan.release()
    rerun = process_batch(b2(), 2, cfg, "q-1")                  # Spark re-runs batch 2
    _check_r8(rerun)

    stored = [x.asDict() for x in spark.read.format("delta").load(cfg.control_path).collect()
              if x.batch_id == 2]
    assert len(stored) == 1
    ref_stored = next(x.asDict() for x in
                      spark.read.format("delta").load(ref.control_path).collect()
                      if x.batch_id == 2)
    keys = set(stored[0]) - ATTEMPT_FIELDS
    assert {k: stored[0][k] for k in keys} == {k: ref_stored[k] for k in keys}   # incl. cut
    for path_of in (lambda c: c.table_path, lambda c: c.late_path, lambda c: c.dlq_path):
        assert _content(spark, path_of(cfg)) == _content(spark, path_of(ref))
    _check_disjoint(spark, cfg)
    assert _ids(spark, cfg.table_path) | _ids(spark, cfg.late_path) == {"m", "y", "z", "l1",
                                                                       "o1"}
    assert set(_dlq(spark, cfg)) == {"id:d0", "id:r1", off_key(10)}
    attempt = {"plan": (2, 1, 1), "dlq write": (0, 1, 1), "late write": (0, 0, 1),
               "silver write": (0, 0, 0), "control row": (0, 0, 0)}[crash_after]
    assert (rerun["dlq_keyless_inserted_this_attempt"]
            + rerun["dlq_keyed_inserted_this_attempt"], rerun["late_inserted_this_attempt"],
            rerun["inserted_this_attempt"]) == attempt


# --- 7. start-up guard: rules version, missing DLQ tables ---------------------------------

def _checkpoint(cfg: SilverJobConfig, qid: str, committed: int, planned: int,
                tail_offsets: Any) -> None:
    cp = Path(cfg.checkpoint_path)
    (cp / "commits").mkdir(parents=True, exist_ok=True)
    (cp / "metadata").write_text(json.dumps({"id": qid}) + "\n", encoding="utf-8")
    for n in range(committed + 1):
        (cp / "commits" / str(n)).write_text('v1\n{"nextBatchWatermarkMs":0}\n',
                                             encoding="utf-8")
    tail_offsets(cfg.checkpoint_path, planned + 1)


def test_rules_version_may_change_only_after_a_clean_stop(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    cfg = _ready(spark, tmp_path, tail_offsets, n_tail=1)
    process_batch(bronze(spark, [(1, ev("a"))]), 0, cfg, "q-1")
    _checkpoint(cfg, "q-1", committed=0, planned=1, tail_offsets=tail_offsets)
    v2 = replace(cfg, rules_version=2, allow_rebuild=True)
    # Batch 1 in flight, no evidence of its first attempt: batch 0's version decides.
    assert check_silver_guard(spark, cfg) == "resume"
    with pytest.raises(SilverStartupError, match="batch 0 used rules version 1"):
        check_silver_guard(spark, v2)
    # Evidence: batch 1's first attempt wrote DLQ rows under version 1, then crashed.
    plan = plan_batch(bronze(spark, [(2, ev("b", region="SEA"))]), 1, cfg, "q-1",
                      datetime.now(UTC))
    write_dlq(spark, cfg, plan)
    plan.release()
    with pytest.raises(SilverStartupError, match=r"first attempt used rules version \[1\]"):
        check_silver_guard(spark, v2)
    # The re-run with version 1 finishes batch 1; after a clean stop version 2 is fine.
    process_batch(bronze(spark, [(2, ev("b", region="SEA"))]), 1, cfg, "q-1")
    _checkpoint(cfg, "q-1", committed=1, planned=1, tail_offsets=tail_offsets)
    assert check_silver_guard(spark, replace(cfg, rules_version=2)) == "resume"
    row = process_batch(bronze(spark, [(3, ev("c", region="SEA"))]), 2,
                        replace(cfg, rules_version=2), "q-1")
    assert (row["rules_version"], row["rejected_rows"], row["inserted_this_attempt"]) == (
        2, 0, 1)


def test_a_missing_dlq_table_after_phase3_batches_is_refused(
        spark: SparkSession, tmp_path: Path, tail_offsets: Any) -> None:
    import shutil

    cfg = _ready(spark, tmp_path, tail_offsets, n_tail=1)
    process_batch(bronze(spark, [(1, ev("a"))]), 0, cfg, "q-1")
    _checkpoint(cfg, "q-1", committed=0, planned=0, tail_offsets=tail_offsets)
    assert check_silver_guard(spark, cfg) == "resume"
    shutil.move(cfg.dlq_path, tmp_path / "dlq_aside")
    with pytest.raises(SilverStartupError, match="the table existed and is gone"):
        check_silver_guard(spark, cfg)
    assert check_silver_guard(spark, replace(cfg, allow_rebuild=True)) == "rebuild"
    shutil.move(tmp_path / "dlq_aside", cfg.dlq_path)
    assert check_silver_guard(spark, cfg) == "resume"

