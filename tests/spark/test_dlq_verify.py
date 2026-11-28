"""Phase 3 slice 1: verify R8, R9 and R10 on the real streaming query.

Runs inside the Spark image (make test-spark SPARK_TESTS=tests/spark/test_dlq_verify.py).
Every table and checkpoint lives in pytest's tmp_path. Every test asserts exact sets
and counts. Plan: docs/plans/phase3-plan.md sections 1-2, addenda A and B. Shared
builders: dlq_helpers.py.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from streaming.bronze import bronze_schema, ensure_bronze_table  # noqa: E402
from streaming.config import SilverJobConfig  # noqa: E402
from streaming.silver import (  # noqa: E402
    prepare_silver,
    start_silver_query,
)
from tests.spark.dlq_helpers import (  # noqa: E402
    KAFKA_SCHEMA,
    TOPIC,
    Msg,
    _cfg,
    bronze,
    ev,
)

# --- 8. verify: R8, R9, good data rejected (the real streaming query) ---------------------

def _append_bronze(spark: SparkSession, cfg: SilverJobConfig, msgs: list[Msg]) -> None:
    ensure_bronze_table(spark, cfg.bronze_path,
                        bronze_schema(spark.createDataFrame([], KAFKA_SCHEMA)))
    bronze(spark, msgs).coalesce(1).write.format("delta").mode("append").save(cfg.bronze_path)


def _run(spark: SparkSession, cfg: SilverJobConfig) -> None:
    assert prepare_silver(spark, cfg) in {"fresh", "resume", "rebuild-from-bronze"}
    q = start_silver_query(spark, cfg, trigger={"availableNow": True})
    q.awaitTermination(300)
    assert q.exception() is None, q.exception()


def _verify(spark: SparkSession, cfg: SilverJobConfig, tmp_path: Path,
            ids: list[str]) -> tuple[set[str], dict[str, Any], list[str]]:
    from streaming.verify_manifest import evaluate_silver, verify_silver

    path = tmp_path / "run.json"
    path.write_text(json.dumps({
        "manifest_version": 1, "run_id": "r1", "seed": 1, "topic": TOPIC,
        "totals": {"matches": 1, "events_generated": len(ids), "events_delivered": len(ids),
                   "events_failed": 0},
        "matches": {"m-1": {"events_generated": len(ids), "events_delivered": len(ids),
                            "max_match_seq": len(ids), "partitions": [0], "event_ids": ids}},
        "failed_event_ids": []}), encoding="utf-8")
    rep = verify_silver(spark, path, cfg.table_path, cfg.late_path, cfg.control_path,
                        cfg.checkpoint_path, cfg.bronze_path, 7)
    failed, notes = evaluate_silver(rep)
    rules = {m.group(1) for n in notes if (m := re.match(r"(?:\d+ )?(R\d+[a-d]?):", n))}
    assert failed == bool(rules), notes
    return rules, rep, notes


def test_verify_r8_r9_and_good_data_rejected(spark: SparkSession, tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    _append_bronze(spark, cfg, [(1, ev("a")), (2, ev("b")), (3, ev("c", region="XX")),
                                (4, "{x")])
    _run(spark, cfg)
    rules, rep, notes = _verify(spark, cfg, tmp_path, ["a", "b"])
    assert rules == set(), notes
    dl, cons = rep["dlq"], rep["consistency"]
    # Manifest v1 has no kafka_offsets: R10b is not checkable (never a PASS).
    assert dl["verdicts"] == {"R8": "PASS", "R9": "PASS", "R10a": "PASS",
                              "R10b": "not checkable: the manifest has no kafka_offsets (v1)",
                              "R10c": "PASS", "R10d": "PASS", "R11": "PASS", "R14": "PASS"}

    assert (dl["phase3_batches"], dl["dlq_rows_for_run"], dl["dlq_total_rows"]) == (1, 0, 2)
    assert dl["versions_after_cut"] == {"silver": 0, "late": 0, "dlq": 0, "resolutions": 0}
    assert (cons["bronze_keyed_distinct_ids"], cons["silver_rows_upto_batch"],
            cons["dlq_open_keyed_rows_upto_batch"], cons["dlq_keyless_rows_upto_batch"],
            cons["control_keyless_rows_phase3"]) == (3, 2, 1, 1, 1)

    # Good data rejected: the manifest says c was a good event of the run.
    # c is then missing from silver/late (R2) AND sits in the DLQ (R10a): both are true.
    rules, rep, notes = _verify(spark, cfg, tmp_path, ["a", "b", "c"])
    assert rep["dlq"]["dlq_rows_for_run"] == 1
    assert rules == {"R2", "R10a"} and rep["dlq"]["r10"]["by_id_examples"] == ["id:c"], notes

    # R8 tamper: a control row whose equations do not add up (another query id, so
    # nothing else looks at it). Exactly R8 fails.
    row = spark.read.format("delta").load(cfg.control_path).first().asDict()
    row.update(silver_query_id="q-fake", rows_in=row["rows_in"] + 1)
    (spark.createDataFrame([row], spark.read.format("delta").load(cfg.control_path).schema)
     .write.format("delta").mode("append").save(cfg.control_path))
    rules, rep, notes = _verify(spark, cfg, tmp_path, ["a", "b"])
    assert rules == {"R8"}, notes
    assert rep["dlq"]["r8"]["examples"][0]["query_id"] == "q-fake"


def test_verify_r9_sees_a_dlq_row_for_an_accepted_id_inside_a_cut(
        spark: SparkSession, tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    _append_bronze(spark, cfg, [(1, ev("a")), (2, ev("c", region="XX"))])
    _run(spark, cfg)
    # Tamper: a DLQ row for a (which is in silver), then one more batch, so the newest
    # cut includes the tampered DLQ version.
    d = spark.read.format("delta").load(cfg.dlq_path)
    fake = d.first().asDict()
    fake.update(dlq_key="id:a", event_id="a")
    spark.createDataFrame([fake], d.schema).write.format("delta").mode("append").save(
        cfg.dlq_path)
    rules, rep, notes = _verify(spark, cfg, tmp_path, ["a"])
    assert rep["dlq"]["versions_after_cut"]["dlq"] >= 1     # reported, not judged
    assert rep["dlq"]["r9"]["s_and_d_open"] == 0             # R9 reads at the cut
    _append_bronze(spark, cfg, [(3, ev("e"))])
    _run(spark, cfg)
    rules, rep, notes = _verify(spark, cfg, tmp_path, ["a", "e"])
    assert rep["dlq"]["r9"]["s_and_d_open"] == 1
    assert "R9" in rules and rep["dlq"]["verdicts"]["R9"] == "FAIL", notes
