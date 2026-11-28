"""Compare the bronze and silver Delta tables against one simulator run manifest.

The topic contains many runs (and test messages), so raw row counts are
meaningless. Instead we take the manifest's acknowledged event_ids (ground
truth) and check, for exactly those ids:

    missing     ids in the manifest but not in bronze           -> data loss
    duplicates  ids that appear in more than one bronze row     -> double counting
    per-match   rows per match vs manifest counts

and, for the whole table, rule R0 (DECISIONS D2.2):

    duplicate_kafka_offsets  rows sharing (kafka_topic, kafka_partition,
                             kafka_offset). Bronze holds one row per Kafka
                             message, so this must be 0. Duplicate *event ids*
                             are expected in bronze; a duplicate *offset* means
                             the sink wrote one message twice.
    null_kafka_offset_rows   rows where any of the three is NULL. Every Kafka
                             message has all three, so this must be 0 too: such
                             a row cannot be tied to one message, and SQL
                             DISTINCT would silently treat NULLs as equal.

The table is read through Delta at one pinned version (`table_version` in the
report), so every number comes from the same snapshot. Only committed data is
visible. `--versions V0,V1` also reports the row count at those versions (time
travel), used by smoke-restart to cross-check its Delta-log count.

Silver + late (--layer silver|all; DECISIONS D2.3, D2.4). With C = the run's ids,
S = silver ids from C, L = late ids from C. Always FAIL:
    R1 in_both_ids          S ∩ L must be empty (and no id in both tables at all)
    R2 missing_ids          S ∪ L must be C (nothing lost)
    R3 duplicate rows       an id of the run twice in silver or twice in late;
                            any event_id twice in the whole silver or late table
    R7 null_key_rows        a silver or late row with NULL event_id or event_time
    ids_not_in_bronze       silver/late ids that bronze does not have
Oracle rules R4-R6 (slice 5, D2.5; pure parts in streaming/oracle.py):
    R4 L ⊆ may_drop ids, R5 keep ids ⊆ S. Judged only when two gates hold:
       the W check (manifest assumed_watermark_s == the one W recorded for the
       run's batches) and isolation (no foreign bronze row could have moved a
       watermark past one of the run's events). Otherwise "not checkable".
    R6a-d the job against its own control table, for every slice-4 batch:
       late rows at or behind their batch's wm, silver rows ahead of it, the wm
       formula and its order, recorded m_k >= the clamped times it inserted.
    Timing evidence (G, per very event, slight margin) is reported, not judged.
Pinned to what silver has consumed (its newest committed batch C ended at bronze
version V, so it has read bronze versions < V):
    silver rows + late rows (excluding batches > C of the current query) ==
        distinct keyed event_ids of bronze@V-1  (only if no id's copies are > H days apart)
    sum(control excluded_total) == keyless rows of bronze@V-1
    control batch ids of the current query == 0..C (one row per batch)
These print "not checkable" (never "pass") when the preconditions do not hold.
Phase 3 (slice 1; pure parts in streaming/oracle_dlq.py). With D = the dead-letter
table, D_open = its keyed rows without a resolution:
    R8 per Phase 3 control row, the equations of plan 2.5 add up
    R9 at batch C's recorded cut (its control row's silver/late/dlq/resolutions
       versions, addendum B): S, L, D_open pairwise disjoint; no dlq_key twice; no
       keyed event_id twice in D; resolved ids in S or L. Versions written after the
       cut are reported (`versions_after_cut`), not judged.
    The consistency balance above becomes silver + late + D_open keyed (upto C) ==
       distinct keyed bronze ids, plus DLQ keyless rows (upto C) == sum keyless_rows
       of Phase 3 batches (plan 2.8's R9 balances, kept here so one fault -> one rule).
    R10 good data is never dead-lettered (manifests v1-v3 are good data; manifest v4,
       with deliberate bad data, is slice 2). R10a: no DLQ row carries a manifest id.
       R10b: no DLQ row sits at offsets only this run wrote (a partition whose
       count == max - min + 1); rows in ranges other producers shared are counted and
       reported as not checkable. Pure verdict: oracle_dlq.r10_verdicts.
    dlq_rows_for_run  every DLQ row R10 looked at (by id + own + shared offsets); the
       good-data smokes report it and must show 0.
Phase 3 slice 2 (manifest v4 with bad_data, D3.7; pure verdicts in oracle_dlq.py):
    bronze R1 finds a bad message whose id is not readable (half a message, a missing or
       blank id) by its (topic, partition, offset) from bad_events; every other by id.
    R2 on v4 (C7): S ∪ L == C - (ids the manifest expects in the DLQ). R10a/R10b then
       judge the ids/offsets NOT expected in the DLQ.
    R10c every expected rejection is in the DLQ (expected dlq_key and reason_code).
    R10d the expected flags == quality_flags on the run's S ∪ L rows (none on good rows).
    R11 verify's own gates (streaming/oracle_gates.py, stdlib) on the run's bronze rows
       == the run's DLQ rows and S ∪ L flags, under the rules version the run's batches
       recorded. Not checkable when an id has more than one bronze copy.
    R14 stranded valid copy: keyed D_open ids at batch C's cut with a consumed bronze
       copy that passes every hard rule (verify's gates), or dup_of_dlq + late_dup_of_dlq
       > 0 over Phase 3 batches.
    bad_evidence  per gate, the numbers e2e.smoke_bad's evidence table needs.
R10c, R10d, R11 and R14 run for every manifest version (a good-data run expects no DLQ
row and no flag). A v4 manifest whose bad_events list was left out is refused (exit 2).
The key check follows the job's (C3): a blank event_id is no key.

Accepts manifest v1-v4 (see common/manifest_reader.py).

Duplicate event ids are always *reported*. They only cause FAIL with
--fail-on-duplicates: bronze deliberately keeps duplicates (same seed sent
twice, D1.14) until silver dedupes them, so plain verification must not hide
them behind a failure. Missing ids and duplicate offsets always FAIL.

Usage (inside the Spark container):
    python -m streaming.verify_manifest --manifest manifests/run_X.json
    python -m streaming.verify_manifest --manifest ... --fail-on-duplicates
    python -m streaming.verify_manifest --manifest ... --layer silver

Exit code: 0 PASS, 1 FAIL, 2 unusable manifest or unusable arguments.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common.logging_setup import setup_logging
from common.manifest_reader import Manifest, ManifestError, load_manifest
from streaming import dlq as dlq_mod
from streaming import oracle as orc
from streaming import oracle_dlq as odl
from streaming import oracle_gates as ogt

log = logging.getLogger("streaming.verify_manifest")

DEFAULT_TABLE = "/data/lake/bronze/game_events"
DEFAULT_SILVER = "/data/lake/silver/game_events"
DEFAULT_CONTROL = "/data/lake/_control/silver_batches"
DEFAULT_LATE = "/data/lake/silver/late_events"
DEFAULT_SILVER_CHECKPOINT = "/data/checkpoints/silver"
DEFAULT_DLQ = "/data/lake/silver/dead_letter"
DEFAULT_RESOLUTIONS = "/data/lake/_control/dlq_resolutions"
OFFSET_KEY = ("kafka_topic", "kafka_partition", "kafka_offset")
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
LAYERS = ("bronze", "silver", "all")


def evaluate(report: dict[str, Any], fail_on_duplicates: bool) -> tuple[str, list[str]]:
    """Decide PASS/FAIL from a report. Returns (result, human-readable notes).

    Missing ids and duplicate Kafka offsets (R0) always fail. Duplicate event
    ids fail only if `fail_on_duplicates`.
    """
    notes: list[str] = []
    failed = False
    if report["missing_ids"] > 0:
        failed = True
        notes.append(f"{report['missing_ids']} event ids are missing (data loss)")
    if report["duplicate_kafka_offsets"] > 0:
        failed = True
        notes.append(f"R0: {report['duplicate_kafka_offsets']} extra rows share a Kafka "
                     "(topic, partition, offset): one message was written twice")
    if report["null_kafka_offset_rows"] > 0:
        failed = True
        notes.append(f"R0: {report['null_kafka_offset_rows']} rows have a NULL Kafka topic, "
                     "partition or offset: they cannot be tied to one message")
    if report["duplicate_rows"] > 0:
        if fail_on_duplicates:
            failed = True
            notes.append(f"{report['duplicate_rows']} duplicate rows (--fail-on-duplicates)")
        else:
            notes.append(
                f"{report['duplicate_rows']} duplicate rows reported, not failing "
                "(expected in the bronze layer; use --fail-on-duplicates to enforce)"
            )
    return ("FAIL" if failed else "PASS"), notes


def latest_version(spark: SparkSession, table_path: str) -> int:
    """Newest committed version of a Delta table."""
    from delta.tables import DeltaTable

    return int(DeltaTable.forPath(spark, table_path).history(1).first()["version"])


def count_at(spark: SparkSession, table_path: str, version: int) -> int:
    """Row count of the table as of `version` (Delta time travel)."""
    return spark.read.format("delta").option("versionAsOf", version).load(table_path).count()


def verify(spark: SparkSession, manifest_path: Path, table_path: str,
           versions: list[int] | None = None) -> dict[str, Any]:
    """Compute the comparison report for one manifest (v1, v2 or v3)."""
    manifest = load_manifest(manifest_path)
    # v4: a bad message whose id is not readable is found by offset (slice 2).
    by_offset = [b for b in (manifest.bad_data.bad_events or {}).values()
                 if not b.id_readable] if manifest.bad_data else []
    skip = {b.event_id for b in by_offset}
    pairs = [(m, e) for m, e in manifest.event_pairs if e not in skip]
    expected = spark.createDataFrame(pairs, "match_id string, event_id string")
    version = latest_version(spark, table_path)
    rows = spark.read.format("delta").option("versionAsOf", version).load(table_path)
    found_by_offset = 0
    missing_offsets: list[str] = []
    if by_offset:
        offs = spark.createDataFrame(
            [(manifest.topic, b.kafka_partition, b.kafka_offset) for b in by_offset],
            "kafka_topic string, kafka_partition int, kafka_offset long")
        hit = {(r.kafka_partition, r.kafka_offset) for r in
               rows.join(offs, list(OFFSET_KEY), "left_semi").select(*OFFSET_KEY).collect()}
        found_by_offset = len(hit)
        missing_offsets = [f"{manifest.topic}/{b.kafka_partition}/{b.kafka_offset}"
                           for b in by_offset if (b.kafka_partition, b.kafka_offset)
                           not in hit][:20]

    matched = rows.join(expected.select("event_id"), "event_id", "inner")
    per_id = matched.groupBy("event_id").count()
    missing_ids = [
        r.event_id for r in expected.join(per_id, "event_id", "left_anti").limit(20).collect()
    ]
    stats = per_id.agg(
        F.count("*").alias("found_unique"),
        F.coalesce(F.sum("count"), F.lit(0)).alias("rows_matched"),
        F.coalesce(F.sum(F.when(F.col("count") > 1, 1).otherwise(0)), F.lit(0))
        .alias("ids_duplicated"),
        F.coalesce(F.max("count"), F.lit(0)).alias("max_copies"),
    ).first()
    any_null = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in OFFSET_KEY])
    table = rows.agg(
        F.count("*").alias("total"),
        F.coalesce(F.sum(F.when(~F.col("parse_ok"), 1).otherwise(0)), F.lit(0))
        .alias("unparsed"),
        F.coalesce(F.sum(F.when(any_null, 1).otherwise(0)), F.lit(0)).alias("null_offsets"),
    ).first()
    keyed = rows.where(~any_null)
    distinct_offsets = keyed.select(*OFFSET_KEY).distinct().count()

    expected_unique = len({event_id for _, event_id in pairs}) + len(by_offset)
    found_unique = int(stats["found_unique"]) + found_by_offset
    rows_matched = int(stats["rows_matched"])
    total = int(table["total"])
    report: dict[str, Any] = {
        "manifest": str(manifest_path),
        "manifest_version": manifest.version,
        "run_id": manifest.run_id,
        "seed": manifest.seed,
        "table_path": table_path,
        "table_version": version,
        "expected_unique_ids": expected_unique,
        "found_unique_ids": found_unique,
        "missing_ids": expected_unique - found_unique,
        "missing_examples": missing_ids + missing_offsets,
        "expected_by_offset": len(by_offset), "found_by_offset": found_by_offset,
        "rows_for_these_ids": rows_matched,
        "duplicate_rows": rows_matched - (found_unique - found_by_offset),
        "ids_with_duplicates": int(stats["ids_duplicated"]),
        "max_copies_of_one_id": int(stats["max_copies"]),
        "total_rows": total,
        "unparsed_rows": int(table["unparsed"]),
        "duplicate_kafka_offsets": total - int(table["null_offsets"]) - distinct_offsets,
        "null_kafka_offset_rows": int(table["null_offsets"]),
    }
    if versions:
        report["version_counts"] = {str(v): count_at(spark, table_path, v) for v in versions}
    return report


def _keyed(df: Any) -> Any:
    return df.where(_job_key())


def _null_key(df: Any) -> Any:
    return F.coalesce(F.sum(F.when(F.col("event_id").isNull() | F.col("event_time").isNull(), 1)
                            .otherwise(0)), F.lit(0))


def _table_stats(df: Any) -> dict[str, int]:
    t = df.agg(F.count("*").alias("total"), F.count_distinct("event_id").alias("ids"),
               _null_key(df).alias("null_key")).first()
    return {"total": int(t["total"]), "duplicate_event_ids":
            int(t["total"]) - int(t["null_key"]) - int(t["ids"]), "null_key": int(t["null_key"])}


def verify_silver(spark: SparkSession, manifest_path: Path, silver_path: str, late_path: str,
                  control_path: str, checkpoint_path: str, bronze_path: str,
                  horizon_days: int, dlq_path: str | None = None,
                  resolutions_path: str | None = None) -> dict[str, Any]:
    """Silver + late (+ DLQ) section of the report (see module docstring). Read-only.

    dlq_path / resolutions_path default to the job's sibling folders of the late and
    control tables (streaming/config.py)."""
    from delta.tables import DeltaTable

    from streaming.start_guard import committed_source_offset, read_checkpoint_state

    dlq_path = dlq_path or str(Path(late_path).parent / "dead_letter")
    resolutions_path = resolutions_path or str(Path(control_path).parent / "dlq_resolutions")
    rep: dict[str, Any] = {"table_path": silver_path, "late_path": late_path}
    if not DeltaTable.isDeltaTable(spark, silver_path):
        rep["table_missing"] = True
        return rep
    if not DeltaTable.isDeltaTable(spark, late_path):
        rep["late_table_missing"] = True
        return rep
    manifest = load_manifest(manifest_path)
    # C7 (v4): the ids the manifest expects in the DLQ are not expected in S ∪ L.
    dlq_ids = manifest.expected_dlq_ids
    expected = spark.createDataFrame([(e,) for _, e in manifest.event_pairs
                                      if e not in dlq_ids], "event_id string").distinct()
    rep["expected_dlq_ids"] = len(dlq_ids)
    # Two tables are two snapshots, not one transaction (D5): a batch committing in
    # between can be half visible. e2e calls verify after silver caught up, when no
    # batch with rows is in flight. R1 cannot be faked by this: the job keeps the
    # tables disjoint at every commit. Bronze is read last, so it holds all they read.
    lv = latest_version(spark, late_path)
    late = spark.read.format("delta").option("versionAsOf", lv).load(late_path)
    sv = latest_version(spark, silver_path)
    silver = spark.read.format("delta").option("versionAsOf", sv).load(silver_path)
    bronze = spark.read.format("delta").option(
        "versionAsOf", latest_version(spark, bronze_path)).load(bronze_path)

    def per_run(df: Any) -> tuple[Any, int, int]:
        per_id = df.join(expected, "event_id").groupBy("event_id").count()
        st = per_id.agg(F.count("*").alias("found"),
                        F.coalesce(F.sum("count"), F.lit(0)).alias("rows")).first()
        return per_id.select("event_id"), int(st["found"]), int(st["rows"]) - int(st["found"])

    s_ids, s_found, s_dupes = per_run(silver)
    l_ids, l_found, l_dupes = per_run(late)
    union = s_ids.union(l_ids).distinct()
    expected_unique = expected.count()
    found = union.count()
    s_tab, l_tab = _table_stats(silver), _table_stats(late)
    rep.update({
        "table_version": sv, "late_table_version": lv,
        "expected_unique_ids": expected_unique,
        "found_unique_ids": found,                       # |S ∪ L|
        "silver_found_ids": s_found, "late_found_ids": l_found,   # |S|, |L|
        "missing_ids": expected_unique - found,          # R2
        "missing_examples": [r.event_id for r in
                             expected.join(union, "event_id", "left_anti").limit(20).collect()],
        "in_both_ids": s_ids.join(l_ids, "event_id").count(),     # R1 (this run)
        "duplicate_rows": s_dupes + l_dupes,             # R3 (this run)
        "total_rows": s_tab["total"], "late_total_rows": l_tab["total"],
        "duplicate_event_ids": s_tab["duplicate_event_ids"],       # R3 (whole silver)
        "late_duplicate_event_ids": l_tab["duplicate_event_ids"],  # R3 (whole late)
        "ids_in_both_tables": silver.select("event_id").join(      # R1 (whole tables)
            late.select("event_id"), "event_id").distinct().count(),
        "null_key_rows": s_tab["null_key"] + l_tab["null_key"],     # R7
        "ids_not_in_bronze": silver.select("event_id").union(late.select("event_id")).join(
            bronze.select("event_id"), "event_id", "left_anti").distinct().count(),
    })
    # R2 on v4 (C7) is an equality: an id the manifest expects in the DLQ must not be in
    # S ∪ L either.
    rep["dlq_ids_in_accepted"] = silver.select("event_id").union(late.select("event_id")).join(
        spark.createDataFrame([(e,) for e in dlq_ids], "event_id string"), "event_id",
        "left_semi").distinct().count() if dlq_ids else 0
    # Which send's copy was kept? (smoke-dupes: must be this manifest's send.)
    if manifest.kafka_offsets and manifest.topic:
        in_send = reduce(lambda x, y: x | y, [
            (F.col("kafka_partition") == part) & F.col("kafka_offset").between(r.min, r.max)
            for part, r in manifest.kafka_offsets.items()])
        cols = ["event_id", "kafka_topic", "kafka_partition", "kafka_offset"]
        rep["rows_from_this_send"] = silver.select(*cols).union(late.select(*cols)).join(
            expected, "event_id").where((F.col("kafka_topic") == manifest.topic) & in_send
                                        ).count()

    # Control is read after late and silver, so it holds a row for every batch whose
    # late or silver rows they show (the job writes the control row last).
    rep["oracle"] = oracle_section(spark, manifest, expected, silver, late, bronze,
                                   spark.read.format("delta").option(
                                       "versionAsOf", latest_version(spark, control_path))
                                   .load(control_path))

    state = read_checkpoint_state(checkpoint_path)
    committed = committed_source_offset(checkpoint_path)
    rep["dlq"] = dlq_section(spark, manifest, expected, silver_path, late_path, control_path,
                             dlq_path, resolutions_path, state.query_id,
                             None if committed is None else committed[0])
    consumed_v, consumed_reason = consumed_bronze_version(committed)
    rep["dlq"].update(slice2_section(
        spark, manifest, bronze, silver, late, control_path, dlq_path, resolutions_path,
        bronze_path, rep["dlq"], consumed_v, consumed_reason))

    # --- consistency, pinned to what silver has consumed --------------------------
    cons: dict[str, Any] = {"checkable": False}
    rep["consistency"] = cons
    if committed is None or committed[1] is None or not state.query_id:
        cons["reason"] = f"no committed silver batch with readable offsets in {checkpoint_path}"
        return rep
    c, offset = committed
    if offset.get("isStartingVersion") or "reservoirVersion" not in offset:
        cons["reason"] = f"batch {c} ended inside the initial snapshot: {offset}"
        return rep
    v = int(offset["reservoirVersion"])
    qid = state.query_id
    cons.update(query_id=qid, batch=c, bronze_versions_consumed_below=v,
                index=int(offset.get("index", -1)))
    if int(offset.get("index", -1)) != -1:
        cons["reason"] = f"batch {c} ended in the middle of bronze version {v}"
        return rep
    b = spark.read.format("delta").option("versionAsOf", v - 1).load(bronze_path)
    keyed = _keyed(b)
    spread = keyed.groupBy("event_id").agg(
        F.datediff(F.max(F.to_date("event_time")), F.min(F.to_date("event_time"))).alias("d")
    ).agg(F.coalesce(F.max("d"), F.lit(0)).alias("m")).first()["m"]
    control = spark.read.format("delta").load(control_path).where(
        (F.col("silver_query_id") == qid) & (F.col("batch_id") <= c))
    ctl = control.agg(F.count("*").alias("n"), F.count_distinct("batch_id").alias("ids"),
                      F.coalesce(F.min("batch_id"), F.lit(-1)).alias("lo"),
                      F.coalesce(F.max("batch_id"), F.lit(-1)).alias("hi"),
                      F.coalesce(F.sum("excluded_total"), F.lit(0)).alias("excl")).first()
    upto = ~((F.col("silver_query_id") == qid) & (F.col("silver_batch_id") > c))
    silver_upto, late_upto = silver.where(upto).count(), late.where(upto).count()
    dl = rep["dlq"]
    d_open_upto = d_keyless_upto = 0
    p3_keyless = int(dl.get("control_keyless_rows_phase3") or 0)
    if dl.get("dlq_table_exists"):
        d_all = spark.read.format("delta").load(dlq_path).where(upto)
        res_keys = spark.read.format("delta").load(resolutions_path).select("dlq_key")
        d_open_upto = (d_all.where(F.col("key_kind") == "event_id")
                       .join(res_keys, "dlq_key", "left_anti").count())
        d_keyless_upto = d_all.where(F.col("key_kind") == "offset").count()
    cons.update(
        checkable=True,
        bronze_rows=b.count(),
        bronze_keyed_distinct_ids=keyed.select("event_id").distinct().count(),
        bronze_keyless_rows=b.count() - keyed.count(),
        max_copy_spread_days=int(spread), horizon_days=horizon_days,
        silver_rows_upto_batch=silver_upto, late_rows_upto_batch=late_upto,
        dlq_open_keyed_rows_upto_batch=d_open_upto, dlq_keyless_rows_upto_batch=d_keyless_upto,
        control_keyless_rows_phase3=p3_keyless,
        control_rows=int(ctl["n"]), control_distinct_batches=int(ctl["ids"]),
        control_batch_range=[int(ctl["lo"]), int(ctl["hi"])],
        control_excluded_total=int(ctl["excl"]),
    )
    cons["rows_checkable"] = cons["max_copy_spread_days"] <= horizon_days
    return rep


# --- Phase 3: DLQ, R8, R9 (slice 1) -------------------------------------------------

R8_FIELDS = ("silver_query_id", "batch_id", "rules_version", "rows_in", "keyed_unique",
             *sorted({t for _, terms in odl.R8_EQUATIONS for t in terms} - {"keyed_rows"}),
             "rejected_rows", "reject_counts", "silver_version", "late_version",
             "dlq_version", "resolutions_version")


def r10_counts(manifest: Manifest, expected: DataFrame, dlq: DataFrame) -> dict[str, Any]:
    """The numbers R10 judges (oracle_dlq.r10_verdicts): this run's rows in the DLQ.

    By id: every DLQ row (any key kind, resolved or not) with a manifest id. By offset:
    the rows NOT caught by id, inside a partition's [min, max] of the manifest, split
    into the run's own ranges (count == max - min + 1) and shared ones.
    v4 (slice 2): `expected` holds only the ids NOT expected in the DLQ, and the rows the
    manifest expects (their dlq_key) are left out of the offset count: R10c judges them.
    """
    if manifest.bad_data is not None and manifest.topic:
        keys = [b.dlq_key(manifest.topic) for b in manifest.bad_data.dlq_events()]
        if keys:
            dlq = dlq.where(~F.col("dlq_key").isin(keys))
    by_id = dlq.join(expected, "event_id", "left_semi").select("dlq_key").distinct()
    out: dict[str, Any] = {
        "ids_listed": len(manifest.event_pairs),
        "by_id_rows": by_id.count(),
        "by_id_examples": [r.dlq_key for r in by_id.limit(odl.MAX_EXAMPLES).collect()],
        "own_rows": 0, "own_keyless": 0, "own_examples": [],
        "shared_rows": 0, "shared_keyless": 0}
    if not (manifest.kafka_offsets and manifest.topic):
        out.update(offsets_checkable=False,
                   offsets_reason="the manifest has no kafka_offsets (v1)")
        return out
    ranges = {int(p): r for p, r in manifest.kafka_offsets.items()}
    out["offsets_checkable"] = True
    out["ranges"] = {str(p): {"min": r.min, "max": r.max, "count": r.count,
                              "own": odl.own_range(r.min, r.max, r.count)}
                     for p, r in sorted(ranges.items())}
    in_send = reduce(lambda x, y: x | y, [
        (F.col("kafka_partition") == F.lit(p)) & F.col("kafka_offset").between(r.min, r.max)
        for p, r in ranges.items()])
    at = (dlq.where((F.col("kafka_topic") == manifest.topic) & in_send)
          .join(by_id, "dlq_key", "left_anti"))
    for r in at.groupBy("kafka_partition", "key_kind").count().collect():
        side = "own" if out["ranges"][str(r.kafka_partition)]["own"] else "shared"
        out[f"{side}_rows"] += int(r["count"])
        if r.key_kind == dlq_mod.KEY_KIND_OFFSET:
            out[f"{side}_keyless"] += int(r["count"])
    own_p = [p for p, r in ranges.items() if out["ranges"][str(p)]["own"]]
    if out["own_rows"] and own_p:
        out["own_examples"] = [r.dlq_key for r in at.where(F.col("kafka_partition").isin(own_p))
                               .select("dlq_key").limit(odl.MAX_EXAMPLES).collect()]
    return out


def dlq_section(spark: SparkSession, manifest: Manifest, expected: DataFrame,
                silver_path: str, late_path: str, control_path: str, dlq_path: str,
                resolutions_path: str, query_id: str | None,
                committed_batch: int | None) -> dict[str, Any]:
    """R8 over every control row, R9 at batch C's cut, and this run's DLQ rows."""
    from delta.tables import DeltaTable

    out: dict[str, Any] = {"dlq_path": dlq_path, "resolutions_path": resolutions_path}
    ctl = spark.read.format("delta").option(
        "versionAsOf", latest_version(spark, control_path)).load(control_path)
    if "rules_version" not in ctl.columns:
        out.update(dlq_table_exists=DeltaTable.isDeltaTable(spark, dlq_path),
                   phase3_batches=0, r8=odl.r8([]),
                   r9={"checkable": False, "reason": "no Phase 3 control row"})
        return out
    rows = [r.asDict() for r in ctl.select(*R8_FIELDS).collect()]
    p3 = [r for r in rows if r["rules_version"] is not None]
    out["phase3_batches"] = len(p3)
    out["r8"] = odl.r8(rows)
    out["control_keyless_rows_phase3"] = sum(
        int(r["keyless_rows"] or 0) for r in p3 if r["silver_query_id"] == query_id
        and committed_batch is not None and r["batch_id"] <= committed_batch)
    exists = DeltaTable.isDeltaTable(spark, dlq_path) and DeltaTable.isDeltaTable(
        spark, resolutions_path)
    out["dlq_table_exists"] = exists
    if not exists:
        out["r9"] = {"checkable": False, "reason": f"{dlq_path} or {resolutions_path} missing"}
        return out
    d_now = spark.read.format("delta").option(
        "versionAsOf", latest_version(spark, dlq_path)).load(dlq_path)
    out["dlq_total_rows"] = d_now.count()
    out["r10"] = r10 = r10_counts(manifest, expected, d_now)
    out["dlq_rows_for_run"] = r10["by_id_rows"] + r10["own_rows"] + r10["shared_rows"]
    cut_row = next((r for r in p3 if r["silver_query_id"] == query_id
                    and r["batch_id"] == committed_batch), None)
    if cut_row is None:
        out["r9"] = {"checkable": False, "reason": f"batch {committed_batch} of query "
                     f"{query_id} has no Phase 3 control row (no recorded cut)"}
        return out
    out["cut_batch"] = committed_batch
    cut = {k: cut_row[f"{k}_version"] for k in ("silver", "late", "dlq", "resolutions")}
    out["cut"] = cut
    if None in cut.values():
        out["r9"] = {"checkable": False, "reason": f"incomplete cut {cut}"}
        return out

    def ids(path: str, v: int) -> DataFrame:
        return spark.read.format("delta").option("versionAsOf", v).load(path)

    s = ids(silver_path, cut["silver"]).select("event_id")
    lt = ids(late_path, cut["late"]).select("event_id")
    d = ids(dlq_path, cut["dlq"])
    res = ids(resolutions_path, cut["resolutions"]).select("dlq_key")
    keyed = d.where(F.col("key_kind") == "event_id")
    d_open = keyed.join(res, "dlq_key", "left_anti").select("event_id")
    resolved = keyed.join(res, "dlq_key", "left_semi").select("event_id")
    st = d.agg(F.count("*").alias("n"), F.count_distinct("dlq_key").alias("keys")).first()
    kst = keyed.agg(F.count("*").alias("n"),
                    F.count_distinct("event_id").alias("ids")).first()

    def both(a: DataFrame, b: DataFrame) -> int:
        return a.distinct().join(b.distinct(), "event_id").count()

    out["r9"] = {
        "checkable": True, "cut": cut,
        "s_and_d_open": both(s, d_open), "l_and_d_open": both(lt, d_open),
        "s_and_l": both(s, lt),
        "duplicate_dlq_keys": int(st["n"]) - int(st["keys"]),
        "duplicate_keyed_dlq_ids": int(kst["n"]) - int(kst["ids"]),
        "resolved_not_accepted": resolved.distinct().join(
            s.union(lt), "event_id", "left_anti").count(),
        "d_open_keyed_rows": d_open.count(), "dlq_rows": int(st["n"]),
    }
    # Reported, not judged (yet): versions written after the cut. A writer outside the
    # stream (or a tamper) shows here; R1-R3 read the latest versions and still see it.
    latest = {"silver": latest_version(spark, silver_path),
              "late": latest_version(spark, late_path),
              "dlq": latest_version(spark, dlq_path),
              "resolutions": latest_version(spark, resolutions_path)}
    out["versions_after_cut"] = {k: latest[k] - cut[k] for k in cut}
    return out


# --- Phase 3 slice 2: R10c, R10d, R11, R14 and the smoke-bad evidence ----------------------

RUN_OFFSET_SCHEMA = "kafka_topic string, kafka_partition int, kafka_offset long"
_RUN_COLS = ("kafka_topic", "kafka_partition", "kafka_offset", "event_id", "raw_value")
_CTL_FIELDS = ("silver_query_id", "batch_id", "rules_version", "rows_in", "reject_counts",
               "flag_counts", "dup_of_dlq", "late_dup_of_dlq")


def consumed_bronze_version(committed: tuple[int, dict] | None) -> tuple[int | None, str | None]:
    """The newest bronze version silver has consumed (its batch C ended at V: V - 1),
    or (None, why not). Same preconditions as the consistency section."""
    if committed is None or committed[1] is None:
        return None, "no committed silver batch with readable offsets"
    c, offset = committed
    if offset.get("isStartingVersion") or "reservoirVersion" not in offset:
        return None, f"batch {c} ended inside the initial snapshot"
    if int(offset.get("index", -1)) != -1:
        return None, f"batch {c} ended in the middle of a bronze version"
    return int(offset["reservoirVersion"]) - 1, None


def _utc(us: int | None) -> datetime | None:
    return None if us is None else EPOCH_UTC + timedelta(microseconds=int(us))


def _run_bronze_rows(spark: SparkSession, manifest: Manifest, bronze: DataFrame) -> list[Any]:
    """The run's bronze rows: by id, plus every bad message by (topic, partition, offset)
    (a half message or a missing / blank id cannot be found by id)."""
    bad = (manifest.bad_data.bad_events or {}) if manifest.bad_data else {}
    unreadable = {e for e, b in bad.items() if not b.id_readable}
    ids = spark.createDataFrame([(e,) for _, e in manifest.event_pairs if e not in unreadable],
                                "event_id string")
    cols = [*_RUN_COLS, F.unix_micros("ingest_time").alias("ingest_us")]
    out = bronze.join(ids, "event_id", "left_semi").select(*cols)
    if bad and manifest.topic:
        offs = spark.createDataFrame([(manifest.topic, b.kafka_partition, b.kafka_offset)
                                      for b in bad.values()], RUN_OFFSET_SCHEMA)
        out = out.unionByName(bronze.join(offs, list(OFFSET_KEY), "left_semi").select(*cols))
    return out.dropDuplicates(list(OFFSET_KEY)).collect()


def _not_checkable(reason: str) -> dict[str, Any]:
    return {"checkable": False, "reason": reason}


def slice2_section(spark: SparkSession, manifest: Manifest, bronze: DataFrame,
                   silver: DataFrame, late: DataFrame, control_path: str, dlq_path: str,
                   resolutions_path: str, bronze_path: str, dl: dict[str, Any],
                   consumed_v: int | None, consumed_reason: str | None) -> dict[str, Any]:
    """R10c, R10d, R11 and R14 inputs, plus `bad_evidence` (see the module docstring)."""
    out: dict[str, Any] = {}
    ctl_df = spark.read.format("delta").option(
        "versionAsOf", latest_version(spark, control_path)).load(control_path)
    ctl_rows = ([r.asDict() for r in ctl_df.select(*_CTL_FIELDS).collect()]
                if "rules_version" in ctl_df.columns else [])
    if not dl.get("dlq_table_exists"):
        nc = _not_checkable("no DLQ table to look in")
        return {"r10c": nc, "r10d": nc, "r11": nc, "r14": nc}
    dlq_now = spark.read.format("delta").option(
        "versionAsOf", latest_version(spark, dlq_path)).load(dlq_path)
    topic = manifest.topic or ""
    run = _run_bronze_rows(spark, manifest, bronze)
    run_offsets = [(r.kafka_topic, int(r.kafka_partition), int(r.kafka_offset)) for r in run]
    off_df = spark.createDataFrame(run_offsets, RUN_OFFSET_SCHEMA)
    act_dlq = dlq_now.join(off_df, list(OFFSET_KEY), "left_semi").select(
        "dlq_key", "key_kind", "reason_code", "reasons", "rules_version", "event_id",
        "silver_query_id", "silver_batch_id", "kafka_partition", "kafka_offset").collect()
    has_flags = "quality_flags" in silver.columns and "quality_flags" in late.columns
    acc_cols = ["event_id", "silver_query_id", "silver_batch_id", *OFFSET_KEY,
                "quality_flags" if has_flags else F.lit(None).cast("array<string>")
                .alias("quality_flags")]
    act_acc = (silver.select(*acc_cols).unionByName(late.select(*acc_cols))
               .join(off_df, list(OFFSET_KEY), "left_semi").collect())

    # --- R10c: every expected rejection under its expected key and reason ---------------
    expected_dlq = manifest.bad_data.dlq_events() if manifest.bad_data else []
    by_key = {r.dlq_key: r for r in act_dlq}
    by_off = {(int(r.kafka_partition), int(r.kafka_offset)): r for r in act_dlq}
    missing = wrong_reason = wrong_kind = 0
    found_by_reason: dict[str, int] = {}
    examples: list[str] = []
    for b in expected_dlq:
        key = b.dlq_key(topic)
        r = by_key.get(key)
        if r is None:
            missing += 1
            other = by_off.get((b.kafka_partition, b.kafka_offset))
            if other is not None:
                wrong_kind += 1
            examples.append(f"{key} ({b.kind}, {b.expected_code}) not in DLQ"
                            + (f"; found as {other.dlq_key}" if other is not None else ""))
        elif r.reason_code != b.expected_code:
            wrong_reason += 1
            examples.append(f"{key} ({b.kind}): reason {r.reason_code} != {b.expected_code}")
        else:
            found_by_reason[b.expected_code] = found_by_reason.get(b.expected_code, 0) + 1
    expected_keys = {b.dlq_key(topic) for b in expected_dlq}
    extra_by_reason: dict[str, int] = {}
    for r in act_dlq:
        if r.dlq_key not in expected_keys:
            extra_by_reason[r.reason_code] = extra_by_reason.get(r.reason_code, 0) + 1
    out["r10c"] = {"checkable": True, "expected": len(expected_dlq), "missing": missing,
                   "wrong_reason": wrong_reason, "wrong_key_kind": wrong_kind,
                   "found_by_reason": found_by_reason, "extra_by_reason": extra_by_reason,
                   "examples": examples[:odl.MAX_EXAMPLES]}

    # --- R10d: expected flags == the run's S ∪ L flags ---------------------------------
    flag_events = {b.event_id: b for b in (manifest.bad_data.flag_events()
                                           if manifest.bad_data else [])}
    mismatched = null_flags = 0
    ex_d: list[str] = []
    rows_with: dict[str, set[str]] = {}
    for r in act_acc:
        if r.quality_flags is None:
            null_flags += 1
            continue
        got = tuple(r.quality_flags)
        for f in got:
            rows_with.setdefault(f, set()).add(r.event_id)
        want = (flag_events[r.event_id].expected_code,) if r.event_id in flag_events else ()
        if got != want:
            mismatched += 1
            ex_d.append(f"{r.event_id}: {list(got)} != expected {list(want)}")
    flag_found = {f: len({e for e, b in flag_events.items() if b.expected_code == f}
                         & rows_with.get(f, set())) for f in ogt.SOFT_CODES}
    flag_others = {f: len(rows_with.get(f, set()) - {e for e, b in flag_events.items()
                                                       if b.expected_code == f})
                   for f in ogt.SOFT_CODES}
    out["r10d"] = {"rows_checked": len(act_acc), "mismatched": mismatched,
                   "null_flags": null_flags, "examples": ex_d[:odl.MAX_EXAMPLES],
                   "found_by_flag": flag_found, "others_by_flag": flag_others}

    # --- R11: verify's own gates on the run's bronze rows ----------------------------------
    copies: dict[str, int] = {}
    for r in run:
        if r.event_id is not None and r.event_id.strip(" ") != "":
            copies[r.event_id] = copies.get(r.event_id, 0) + 1
    dup_ids = sorted(e for e, n in copies.items() if n > 1)
    batches = {(r.silver_query_id, int(r.silver_batch_id)) for r in [*act_dlq, *act_acc]
               if r.silver_batch_id is not None}
    ctl_by = {(r["silver_query_id"], int(r["batch_id"])): r for r in ctl_rows}
    versions = sorted({ctl_by[b]["rules_version"] for b in batches if b in ctl_by},
                      key=lambda v: (v is not None, v or 0))
    no_ctl = sorted(b for b in batches if b not in ctl_by)
    rec_by_reason: dict[str, int] = {}
    rec_by_flag: dict[str, int] = {}
    if not run:
        out["r11"] = _not_checkable("no bronze row of the run")
    elif dup_ids:
        out["r11"] = _not_checkable(f"{len(dup_ids)} ids have more than one bronze copy "
                                    f"(copies may sit in different batches): {dup_ids[:5]}")
    elif not batches:
        out["r11"] = _not_checkable("no row of the run is in the DLQ, silver or late")
    elif no_ctl:
        out["r11"] = _not_checkable(f"{len(no_ctl)} batches of the run have no control row")
    elif None in versions or len(versions) != 1:
        out["r11"] = _not_checkable(f"the run's batches recorded rules versions {versions}")
    else:
        v = int(versions[0])
        rec_dlq: dict[str, tuple] = {}
        rec_flags: dict[str, tuple[str, ...]] = {}
        for r in run:
            vd = ogt.classify(r.raw_value, topic=r.kafka_topic,
                              partition=int(r.kafka_partition), offset=int(r.kafka_offset),
                              ingest_time=_utc(r.ingest_us), rules_version=v)
            if vd.rejected:
                assert vd.dlq_key is not None and vd.reason_code is not None
                rec_dlq[vd.dlq_key] = (vd.reason_code, vd.reasons, v)
                rec_by_reason[vd.reason_code] = rec_by_reason.get(vd.reason_code, 0) + 1
            else:
                assert vd.event_id is not None
                rec_flags[vd.event_id] = vd.flags
                for f in vd.flags:
                    rec_by_flag[f] = rec_by_flag.get(f, 0) + 1
        act = {r.dlq_key: (r.reason_code, tuple(r.reasons or ()), r.rules_version)
               for r in act_dlq}
        act_f = {r.event_id: (None if r.quality_flags is None else tuple(r.quality_flags))
                 for r in act_acc}
        out["r11"] = {"checkable": True, "rules_version": v, "bronze_rows": len(run),
                      "recomputed_rejections": len(rec_dlq),
                      "recomputed_accepted": len(rec_flags),
                      "by_reason": rec_by_reason, "by_flag": rec_by_flag,
                      "compare": odl.r11_compare(rec_dlq, act, rec_flags, act_f)}

    # --- evidence pieces 3 and 5 (e2e.smoke_bad's table) -----------------------------------
    run_rows_per_batch: dict[tuple[str, int], int] = {}
    for r in [*act_dlq, *act_acc]:
        if r.silver_batch_id is not None:
            k = (r.silver_query_id, int(r.silver_batch_id))
            run_rows_per_batch[k] = run_rows_per_batch.get(k, 0) + 1
    rows_in = sum(int(ctl_by[b]["rows_in"] or 0) for b in batches if b in ctl_by)
    reject_sum: dict[str, int] = {}
    flag_sum: dict[str, int] = {}
    for b in batches:
        if b not in ctl_by:
            continue
        for k, n in (ctl_by[b]["reject_counts"] or {}).items():
            reject_sum[k] = reject_sum.get(k, 0) + int(n)
        for k, n in (ctl_by[b]["flag_counts"] or {}).items():
            flag_sum[k] = flag_sum.get(k, 0) + int(n)
    dlq_ids = {b.event_id: b.expected_code for b in expected_dlq}
    accepted_ids = {r.event_id for r in silver.select("event_id").union(
        late.select("event_id")).join(spark.createDataFrame(
            [(e,) for e in dlq_ids] or [("",)], "event_id string"), "event_id", "left_semi")
        .collect()} if dlq_ids else set()
    in_accepted: dict[str, int] = {}
    for e in accepted_ids:
        in_accepted[dlq_ids[e]] = in_accepted.get(dlq_ids[e], 0) + 1
    flag_keys = {f"id:{e}": b for e, b in flag_events.items()} | {
        f"off:{topic}/{b.kafka_partition}/{b.kafka_offset}": b for b in flag_events.values()}
    in_dlq: dict[str, int] = {}
    for r in dlq_now.where(F.col("dlq_key").isin(list(flag_keys)) if flag_keys
                           else F.lit(False)).select("dlq_key").collect():
        code = flag_keys[r.dlq_key].expected_code
        in_dlq[code] = in_dlq.get(code, 0) + 1
    out["bad_evidence"] = {
        "run_batches": [list(b) for b in sorted(batches)],
        "control_rows_in": rows_in, "run_rows_in_batches": sum(run_rows_per_batch.values()),
        "batches_shared": rows_in != sum(run_rows_per_batch.values()),
        "control_reject_counts": reject_sum, "control_flag_counts": flag_sum,
        "dlq_expected_in_accepted": in_accepted, "flag_expected_in_dlq": in_dlq,
    }

    # --- R14: stranded valid copies (addendum A), at batch C's cut -------------------------
    p3 = [r for r in ctl_rows if r["rules_version"] is not None]
    cut = dl.get("cut")
    if not p3:
        out["r14"] = _not_checkable("no Phase 3 control row")
    elif not cut or None in cut.values():
        out["r14"] = _not_checkable(f"no complete recorded cut ({cut})")
    elif consumed_v is None or consumed_v < 0:
        out["r14"] = _not_checkable(f"bronze version consumed by silver unknown: "
                                    f"{consumed_reason}")
    else:
        d = spark.read.format("delta").option("versionAsOf", cut["dlq"]).load(dlq_path)
        res = spark.read.format("delta").option("versionAsOf", cut["resolutions"]).load(
            resolutions_path).select("dlq_key")
        d_open = (d.where(F.col("key_kind") == dlq_mod.KEY_KIND_ID)
                  .join(res, "dlq_key", "left_anti")
                  .select("event_id", F.col("rules_version").alias("_dlq_rules_version")))
        # An id already in S ∪ L is not stranded: a DLQ row beside it is R9's finding.
        accepted = silver.select("event_id").union(late.select("event_id"))
        d_open = d_open.join(accepted, "event_id", "left_anti")
        b_used = spark.read.format("delta").option("versionAsOf", consumed_v).load(bronze_path)
        found = (b_used.join(d_open, "event_id", "inner")
                 .select(*_RUN_COLS, F.unix_micros("ingest_time").alias("ingest_us"),
                         "_dlq_rules_version").collect())
        stranded: dict[str, int] = {}
        for r in found:
            vd = ogt.classify(r.raw_value, topic=r.kafka_topic,
                              partition=int(r.kafka_partition), offset=int(r.kafka_offset),
                              ingest_time=_utc(r.ingest_us),
                              rules_version=int(r["_dlq_rules_version"]))
            if vd.keyed and not vd.rejected:
                stranded[r.event_id] = stranded.get(r.event_id, 0) + 1
        counter = sum(int(r["dup_of_dlq"] or 0) + int(r["late_dup_of_dlq"] or 0) for r in p3)
        out["r14"] = {"checkable": True, "bronze_version": consumed_v,
                      "d_open_ids": d_open.count(), "copies_checked": len(found),
                      "stranded_ids": len(stranded), "stranded_copies": sum(stranded.values()),
                      "counter_dup_of_dlq": counter,
                      "examples": sorted(stranded)[:odl.MAX_EXAMPLES]}
    return out


# --- oracle R4-R6 (slice 5) --------------------------------------------------------


def _job_key() -> Column:
    """The silver job's key check (streaming/silver.py key_ok, amended by C3: a blank
    event_id is no key), written again here."""
    return (F.coalesce(F.col("parse_ok"), F.lit(False)) & F.col("event_id").isNotNull()
            & (F.trim(F.col("event_id")) != F.lit("")) & F.col("event_time").isNotNull())


def _clamped_us(keyed: Column) -> Column:
    """The job's clamp (streaming/silver.py clamped_event_time), written again here:
    least(event_time, kafka_timestamp) for keyed rows WITH a Kafka time, else NULL.
    least() skips NULLs, so without the explicit check a row with no Kafka time
    would count unclamped; the job never counts it."""
    kts = F.col("kafka_timestamp")
    return F.when(keyed & kts.isNotNull(), F.unix_micros(F.least(F.col("event_time"), kts)))


def _count(cond: Column) -> Column:
    return F.coalesce(F.sum(F.when(cond, 1).otherwise(0)), F.lit(0))


def _median(values: list[int]) -> int | None:
    v = sorted(values)
    return v[len(v) // 2] if v else None


def _with_ctl(df: DataFrame, ctl: DataFrame) -> DataFrame:
    return df.join(ctl, (df.silver_query_id == ctl.ctl_q) & (df.silver_batch_id == ctl.ctl_b),
                   "left")


def _r6_tables(silver: DataFrame, late: DataFrame, control: DataFrame) -> dict[str, Any]:
    """R6a, R6b, R6d over the WHOLE silver and late tables (every query id)."""
    ctl = control.select(F.col("silver_query_id").alias("ctl_q"),
                         F.col("batch_id").alias("ctl_b"),
                         F.col("watermark_delay_s").alias("ctl_w"),
                         F.col("watermark_used").alias("ctl_wm"),
                         F.unix_micros("batch_max_clamped_event_time").alias("ctl_max_us"))
    lj = _with_ctl(late, ctl)
    no_ctl = F.col("ctl_w").isNull()                      # no control row, or pre-slice-4
    ahead = F.col("watermark_used").isNull() | (F.col("event_time") > F.col("watermark_used"))
    wm_diff = ~F.col("watermark_used").eqNullSafe(F.col("ctl_wm"))
    a = lj.agg(F.count("*").alias("rows"), _count(no_ctl).alias("no_ctl"),
               _count(ahead).alias("ahead"), _count(wm_diff).alias("wm_diff"),
               _count(no_ctl | ahead | wm_diff).alias("bad")).first()
    sj = _with_ctl(silver, ctl)
    s4 = F.col("ctl_w").isNotNull()
    behind = s4 & F.col("ctl_wm").isNotNull() & (F.col("event_time") <= F.col("ctl_wm"))
    b = sj.agg(_count(s4).alias("checked"),
               _count(F.col("ctl_b").isNotNull() & ~s4).alias("pre"),
               _count(F.col("ctl_b").isNull()).alias("no_ctl"),
               _count(behind).alias("bad")).first()
    b_ex = [r.event_id for r in sj.where(behind).select("event_id").limit(orc.MAX_EXAMPLES)
            .collect()]
    keyed = F.col("event_id").isNotNull() & F.col("event_time").isNotNull()
    cols = ["silver_query_id", "silver_batch_id", "event_id", "event_time", "kafka_timestamp"]
    inserted = (silver.select(*cols).unionByName(late.select(*cols))
                .withColumn("c_us", _clamped_us(keyed))
                .groupBy("silver_query_id", "silver_batch_id").agg(F.max("c_us").alias("rows_max")))
    dj = _with_ctl(inserted, ctl).where(F.col("ctl_w").isNotNull())
    low = F.col("rows_max").isNotNull() & (F.col("ctl_max_us").isNull()
                                           | (F.col("ctl_max_us") < F.col("rows_max")))
    d = dj.agg(F.count("*").alias("checked"), _count(low).alias("bad")).first()
    d_ex = [{"query_id": r.silver_query_id, "batch_id": r.silver_batch_id,
             "recorded_us": r.ctl_max_us, "rows_max_us": r.rows_max}
            for r in dj.where(low).limit(orc.MAX_EXAMPLES).collect()]
    return {
        "r6a": {"late_rows": int(a["rows"]), "violations": int(a["bad"]),
                "detail": {"without_slice4_control_row": int(a["no_ctl"]),
                           "event_time_after_wm": int(a["ahead"]),
                           "wm_differs_from_control": int(a["wm_diff"])}},
        "r6b": {"silver_rows_checked": int(b["checked"]), "violations": int(b["bad"]),
                "examples": b_ex, "rows_of_pre_slice4_batches": int(b["pre"]),
                "rows_without_control_row": int(b["no_ctl"])},
        "r6d": {"batches_checked": int(d["checked"]), "violations": int(d["bad"]),
                "examples": d_ex},
    }


def oracle_section(spark: SparkSession, manifest: Manifest, expected: DataFrame,
                   silver: DataFrame, late: DataFrame, bronze: DataFrame,
                   control: DataFrame) -> dict[str, Any]:
    """R4-R6, both gates and the timing evidence (report only). Read-only."""
    run_cols = [F.col("event_id"), F.col("silver_query_id"), F.col("silver_batch_id"),
                F.unix_micros("event_time").alias("t_us"),
                F.unix_micros("kafka_timestamp").alias("k_us")]
    s_rows = {r.event_id: r for r in silver.join(expected, "event_id").select(*run_cols)
              .collect()}
    l_rows = {r.event_id: r for r in late.join(expected, "event_id").select(*run_cols)
              .collect()}
    ctl_rows = control.select(
        "silver_query_id", "batch_id", "in_initial_snapshot", "watermark_delay_s",
        F.unix_micros("watermark_used").alias("wm_us"),
        F.unix_micros("batch_max_clamped_event_time").alias("max_us"),
        F.unix_micros("processed_at").alias("at_us"), "rows_in", "late_rows",
        "late_inserted_this_attempt", "ontime_rows", "inserted_this_attempt").collect()
    ctl = {(r.silver_query_id, int(r.batch_id)): orc.ControlRow(
        r.silver_query_id, int(r.batch_id), r.in_initial_snapshot,
        None if r.watermark_delay_s is None else int(r.watermark_delay_s), r.wm_us, r.max_us)
        for r in ctl_rows}
    raw = {(r.silver_query_id, int(r.batch_id)): r for r in ctl_rows}
    run_rows = list(s_rows.values()) + list(l_rows.values())
    run_batches = {(r.silver_query_id, int(r.silver_batch_id)) for r in run_rows}

    w = orc.w_gate(manifest, run_batches, ctl)
    flag = expected.select("event_id", F.lit(True).alias("_run"))
    bj = bronze.join(F.broadcast(flag), "event_id", "left")
    keyed = _job_key()
    iso_row = bj.agg(
        F.min(F.when(F.col("_run") & keyed, F.unix_micros("event_time"))).alias("min_t"),
        F.max(F.when(F.col("_run").isNull(), _clamped_us(keyed))).alias("foreign_max"),
    ).first()
    min_t = iso_row["min_t"]
    iso = orc.isolation_gate(w.get("w_s"), min_t, iso_row["foreign_max"])
    s_ids, l_ids = set(s_rows), set(l_rows)
    out: dict[str, Any] = {
        "w_check": w, "isolation": iso,
        "late_counts": manifest.late_counts, "pace": manifest.pacing.mode
        if manifest.pacing else None,
        "bands": orc.band_counts(manifest, s_ids, l_ids),
        "r4_r5": orc.r4_r5(manifest, s_ids, l_ids)
        if w["checkable"] and iso["checkable"] else None,
        "r6": {**_r6_tables(silver, late, control), "r6c": orc.r6c(ctl.values())},
    }
    out["verdicts"] = orc.verdicts(out)

    # --- timing evidence: reported, never judged (plan S3, S6) -----------------------
    def wm_of(r: Any) -> int | None:
        c = ctl.get((r.silver_query_id, int(r.silver_batch_id)))
        return c.wm_us if c else None

    g = [(raw[k].at_us - r.k_us) // 1000 for r in run_rows
         if (k := (r.silver_query_id, int(r.silver_batch_id))) in raw
         and raw[k].at_us is not None and r.k_us is not None]
    late_ev = manifest.late_events or {}
    very, slight = [], []
    for e, x in sorted(late_ev.items(), key=lambda kv: kv[1].delay_ms):
        r = l_rows.get(e) or s_rows.get(e)
        if r is None:
            continue
        wm = wm_of(r)
        ahead_ms = None if wm is None else (r.t_us - wm) // 1000
        if x.band == "very":
            very.append({"event_id": e, "delay_ms": x.delay_ms, "stream_lag_ms": x.stream_lag_ms,
                         "batch_id": int(r.silver_batch_id), "wm_us": wm,
                         "event_time_minus_wm_ms": ahead_ms, "routed": e in l_ids})
        else:
            slight.append(ahead_ms)
    per_batch = []
    for k in sorted(run_batches, key=lambda k: (k[0], k[1])):
        c = raw.get(k)
        per_batch.append({
            "query_id": k[0], "batch_id": k[1],
            "run_silver": sum(1 for r in s_rows.values()
                              if (r.silver_query_id, int(r.silver_batch_id)) == k),
            "run_late": sum(1 for r in l_rows.values()
                            if (r.silver_query_id, int(r.silver_batch_id)) == k),
            **({"processed_at_us": c.at_us, "wm_us": c.wm_us, "rows_in": c.rows_in,
                "late_rows": c.late_rows, "late_inserted_this_attempt":
                c.late_inserted_this_attempt, "ontime_rows": c.ontime_rows,
                "inserted_this_attempt": c.inserted_this_attempt} if c else {})})
    prior = None
    if run_batches:
        q0, b0 = min(run_batches, key=lambda k: k[1])
        maxima = [c.max_us for c in ctl.values()
                  if c.query_id == q0 and c.batch_id < b0 and c.max_us is not None]
        prior = {"query_id": q0, "first_run_batch": b0,
                 "max_m_k_before_us": max(maxima) if maxima else None,
                 "gap_to_min_t_ms": (min_t - max(maxima)) // 1000
                 if maxima and min_t is not None else None}
    finite = [x for x in slight if x is not None]
    out["timing"] = {
        "g_ms": {"rows": len(g), "median": _median(g), "max": max(g) if g else None},
        "very_events": very,
        "slight_margin_ms": min(finite) if finite else None,
        "slight_with_wm_null": len(slight) - len(finite),
        "run_batches": per_batch,
        "control_before_run": prior,
    }
    return out


def evaluate_silver(rep: dict[str, Any]) -> tuple[bool, list[str]]:
    """(failed, notes) for the silver section."""
    if rep.get("table_missing"):
        return True, [f"silver table {rep['table_path']} does not exist"]
    if rep.get("late_table_missing"):
        return True, [f"late table {rep['late_path']} does not exist"]
    notes: list[str] = []
    failed = False
    rep.setdefault("dlq_ids_in_accepted", 0)
    for key, text in (
        ("missing_ids", "R2: event ids of the run are in neither silver nor late"),
        ("dlq_ids_in_accepted", "R2: event ids the manifest expects in the DLQ are in silver "
                                "or late (v4, C7)"),
        ("in_both_ids", "R1: event ids of the run are in both silver and late"),
        ("ids_in_both_tables", "R1: event ids are in both the silver and the late table"),
        ("duplicate_rows", "R3: extra silver/late rows for the run's ids"),
        ("duplicate_event_ids", "R3: extra rows share an event_id in silver"),
        ("late_duplicate_event_ids", "R3: extra rows share an event_id in late"),
        ("null_key_rows", "R7: silver/late rows with a NULL event_id or event_time"),
        ("ids_not_in_bronze", "silver/late event_ids that bronze does not have"),
    ):
        if rep[key] > 0:
            failed = True
            notes.append(f"{rep[key]} {text}")
    if "oracle" in rep:
        v = orc.verdicts(rep["oracle"])
        failed = failed or "FAIL" in v.values()
        notes += orc.failure_notes(rep["oracle"], v)
    if "dlq" in rep:
        d_failed, d_notes = evaluate_dlq(rep["dlq"])
        failed = failed or d_failed
        notes += d_notes
    cons = rep["consistency"]
    if not cons["checkable"]:
        notes.append(f"consistency not checkable: {cons['reason']}")
        return failed, notes
    if cons["dlq_keyless_rows_upto_batch"] != cons["control_keyless_rows_phase3"]:
        failed = True
        notes.append(f"DLQ keyless rows {cons['dlq_keyless_rows_upto_batch']} != sum "
                     f"keyless_rows {cons['control_keyless_rows_phase3']} of Phase 3 batches")
    if cons["control_excluded_total"] != cons["bronze_keyless_rows"]:
        failed = True
        notes.append(f"R7: control excluded_total {cons['control_excluded_total']} != bronze "
                     f"keyless rows {cons['bronze_keyless_rows']}")
    c = cons["batch"]
    if (cons["control_rows"], cons["control_distinct_batches"],
            cons["control_batch_range"]) != (c + 1, c + 1, [0, c]):
        failed = True
        notes.append(f"control rows of query {cons['query_id']}: {cons['control_rows']} rows, "
                     f"batches {cons['control_batch_range']}; expected one per batch 0..{c}")
    if not cons["rows_checkable"]:
        notes.append(f"silver rows vs bronze ids not checkable: copies up to "
                     f"{cons['max_copy_spread_days']} days apart > horizon "
                     f"{cons['horizon_days']} (both copies are kept by design)")
    elif (cons["silver_rows_upto_batch"] + cons["late_rows_upto_batch"]
          + cons["dlq_open_keyed_rows_upto_batch"] != cons["bronze_keyed_distinct_ids"]):
        failed = True
        notes.append(f"silver rows {cons['silver_rows_upto_batch']} + late rows "
                     f"{cons['late_rows_upto_batch']} + D_open keyed rows "
                     f"{cons['dlq_open_keyed_rows_upto_batch']} != distinct keyed bronze ids "
                     f"{cons['bronze_keyed_distinct_ids']} (bronze versions < "
                     f"{cons['bronze_versions_consumed_below']})")
    return failed, notes


def evaluate_dlq(dl: dict[str, Any]) -> tuple[bool, list[str]]:
    """(failed, notes) for the Phase 3 DLQ part; also stores dl["verdicts"]."""
    notes: list[str] = []
    r8 = dl["r8"]
    if r8["batches_checked"] == 0:
        v8 = "not checkable: no Phase 3 control row"
    else:
        v8 = "FAIL" if r8["violations"] > 0 else "PASS"
    if v8 == "FAIL":
        notes.append(f"R8: {r8['violations']} Phase 3 control rows whose equations do not "
                     f"add up: {r8['examples']}")
    if dl.get("phase3_batches", 0) > 0:
        v9, n9 = odl.r9_verdict(dl["r9"])
        notes += n9
        if not dl.get("dlq_table_exists"):
            v9 = "FAIL"
            notes.append(f"R9: {dl['r9'].get('reason')} after Phase 3 batches ran")
    else:
        v9 = "not checkable: no Phase 3 control row"
    if "r10" in dl:
        v10, n10 = odl.r10_verdicts(dl["r10"])
        notes += n10
    else:
        reason = "not checkable: no DLQ table to look in"
        v10 = {"R10a": reason, "R10b": reason}
    extra: dict[str, str] = {}
    for rule, key, fn in (("R10c", "r10c", odl.r10c_verdict), ("R10d", "r10d", odl.r10d_verdict),
                          ("R11", "r11", odl.r11_verdict), ("R14", "r14", odl.r14_verdict)):
        if key in dl:
            extra[rule], n = fn(dl[key])
            notes += n
        else:
            extra[rule] = "not checkable: not computed"
    dl["verdicts"] = {"R8": v8, "R9": v9, **v10, **extra}
    return "FAIL" in (v8, v9, *v10.values(), *extra.values()), notes


def _versions(text: str) -> list[int]:
    try:
        out = [int(v) for v in text.split(",") if v.strip()]
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected V0,V1 (integers), got {text!r}") from None
    if not out or any(v < 0 for v in out):
        raise argparse.ArgumentTypeError(f"expected non-negative versions, got {text!r}")
    return out


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    p = argparse.ArgumentParser(description="Verify bronze/silver Delta against a manifest")
    p.add_argument("--manifest", required=True, type=Path)
    p.add_argument("--layer", choices=LAYERS, default="all")
    p.add_argument("--table-path", default=DEFAULT_TABLE, help="bronze table")
    p.add_argument("--silver-path", default=DEFAULT_SILVER)
    p.add_argument("--control-path", default=DEFAULT_CONTROL)
    p.add_argument("--late-path", default=DEFAULT_LATE)
    p.add_argument("--silver-checkpoint", default=DEFAULT_SILVER_CHECKPOINT)
    p.add_argument("--dlq-path", default=DEFAULT_DLQ)
    p.add_argument("--resolutions-path", default=DEFAULT_RESOLUTIONS)
    p.add_argument("--dedupe-horizon-days", type=int,
                   default=int(os.environ.get("DEDUPE_HORIZON_DAYS", "7")))
    p.add_argument("--fail-on-duplicates", action="store_true")
    p.add_argument("--versions", type=_versions, default=None,
                   help="also report row counts at these Delta versions, e.g. 12,15")
    args = p.parse_args(argv)

    setup_logging("INFO", component="verify_manifest")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    try:
        m = load_manifest(args.manifest)  # fail fast, before starting a JVM
    except (ManifestError, OSError) as exc:
        log.error("unusable manifest", extra={"fields": {"error": str(exc)}})
        return 2
    if m.bad_list_missing:
        assert m.bad_data is not None
        log.error("unusable manifest", extra={"fields": {"error": (
            f"{args.manifest}: bad events were sent but their list was left out "
            f"({m.bad_data.bad_events_omitted_reason}); bronze R1, R2, R10 and R11 need it")}})
        return 2
    from streaming.spark_session import build_spark

    spark = build_spark("verify_manifest", "local[2]", "1g", shuffle_partitions=4,
                        extra={"spark.ui.enabled": "false"})
    spark.sparkContext.setLogLevel("ERROR")
    report: dict[str, Any] = {}
    silver: dict[str, Any] | None = None
    try:
        if args.layer in ("silver", "all"):  # silver first: bronze is read after it
            silver = verify_silver(spark, args.manifest, args.silver_path, args.late_path,
                                   args.control_path, args.silver_checkpoint, args.table_path,
                                   args.dedupe_horizon_days, args.dlq_path,
                                   args.resolutions_path)
        if args.layer in ("bronze", "all"):
            report = verify(spark, args.manifest, args.table_path, args.versions)
    finally:
        spark.stop()

    notes: list[str] = []
    failed = False
    if report:
        result, notes = evaluate(report, args.fail_on_duplicates)
        failed = result == "FAIL"
    else:
        report = {"manifest": str(args.manifest)}
    if silver is not None:
        s_failed, s_notes = evaluate_silver(silver)
        silver["result"] = "FAIL" if s_failed else "PASS"
        silver["notes"] = s_notes
        report["silver"] = silver
        failed = failed or s_failed
        notes += [f"silver: {n}" for n in s_notes]
    result = "FAIL" if failed else "PASS"
    report["layer"] = args.layer
    report["fail_on_duplicates"] = args.fail_on_duplicates
    report["notes"] = notes
    report["result"] = result
    print(json.dumps(report, indent=2))
    return 0 if result == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
