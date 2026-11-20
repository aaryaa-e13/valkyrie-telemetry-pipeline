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

Accepts manifest v1, v2 and v3 (see common/manifest_reader.py).

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
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common.logging_setup import setup_logging
from common.manifest_reader import Manifest, ManifestError, load_manifest
from streaming import oracle as orc

log = logging.getLogger("streaming.verify_manifest")

DEFAULT_TABLE = "/data/lake/bronze/game_events"
DEFAULT_SILVER = "/data/lake/silver/game_events"
DEFAULT_CONTROL = "/data/lake/_control/silver_batches"
DEFAULT_LATE = "/data/lake/silver/late_events"
DEFAULT_SILVER_CHECKPOINT = "/data/checkpoints/silver"
OFFSET_KEY = ("kafka_topic", "kafka_partition", "kafka_offset")
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
    pairs = list(manifest.event_pairs)
    expected = spark.createDataFrame(pairs, "match_id string, event_id string")
    version = latest_version(spark, table_path)
    rows = spark.read.format("delta").option("versionAsOf", version).load(table_path)

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

    expected_unique = len({event_id for _, event_id in pairs})
    found_unique = int(stats["found_unique"])
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
        "missing_examples": missing_ids,
        "rows_for_these_ids": rows_matched,
        "duplicate_rows": rows_matched - found_unique,
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
    return df.where(F.coalesce(F.col("parse_ok"), F.lit(False)) & F.col("event_id").isNotNull()
                    & F.col("event_time").isNotNull())


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
                  horizon_days: int) -> dict[str, Any]:
    """Silver + late section of the report (see module docstring). Read-only."""
    from delta.tables import DeltaTable

    from streaming.start_guard import committed_source_offset, read_checkpoint_state

    rep: dict[str, Any] = {"table_path": silver_path, "late_path": late_path}
    if not DeltaTable.isDeltaTable(spark, silver_path):
        rep["table_missing"] = True
        return rep
    if not DeltaTable.isDeltaTable(spark, late_path):
        rep["late_table_missing"] = True
        return rep
    manifest = load_manifest(manifest_path)
    expected = spark.createDataFrame([(e,) for _, e in manifest.event_pairs],
                                     "event_id string").distinct()
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

    # --- consistency, pinned to what silver has consumed --------------------------
    cons: dict[str, Any] = {"checkable": False}
    rep["consistency"] = cons
    state = read_checkpoint_state(checkpoint_path)
    committed = committed_source_offset(checkpoint_path)
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
    cons.update(
        checkable=True,
        bronze_rows=b.count(),
        bronze_keyed_distinct_ids=keyed.select("event_id").distinct().count(),
        bronze_keyless_rows=b.count() - keyed.count(),
        max_copy_spread_days=int(spread), horizon_days=horizon_days,
        silver_rows_upto_batch=silver_upto, late_rows_upto_batch=late_upto,
        control_rows=int(ctl["n"]), control_distinct_batches=int(ctl["ids"]),
        control_batch_range=[int(ctl["lo"]), int(ctl["hi"])],
        control_excluded_total=int(ctl["excl"]),
    )
    cons["rows_checkable"] = cons["max_copy_spread_days"] <= horizon_days
    return rep


# --- oracle R4-R6 (slice 5) --------------------------------------------------------


def _job_key() -> Column:
    """The silver job's key check (streaming/silver.py key_ok), written again here."""
    return (F.coalesce(F.col("parse_ok"), F.lit(False)) & F.col("event_id").isNotNull()
            & F.col("event_time").isNotNull())


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
    for key, text in (
        ("missing_ids", "R2: event ids of the run are in neither silver nor late"),
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
    cons = rep["consistency"]
    if not cons["checkable"]:
        notes.append(f"consistency not checkable: {cons['reason']}")
        return failed, notes
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
          != cons["bronze_keyed_distinct_ids"]):
        failed = True
        notes.append(f"silver rows {cons['silver_rows_upto_batch']} + late rows "
                     f"{cons['late_rows_upto_batch']} != distinct keyed bronze ids "
                     f"{cons['bronze_keyed_distinct_ids']} (bronze versions < "
                     f"{cons['bronze_versions_consumed_below']})")
    return failed, notes


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
    p.add_argument("--dedupe-horizon-days", type=int,
                   default=int(os.environ.get("DEDUPE_HORIZON_DAYS", "7")))
    p.add_argument("--fail-on-duplicates", action="store_true")
    p.add_argument("--versions", type=_versions, default=None,
                   help="also report row counts at these Delta versions, e.g. 12,15")
    args = p.parse_args(argv)

    setup_logging("INFO", component="verify_manifest")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    try:
        load_manifest(args.manifest)  # fail fast, before starting a JVM
    except (ManifestError, OSError) as exc:
        log.error("unusable manifest", extra={"fields": {"error": str(exc)}})
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
                                   args.dedupe_horizon_days)
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
