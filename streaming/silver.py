"""Silver job: bronze Delta -> silver + late Delta, one row per event_id (D2.3, D2.4).

A streaming query reads the bronze table (Delta source, at most
SILVER_MAX_FILES_PER_TRIGGER files per batch) and, per micro-batch
(`foreachBatch` -> process_batch):

  plan (no writes; plan_batch):
    1. snapshot status (streaming/watermark.py): does batch N START inside the
       Delta source's initial snapshot? Read from offsets/N-1; never defaulted.
    2. wm(N): -infinity in the snapshot, else max(batch_max_clamped_event_time of
       this query's control rows k < N) - W. One small control-table read.
    3. key check (D9): parse_ok AND event_id IS NOT NULL AND event_time IS NOT NULL.
       Rows without a usable key are excluded and counted per reason.
    4. in-batch dedupe: per event_id keep the lowest (kafka_topic, kafka_partition,
       kafka_offset), then the split: late iff event_time <= wm(N). One class per id.
       late + on-time must equal keyed_unique, or the batch fails before any write.
  writes, in this order:
    5. late: late rows minus ids already in silver (silver pruned to the late rows'
       own dates +- H), then insert-if-absent MERGE on event_id into late.
    6. silver: on-time rows minus ids already in late (full scan, late is small),
       then insert-only MERGE with DATE bounds from on-time rows only:
       ON t.event_id = s.event_id AND t.event_date BETWEEN DATE'lo' AND DATE'hi'.
    7. one control row per batch, EVERY batch, a plain append with
       txnAppId = silver query id, txnVersion = the batch id: a replay writes
       nothing. The guard reads that txn.

Replay: steps 1-4 give the same result on every attempt (same bronze range from
offsets/N, same offsets/N-1, same control rows k < N, same W), and 5-7 are
insert-if-absent / txn-skipped. So any crash point leaves the same tables and one
control row per batch. Only *_this_attempt, *_ms and processed_at differ.

Rules for foreachBatch kept here (Delta docs, "Idempotent table writes in
foreachBatch"): no exception is caught or retried inside the batch (only
try/finally to unpersist), so a failure fails the batch and Spark re-runs it
after a restart; txnVersion is always the batch id.

Single writer: this query is the only writer of silver and late. That is what
makes "check the other table, then insert" safe without a cross-table
transaction. Effectively-once, not "exactly-once".

Phase 3 (slice 1; plan docs/plans/phase3-plan.md, D3.1-D3.3):
  * key check amended (C3): a blank event_id ('' or whitespace) is no key.
  * quality gates (streaming/gates.py) in the plan: hard rules -> the dead-letter
    table (DLQ), soft rules -> `quality_flags`. In-batch dedupe prefers a copy that
    passes every hard rule, then the lowest offset (D2.3 for all-valid batches).
  * write order: DLQ -> late -> silver -> cut versions -> control row -> commits/N.
    DLQ rows are keyed by `dlq_key` ('id:<event_id>' or 'off:<topic>/<p>/<offset>').
    The first decision for an id is final: an accepted copy of an id that is open in
    the DLQ is counted (dup_of_dlq, late_dup_of_dlq: a stranded valid copy, D3.1) and
    not inserted; a rejected copy of an accepted id is rejected_dup_of_accepted.
  * the control row records the versions of silver, late, DLQ and resolutions read
    after the batch's writes: a consistent cut for gold and verify (addendum B).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common import delta_log
from streaming import dlq, faults, gates
from streaming.bronze import table_has_data, table_txn_version
from streaming.config import SilverJobConfig
from streaming.schemas import EVENT_JSON_SCHEMA
from streaming.silver_merge import merge_bounds, merge_condition
from streaming.start_guard import BronzeStartupError, decide, read_checkpoint_state
from streaming.watermark import (
    SnapshotStatusError,
    SplitError,
    batch_starts_in_snapshot,
    check_split,
    recorded_delay_problem,
    us_to_datetime,
    watermark_us,
)

log = logging.getLogger("streaming.silver")

PARTITION_COLUMN = "event_date"
APPEND_ONLY_PROPERTY = "delta.appendOnly"
ORDER_KEY = ("kafka_topic", "kafka_partition", "kafka_offset")
EVENT_COLUMNS = tuple(f.name for f in EVENT_JSON_SCHEMA.fields)
LINEAGE_COLUMNS = ("kafka_key", "kafka_topic", "kafka_partition", "kafka_offset",
                   "kafka_timestamp")
MERGE_VIEW = "valkyrie_silver_batch"
LATE_VIEW = "valkyrie_late_batch"
LATE_FLAG = "_is_late"

CONTROL_SCHEMA = StructType([
    StructField("silver_query_id", StringType(), True),
    StructField("batch_id", LongType(), True),
    StructField("rows_in", LongType(), True),
    StructField("excluded_unparsed", LongType(), True),
    StructField("excluded_null_event_id", LongType(), True),
    StructField("excluded_null_event_time", LongType(), True),
    StructField("excluded_total", LongType(), True),
    StructField("dup_in_batch", LongType(), True),
    StructField("keyed_unique", LongType(), True),
    StructField("merge_skipped", BooleanType(), True),
    StructField("merge_lo", DateType(), True),
    StructField("merge_hi", DateType(), True),
    StructField("merge_window_days", IntegerType(), True),
    StructField("inserted_this_attempt", LongType(), True),
    StructField("merge_ms", LongType(), True),
    StructField("processed_at", TimestampType(), True),
    # --- slice 4 (D2.4). Appended at the end, in this order, so ALTER TABLE ADD
    # COLUMNS on a slice 3 table gives exactly this schema.
    StructField("in_initial_snapshot", BooleanType(), True),
    StructField("watermark_delay_s", IntegerType(), True),
    StructField("watermark_used", TimestampType(), True),
    StructField("batch_max_clamped_event_time", TimestampType(), True),
    StructField("clamped_rows", LongType(), True),
    StructField("late_rows", LongType(), True),
    StructField("late_dup_of_silver", LongType(), True),
    StructField("late_inserted_this_attempt", LongType(), True),
    StructField("late_merge_lo", DateType(), True),
    StructField("late_merge_hi", DateType(), True),
    StructField("late_ms", LongType(), True),
    StructField("ontime_rows", LongType(), True),
    StructField("dup_of_late", LongType(), True),
    StructField("wm_read_ms", LongType(), True),
    # --- Phase 3 slice 1 (D3.x). Appended at the end, in this order (ALTER ADD COLUMNS).
    StructField("rules_version", IntegerType(), True),
    StructField("excluded_blank_event_id", LongType(), True),
    StructField("keyless_rows", LongType(), True),
    StructField("rejected_rows", LongType(), True),
    StructField("dlq_keyless_inserted_this_attempt", LongType(), True),
    StructField("dlq_keyless_already_present", LongType(), True),
    StructField("dlq_keyed_inserted_this_attempt", LongType(), True),
    StructField("dlq_keyed_already_present", LongType(), True),
    StructField("rejected_dup_of_accepted", LongType(), True),
    StructField("late_dup_of_dlq", LongType(), True),
    StructField("late_already_present", LongType(), True),
    StructField("dup_of_dlq", LongType(), True),
    StructField("ontime_already_present", LongType(), True),
    StructField("reject_counts", MapType(StringType(), LongType()), True),
    StructField("flag_counts", MapType(StringType(), LongType()), True),
    StructField("dlq_ms", LongType(), True),
    StructField("d_open_read_ms", LongType(), True),
    StructField("silver_version", LongType(), True),
    StructField("late_version", LongType(), True),
    StructField("dlq_version", LongType(), True),
    StructField("resolutions_version", LongType(), True),
    StructField("cut_ms", LongType(), True),
])
SLICE3_CONTROL_FIELDS = 16   # columns before slice 4 (the upgrade adds the rest)
LATE_EXTRA_FIELDS = (StructField("watermark_used", TimestampType(), True),
                     StructField("lag_behind_wm_ms", LongType(), True))
# Phase 3: soft-rule flags on silver and late rows ([] = none; NULL = row from before Phase 3).
QUALITY_FLAGS = StructField("quality_flags", ArrayType(StringType()), True)
REASON, REASONS = "_reason", "_reasons"            # internal plan columns, never written
# Carried in the plan for the DLQ rows; never written to silver or late.
PLAN_EXTRA = (REASON, REASONS, "raw_value", "ingest_date")
INTERNAL_COLUMNS = (LATE_FLAG, *PLAN_EXTRA)

class SilverStartupError(BronzeStartupError):
    """Silver refused to start (exit 2). Subclass so one except covers both jobs."""


# --- pure transformations ---------------------------------------------------------


def key_ok() -> Column:
    """D9 key check, amended in Phase 3 (C3): a blank event_id ('' or whitespace after
    trim) is no key, so blank ids cannot collapse into one silver row (slice 0, P1).
    parse_ok already implies the NULL checks; they stay explicit so a future change to
    parse_ok cannot let a NULL key through."""
    eid = F.col("event_id")
    return (F.coalesce(F.col("parse_ok"), F.lit(False))
            & eid.isNotNull() & (F.trim(eid) != F.lit("")) & F.col("event_time").isNotNull())


@dataclass(frozen=True)
class BatchCounts:
    """One aggregation pass over a bronze micro-batch."""

    rows_in: int
    excluded_unparsed: int
    excluded_null_event_id: int
    excluded_null_event_time: int
    excluded_total: int          # = keyless rows (all go to the DLQ, keyed by offset)
    keyed_rows: int
    keyed_unique: int
    min_date: date | None
    max_date: date | None
    clamped_max_us: int | None   # m_N: max least(event_time, kafka_timestamp), keyed rows
    clamped_rows: int            # keyed rows with kafka_timestamp < event_time
    excluded_blank_event_id: int = 0   # Phase 3 (C3): '' or whitespace event_id

    @property
    def dup_in_batch(self) -> int:
        return self.keyed_rows - self.keyed_unique


def beyond_future_tolerance() -> Column:
    """event_time > ingest_time + FUTURE_TOLERANCE_S: the S1 condition (D1.13), NULL-safe.

    The same expression as gates' S1 flag (pinned by tests/spark/test_dlq_oracle.py).
    """
    return F.coalesce(F.col("event_time") > F.col("ingest_time")
                      + F.expr(f"INTERVAL {gates.FUTURE_TOLERANCE_S} SECONDS"), F.lit(False))


def clamped_event_time() -> Column:
    """least(event_time, kafka_timestamp) for keyed rows with a Kafka time, else NULL.

    The explicit NULL check matters: least() SKIPS NULL arguments (pinned in
    tests/spark/test_late_routing.py), so least(t, NULL) would be the unclamped t.
    A row without a Kafka time therefore never feeds the watermark.

    D3.7: a keyed row past the S1 future tolerance never feeds it either, whatever its
    class. In backfill its Kafka time (the send time) is after every event of the run,
    so the clamp alone would push the wm ahead of the good rows.
    """
    kts = F.col("kafka_timestamp")
    return F.when(key_ok() & kts.isNotNull() & ~beyond_future_tolerance(),
                  F.least(F.col("event_time"), kts))


def batch_counts(df: DataFrame) -> BatchCounts:
    """Counts for the control row, plus m_N for the watermark (keyed rows only).

    A row can hit several exclusion reasons; excluded_total counts it once.
    """
    k = key_ok()
    one = lambda cond: F.coalesce(F.sum(F.when(cond, 1).otherwise(0)), F.lit(0))  # noqa: E731
    keyed_date = F.when(k, F.to_date("event_time"))
    kts = F.col("kafka_timestamp")
    r = df.agg(
        F.count(F.lit(1)).alias("rows_in"),
        one(~F.coalesce(F.col("parse_ok"), F.lit(False))).alias("unparsed"),
        one(F.col("event_id").isNull()).alias("null_id"),
        one(F.col("event_time").isNull()).alias("null_time"),
        one(~k).alias("excluded"),
        one(k).alias("keyed"),
        F.count_distinct(F.when(k, F.col("event_id"))).alias("keyed_unique"),
        F.min(keyed_date).alias("min_date"),
        F.max(keyed_date).alias("max_date"),
        F.max(F.unix_micros(clamped_event_time())).alias("clamped_max_us"),
        one(k & kts.isNotNull() & (kts < F.col("event_time"))).alias("clamped"),
        one(F.col("event_id").isNotNull() & (F.trim(F.col("event_id")) == F.lit("")))
        .alias("blank_id"),
    ).first()
    assert r is not None  # an aggregate without grouping always returns one row
    cmax = r["clamped_max_us"]
    return BatchCounts(int(r["rows_in"]), int(r["unparsed"]), int(r["null_id"]),
                       int(r["null_time"]), int(r["excluded"]), int(r["keyed"]),
                       int(r["keyed_unique"]), r["min_date"], r["max_date"],
                       None if cmax is None else int(cmax), int(r["clamped"]),
                       int(r["blank_id"]))


def with_gates(df: DataFrame, rules_version: int) -> DataFrame:
    """Bronze rows + the hard-rule result (REASON, REASONS) and the soft flags."""
    return (df.withColumn(REASON, gates.reason_code(rules_version))
            .withColumn(REASONS, gates.reasons(rules_version))
            .withColumn(QUALITY_FLAGS.name, gates.flags()))


def dedupe_in_batch(df: DataFrame) -> DataFrame:
    """Keyed rows only, one per event_id. Phase 3: a copy that passes every hard rule
    is preferred (REASON NULL first), then the lowest (topic, partition, offset), as in
    D2.3. Without the REASON column (Phase 2 callers) only the offset order applies."""
    order = ([F.col(REASON).isNotNull()] if REASON in df.columns else []) + list(ORDER_KEY)
    w = Window.partitionBy("event_id").orderBy(*order)
    return (df.where(key_ok()).withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1).drop("_rn"))


def to_silver_rows(df: DataFrame, query_id: str, batch_id: int,
                   inserted_at: datetime, keep: tuple[str, ...] = ()) -> DataFrame:
    """Project bronze rows to the silver columns (no raw_value, no parse_ok: Q5).

    `keep`: extra columns carried along for the plan (e.g. raw_value for the DLQ);
    they are dropped before any silver or late write (INTERNAL_COLUMNS)."""
    return df.select(
        *EVENT_COLUMNS,
        F.to_date("event_time").alias(PARTITION_COLUMN),
        *LINEAGE_COLUMNS,
        F.col("ingest_time").alias("bronze_ingest_time"),
        F.lit(query_id).alias("silver_query_id"),
        F.lit(batch_id).cast("long").alias("silver_batch_id"),
        F.lit(inserted_at).cast("timestamp").alias("silver_inserted_at"),
        *[F.col(c) for c in keep],
    )


def _base_silver_schema(bronze_df: DataFrame) -> StructType:
    shape = to_silver_rows(bronze_df.limit(0), "q", 0, datetime(2026, 1, 1, tzinfo=UTC))
    return StructType([StructField(f.name, f.dataType, True, f.metadata)
                       for f in shape.schema.fields])


def silver_schema(bronze_df: DataFrame) -> StructType:
    """Silver schema derived from bronze's, every column nullable (like bronze, D2.2),
    plus Phase 3's quality_flags at the end (where ALTER ADD COLUMNS puts it)."""
    return StructType([*_base_silver_schema(bronze_df).fields, QUALITY_FLAGS])


def late_schema(bronze_df: DataFrame) -> StructType:
    """Late table = silver's columns + watermark_used + lag_behind_wm_ms (D2.4)."""
    return StructType([*silver_schema(bronze_df).fields, *LATE_EXTRA_FIELDS])


def is_late(wm_us: int | None) -> Column:
    """The split. wm None (-infinity) is a Python-side `false`, never a NULL comparison."""
    if wm_us is None:
        return F.lit(False)
    return F.col("event_time") <= F.timestamp_micros(F.lit(int(wm_us)))


@dataclass(frozen=True)
class SplitCounts:
    """One aggregation over the deduped, flagged rows of a batch.

    `late` and `ontime` count ACCEPTED rows (no hard rule fails); `rejected` counts the
    kept copies that fail a hard rule (they go to the DLQ, keyed by event_id). Without the
    REASON column (Phase 2 callers) every row is accepted.
    """

    deduped: int
    late: int
    ontime: int
    late_min_date: date | None
    late_max_date: date | None
    ontime_min_date: date | None
    ontime_max_date: date | None
    rejected: int = 0
    rejected_min_date: date | None = None
    rejected_max_date: date | None = None
    flag_counts: dict[str, int] = field(default_factory=dict)


def split_counts(rows: DataFrame) -> SplitCounts:
    """Counts and date bounds per class (rows carry LATE_FLAG; Phase 3 also REASON)."""
    d = F.col(PARTITION_COLUMN)
    accepted = F.col(REASON).isNull() if REASON in rows.columns else F.lit(True)
    late, ontime = accepted & F.col(LATE_FLAG), accepted & ~F.col(LATE_FLAG)
    one = lambda cond: F.coalesce(F.sum(F.when(cond, 1).otherwise(0)), F.lit(0))  # noqa: E731
    flagged = QUALITY_FLAGS.name in rows.columns
    flag_aggs = [one(accepted & F.array_contains(F.col(QUALITY_FLAGS.name), code))
                 .alias(f"flag_{code}") for code in gates.SOFT_CODES] if flagged else []
    r = rows.agg(F.count(F.lit(1)).alias("n"), one(late).alias("late"),
                 one(ontime).alias("ontime"), one(~accepted).alias("rejected"),
                 F.min(F.when(late, d)).alias("llo"), F.max(F.when(late, d)).alias("lhi"),
                 F.min(F.when(ontime, d)).alias("olo"), F.max(F.when(ontime, d)).alias("ohi"),
                 F.min(F.when(~accepted, d)).alias("rlo"),
                 F.max(F.when(~accepted, d)).alias("rhi"), *flag_aggs,
                 ).first()
    assert r is not None
    flags = {code: int(r[f"flag_{code}"]) for code in gates.SOFT_CODES
             if flagged and int(r[f"flag_{code}"]) > 0}
    return SplitCounts(int(r["n"]), int(r["late"]), int(r["ontime"]), r["llo"], r["lhi"],
                       r["olo"], r["ohi"], int(r["rejected"]), r["rlo"], r["rhi"], flags)


# --- tables -------------------------------------------------------------------------


def _ensure_table(spark: SparkSession, path: str, schema: StructType,
                  partition: str | None, what: str) -> None:
    """Create if absent (explicit schema, appendOnly); check appendOnly + partitioning."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        b = DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
        if partition:
            b = b.partitionedBy(partition)
        b.property(APPEND_ONLY_PROPERTY, "true").execute()
        log.info(f"{what} table created", extra={"fields": {"table_path": path}})
    detail = DeltaTable.forPath(spark, path).detail().first()
    expected = [partition] if partition else []
    problems = []
    if list(detail["partitionColumns"]) != expected:
        problems.append(f"partition columns {list(detail['partitionColumns'])} "
                        f"(expected {expected})")
    if (detail["properties"] or {}).get(APPEND_ONLY_PROPERTY) != "true":
        problems.append(f"{APPEND_ONLY_PROPERTY} is not 'true'")
    if problems:
        raise SilverStartupError(f"{what} table {path}: " + "; ".join(problems))


def _add_missing_columns(spark: SparkSession, path: str, schema: StructType,
                         what: str, why: str) -> None:
    """ALTER TABLE ADD COLUMNS for the schema's columns the table lacks (nullable).

    Scalar adds on an append-only table: VERIFIED (D2.4); array/map adds: VERIFIED
    (slice 0, P2). Logged as WARNING "upgrade"."""
    have = set(spark.read.format("delta").load(path).columns)
    missing = [f for f in schema.fields if f.name not in have]
    if not missing:
        return
    cols = ", ".join(f"`{f.name}` {f.dataType.simpleString().upper()}" for f in missing)
    spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({cols})")
    log.warning(f"upgrade: {what} columns added ({why})", extra={"fields": {
        "table_path": path, "columns": [f.name for f in missing]}})


def ensure_silver_tables(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """Create/check silver (partitioned by event_date), control, late, and Phase 3's
    dead-letter, resolutions and replays tables (not partitioned; all append-only).

    Upgrades (logged as WARNING "upgrade"; the guard has already refused the cases
    where they would hide lost data):
      * slice 4: control's slice 4 columns; the late table for an existing silver;
      * Phase 3: control's Phase 3 columns; `quality_flags` on silver and late; the
        DLQ, resolutions and replays tables for an existing silver.
    """
    from delta.tables import DeltaTable

    bronze = spark.read.format("delta").load(cfg.bronze_path)
    _ensure_table(spark, cfg.table_path, silver_schema(bronze), PARTITION_COLUMN, "silver")
    _add_missing_columns(spark, cfg.table_path, silver_schema(bronze), "silver", "Phase 3")
    control_had_rows = table_has_data(spark, cfg.control_path)
    _ensure_table(spark, cfg.control_path, CONTROL_SCHEMA, None, "silver control")
    _add_missing_columns(spark, cfg.control_path, CONTROL_SCHEMA, "control",
                         "slice 4 D2.4 / Phase 3")
    late_existed = DeltaTable.isDeltaTable(spark, cfg.late_path)
    _ensure_table(spark, cfg.late_path, late_schema(bronze), None, "silver late")
    _add_missing_columns(spark, cfg.late_path, late_schema(bronze), "late", "Phase 3")
    if not late_existed and control_had_rows:
        log.warning("upgrade: late table created for an existing silver (slice 4, D2.4); "
                    "earlier batches had no watermark", extra={"fields": {
                        "table_path": cfg.late_path}})
    for path, schema, what in ((cfg.dlq_path, dlq.DLQ_SCHEMA, "dead letter"),
                               (cfg.resolutions_path, dlq.RESOLUTIONS_SCHEMA, "resolutions"),
                               (cfg.replays_path, dlq.REPLAYS_SCHEMA, "replays")):
        existed = DeltaTable.isDeltaTable(spark, path)
        _ensure_table(spark, path, schema, None, what)
        if not existed and control_had_rows:
            log.warning(f"upgrade: {what} table created for an existing silver (Phase 3)",
                        extra={"fields": {"table_path": path}})


# --- start-up guard ------------------------------------------------------------------


def recorded_delays(spark: SparkSession, control_path: str) -> dict[str, int]:
    """{query id: W of its newest control row that has one}. {} before the upgrade."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, control_path):
        return {}
    ctl = spark.read.format("delta").load(control_path)
    if "watermark_delay_s" not in ctl.columns:
        return {}
    rows = (ctl.where(F.col("watermark_delay_s").isNotNull()).groupBy("silver_query_id")
            .agg(F.max_by("watermark_delay_s", "batch_id").alias("w")).collect())
    return {r["silver_query_id"]: int(r["w"]) for r in rows}


def _late_table_problem(spark: SparkSession, cfg: SilverJobConfig, query_id: str | None,
                        recorded: dict[str, int]) -> str | None:
    """The late table may be missing only before the first slice 4 batch (upgrade, Q1)."""
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, cfg.late_path):
        return None
    if query_id is not None and query_id in recorded:
        return (f"late table {cfg.late_path} is missing after the upgrade: query {query_id} "
                f"already ran batches with a watermark (watermark_delay_s="
                f"{recorded[query_id]}), so late rows may be lost")
    if recorded:
        other = sorted(recorded)[0]
        return (f"late table {cfg.late_path} is missing, but control rows of query {other} "
                "already have watermark_delay_s: the late table existed and is gone. "
                "Refusing to create an empty one as an upgrade")
    return None


def _phase3_ran(spark: SparkSession, control_path: str) -> bool:
    """True if any control row was written by Phase 3 code (rules_version set)."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, control_path):
        return False
    ctl = spark.read.format("delta").load(control_path)
    return ("rules_version" in ctl.columns
            and ctl.where(F.col("rules_version").isNotNull()).limit(1).count() > 0)


def _dlq_tables_problem(spark: SparkSession, cfg: SilverJobConfig) -> str | None:
    """The DLQ and resolutions tables may be missing only before the first Phase 3 batch."""
    from delta.tables import DeltaTable

    gone = [p for p in (cfg.dlq_path, cfg.resolutions_path)
            if not DeltaTable.isDeltaTable(spark, p)]
    if gone and _phase3_ran(spark, cfg.control_path):
        return (f"{', '.join(gone)} missing, but control rows already have rules_version: "
                "the table existed and is gone, so DLQ decisions may be lost. Refusing to "
                "create an empty one as an upgrade")
    return None


def rules_version_problem(spark: SparkSession, cfg: SilverJobConfig,
                          state: Any) -> str | None:
    """Plan 2.7: QUALITY_RULES_VERSION may change only after a clean stop.

    While batch P is in flight (P > C), its re-run must use the version its first
    attempt used. Evidence for that version, in order: P's control row (T == C + 1),
    P's DLQ rows. Without evidence, batch C's recorded version must match: the first
    attempt's version is unknown, so finishing P with C's version is the only choice
    that cannot differ from it once a writer has run. Phase 2 batches (rules_version
    NULL) have no version: the first Phase 3 batch may follow them. None if fine.
    """
    from delta.tables import DeltaTable

    p, c, qid = state.last_planned, state.last_committed, state.query_id
    if p is None or not qid or (c is not None and p <= c):
        return None
    configured = cfg.rules_version
    by_batch: dict[int, int] = {}
    if DeltaTable.isDeltaTable(spark, cfg.control_path):
        ctl = spark.read.format("delta").load(cfg.control_path)
        if "rules_version" in ctl.columns:
            by_batch = {int(r["batch_id"]): int(r["rules_version"]) for r in ctl.where(
                (F.col("silver_query_id") == qid) & F.col("batch_id").between(p - 1, p)
                & F.col("rules_version").isNotNull()).select("batch_id", "rules_version")
                .collect()}
    seen = {by_batch[p]} if p in by_batch else set()
    if DeltaTable.isDeltaTable(spark, cfg.dlq_path):
        seen |= {int(r["rules_version"]) for r in spark.read.format("delta").load(cfg.dlq_path)
                 .where((F.col("silver_query_id") == qid) & (F.col("silver_batch_id") == p))
                 .select("rules_version").distinct().collect()}
    hint = ("Set it back, start the stream once so batch {p} finishes, stop it cleanly, then "
            "change the version (it applies from the next batch). Not overridable.")
    if seen and seen != {configured}:
        return (f"QUALITY_RULES_VERSION={configured} but batch {p} of query {qid} is in "
                f"flight and its first attempt used rules version {sorted(seen)}. "
                + hint.format(p=p))
    if not seen and (p - 1) in by_batch and by_batch[p - 1] != configured:
        return (f"QUALITY_RULES_VERSION={configured} but batch {p} of query {qid} is in "
                f"flight; its first attempt's version is not recorded and batch {p - 1} "
                f"used rules version {by_batch[p - 1]}. " + hint.format(p=p))
    return None


def check_silver_guard(spark: SparkSession, cfg: SilverJobConfig) -> str:
    """Silver's start-up guard (DECISIONS D2.3, D2.4).

    Same rule as bronze (streaming/start_guard.decide), with the CONTROL table's
    txn in the role of the Delta sink's txn: T = newest txn version the control
    table holds for the silver checkpoint's query id, C = the checkpoint's newest
    committed batch. Start iff T == C, or T == C + 1 (crash after the control row,
    before commits/C+1). Additionally:
      * if the checkpoint has batches, the silver table must exist;
      * the late table must exist, except for the one-time upgrade (no control row
        of any query has watermark_delay_s yet);
    and, never overridable by SILVER_ALLOW_REBUILD:
      * W (WATERMARK_DELAY_S) must equal the W this query id recorded (Q2);
      * the next batch's snapshot status must be readable from the checkpoint (A2);
      * QUALITY_RULES_VERSION may not change while a batch is in flight (Phase 3, 2.7).
    Phase 3 also refuses (overridable, like the late table) a missing DLQ or
    resolutions table once a Phase 3 batch ran.

    NOT refused: a missing/reset silver checkpoint on a populated silver table.
    The new query re-reads bronze; every write is insert-if-absent, so nothing is
    doubled; it is logged as a WARNING.

    Returns "fresh", "rebuild-from-bronze", "resume" or "rebuild" (override).
    Raises SilverStartupError when refused.
    """
    from delta.tables import DeltaTable

    state = read_checkpoint_state(cfg.checkpoint_path)
    fields: dict[str, Any] = {
        "query": cfg.query_name, "checkpoint_path": cfg.checkpoint_path,
        "query_id": state.query_id, "checkpoint_planned": state.last_planned,
        "checkpoint_committed": state.last_committed,
        "watermark_delay_s": cfg.watermark_delay_s, "rules_version": cfg.rules_version}
    recorded = recorded_delays(spark, cfg.control_path)
    problem = _late_table_problem(
        spark, cfg, state.query_id if state.last_planned is not None else None, recorded
    ) or _dlq_tables_problem(spark, cfg)
    if state.last_planned is None:
        if problem is None:
            if table_has_data(spark, cfg.table_path):
                log.warning("silver rebuild from bronze (idempotent): the checkpoint has no "
                            "batches but silver has rows; every write is insert-if-absent "
                            "and nothing is late while the snapshot is re-read",
                            extra={"fields": fields})
                return "rebuild-from-bronze"
            log.info("start-up state", extra={"fields": {**fields, "mode": "fresh"}})
            return "fresh"
    else:
        if state.query_id:
            refusal = recorded_delay_problem(recorded.get(state.query_id),
                                             cfg.watermark_delay_s, state.query_id)
            if refusal:
                raise SilverStartupError(refusal)
            refusal = rules_version_problem(spark, cfg, state)
            if refusal:
                raise SilverStartupError(refusal)
            nxt = 0 if state.last_committed is None else state.last_committed + 1
            try:
                fields["next_batch_in_snapshot"] = batch_starts_in_snapshot(
                    cfg.checkpoint_path, nxt)
            except SnapshotStatusError as exc:
                raise SilverStartupError(f"silver checkpoint {cfg.checkpoint_path}: {exc}. "
                                         "Not overridable (D2.4, A2)") from None
        txn: int | None = None
        if not DeltaTable.isDeltaTable(spark, cfg.table_path):
            problem = (f"checkpoint (query {state.query_id}) has batches but the silver "
                       f"table {cfg.table_path} is missing")
        elif problem is None:
            if state.query_id:
                txn = table_txn_version(spark, cfg.control_path, state.query_id)
            decision = decide(state, txn, lambda: table_has_data(spark, cfg.control_path))
            fields["control_txn"] = txn
            if decision.mode != "refuse":
                log.info("start-up state", extra={"fields": {**fields, "mode": decision.mode,
                                                             "note": decision.note}})
                return decision.mode
            problem = f"control table {cfg.control_path}: {decision.problem}"
    message = (f"silver checkpoint {cfg.checkpoint_path} and silver tables disagree: "
               f"{problem}. Restore the matching checkpoint/tables, or set "
               "SILVER_ALLOW_REBUILD=1 if that is really intended (DECISIONS D2.3, D2.4).")
    if not cfg.allow_rebuild:
        raise SilverStartupError(message)
    log.warning("REBUILD: silver starting although checkpoint and tables disagree",
                extra={"fields": {**fields, "problem": problem}})
    return "rebuild"


def prepare_silver(spark: SparkSession, cfg: SilverJobConfig) -> str:
    """Guard first (it must see the tables as they were), then create/check tables."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, cfg.bronze_path):
        raise SilverStartupError(f"bronze table {cfg.bronze_path} does not exist")
    mode = check_silver_guard(spark, cfg)
    ensure_silver_tables(spark, cfg)
    return mode


# --- per-batch processing -------------------------------------------------------------


class BatchProcessor:
    """The foreachBatch function. Reads its query id from the checkpoint `metadata`
    file (written by Spark before the first batch, stable across restarts)."""

    def __init__(self, cfg: SilverJobConfig) -> None:
        self.cfg = cfg
        self._query_id: str | None = None

    @property
    def query_id(self) -> str:
        if self._query_id is None:
            qid = read_checkpoint_state(self.cfg.checkpoint_path).query_id
            if not qid:
                raise RuntimeError(f"no query id in {self.cfg.checkpoint_path}/metadata")
            self._query_id = qid
        return self._query_id

    def __call__(self, df: DataFrame, batch_id: int) -> None:
        process_batch(df, batch_id, self.cfg, self.query_id)


@dataclass
class BatchPlan:
    """Steps 1-4 of a batch (no writes). `rows` is persisted; call release()."""

    batch_id: int
    query_id: str
    started: datetime
    in_snapshot: bool
    wm_us: int | None
    wm_read_ms: int
    counts: BatchCounts
    split: SplitCounts
    rows: DataFrame          # deduped keyed rows, silver-shaped + LATE_FLAG + PLAN_EXTRA
    keyless: DataFrame | None = None   # the batch's keyless rows + gate columns (lazy)
    rules_version: int = 1

    def release(self) -> None:
        self.rows.unpersist()


def prior_max_us(spark: SparkSession, control_path: str, query_id: str,
                 batch_id: int) -> int | None:
    """max(batch_max_clamped_event_time) over this query's control rows k < batch_id."""
    r = (spark.read.format("delta").load(control_path)
         .where((F.col("silver_query_id") == query_id) & (F.col("batch_id") < batch_id))
         .agg(F.max(F.unix_micros("batch_max_clamped_event_time")).alias("m")).first())
    return None if r is None or r["m"] is None else int(r["m"])


def plan_batch(df: DataFrame, batch_id: int, cfg: SilverJobConfig, query_id: str,
               started: datetime) -> BatchPlan:
    """Snapshot status, wm(N), counts, gates, dedupe and split; fails before any write.

    Raises:
        SnapshotStatusError: offsets/N-1 cannot say whether N is a snapshot batch.
        SplitError: rejected + late + on-time != keyed_unique (a row would be in no
            table, or in two).
    """
    spark = df.sparkSession
    in_snapshot = batch_starts_in_snapshot(cfg.checkpoint_path, batch_id)
    t0 = time.monotonic()
    prior = None if in_snapshot else prior_max_us(spark, cfg.control_path, query_id, batch_id)
    wm_read_ms = int((time.monotonic() - t0) * 1000)
    wm = watermark_us(prior, cfg.watermark_delay_s, in_snapshot)
    counts = batch_counts(df)
    gated = with_gates(df, cfg.rules_version)
    rows = (to_silver_rows(dedupe_in_batch(gated), query_id, batch_id, started,
                           keep=(QUALITY_FLAGS.name, *PLAN_EXTRA))
            .withColumn(LATE_FLAG, is_late(wm)).persist())
    try:
        split = split_counts(rows)
        check_split(counts.keyed_unique, split.deduped, split.late, split.ontime,
                    split.rejected)
    except BaseException:
        rows.unpersist()
        raise
    return BatchPlan(int(batch_id), query_id, started, in_snapshot, wm, wm_read_ms, counts,
                     split, rows, gated.where(~key_ok()), cfg.rules_version)


# --- writes: DLQ -> late -> silver (plan 2.4) -------------------------------------------


def _to_dlq(df: DataFrame, keyed: bool, plan: BatchPlan, ingest_time: str) -> DataFrame:
    """DLQ rows from plan rows (keyed) or bronze rows (keyless)."""
    return df.select(
        dlq.dlq_key(F.lit(keyed)).alias("dlq_key"),
        F.lit(dlq.KEY_KIND_ID if keyed else dlq.KEY_KIND_OFFSET).alias("key_kind"),
        F.col(REASON).alias("reason_code"), F.col(REASONS).alias("reasons"),
        F.lit(plan.rules_version).cast("int").alias("rules_version"),
        "event_id", "event_type", "event_time", "match_id", "match_seq", "raw_value",
        *LINEAGE_COLUMNS,
        F.col(ingest_time).alias("bronze_ingest_time"),
        F.col("ingest_date").alias("bronze_ingest_date"),
        F.lit(plan.query_id).alias("silver_query_id"),
        F.lit(plan.batch_id).cast("long").alias("silver_batch_id"),
        F.lit(plan.started).cast("timestamp").alias("dlq_inserted_at"),
        (F.timestamp_micros(F.lit(plan.wm_us)) if plan.wm_us is not None
         else F.lit(None).cast("timestamp")).alias("watermark_used"),
        F.lit(plan.in_snapshot).alias("in_initial_snapshot"),
    )


@dataclass(frozen=True)
class DlqResult:
    keyless_inserted: int = 0
    keyless_present: int = 0
    keyed_inserted: int = 0
    keyed_present: int = 0
    rejected_dup_of_accepted: int = 0
    reject_counts: dict[str, int] = field(default_factory=dict)
    ms: int = 0


def _in_accepted(spark: SparkSession, cfg: SilverJobConfig, lo: date, hi: date) -> DataFrame:
    """Event ids in S (pruned to [lo, hi], as D2.3) or L (whole; small)."""
    s = (spark.read.format("delta").load(cfg.table_path)
         .where(F.col(PARTITION_COLUMN).between(F.lit(lo), F.lit(hi))).select("event_id"))
    return s.union(spark.read.format("delta").load(cfg.late_path).select("event_id"))


def write_dlq(spark: SparkSession, cfg: SilverJobConfig, plan: BatchPlan) -> DlqResult:
    """Step 2: keyless rows (by offset) and rejected keyed rows (by event_id) -> DLQ.

    A rejected copy of an id already accepted (in S within +-H, or in L) is
    `rejected_dup_of_accepted` and not inserted (the first decision is final).
    Insert-if-absent on dlq_key. Nothing to reject: no read, no commit.

    Raises:
        SplitError: a keyless row passes every hard rule (it would be in no table).
        DlqWriteError: the MERGE inserted another number of rows than planned.
    """
    keyless_n, rejected_n = plan.counts.excluded_total, plan.split.rejected
    if keyless_n == 0 and rejected_n == 0:
        return DlqResult()
    assert plan.keyless is not None
    t0 = time.monotonic()
    keyless = _to_dlq(plan.keyless, False, plan, "ingest_time")
    rejected = _to_dlq(plan.rows.where(F.col(REASON).isNotNull()), True, plan,
                       "bronze_ingest_time")
    reject_counts: dict[str | None, int] = {
        r["reason_code"]: int(r["count"])
        for r in keyless.unionByName(rejected).groupBy("reason_code").count().collect()}
    if None in reject_counts:
        raise SplitError(f"{reject_counts[None]} keyless rows pass every hard rule of rules "
                         f"version {plan.rules_version}: they would be in no table")
    dup_of_accepted = 0
    if rejected_n > 0:
        lo, hi = merge_bounds(plan.split.rejected_min_date, plan.split.rejected_max_date,
                              cfg.dedupe_horizon_days)
        rejected = rejected.join(_in_accepted(spark, cfg, lo, hi), "event_id", "left_anti")
        dup_of_accepted = rejected_n - rejected.count()
    present = spark.read.format("delta").load(cfg.dlq_path).select("dlq_key")
    # unionByName: the anti-join above moved event_id to the first column.
    src = keyless.unionByName(rejected).join(present, "dlq_key", "left_anti").persist()
    try:
        r = src.agg(F.coalesce(F.sum(F.when(F.col("key_kind") == dlq.KEY_KIND_ID, 1)
                                     .otherwise(0)), F.lit(0)).alias("keyed"),
                    F.count(F.lit(1)).alias("n")).first()
        assert r is not None
        new_keyed, new_total = int(r["keyed"]), int(r["n"])
        dlq.insert_if_absent(spark, cfg.dlq_path, src, dlq.DLQ_COLUMNS, "dlq_key",
                             dlq.DLQ_VIEW, new_total)
    finally:
        src.unpersist()
    new_keyless = new_total - new_keyed
    keyed_n = rejected_n - dup_of_accepted
    return DlqResult(new_keyless, keyless_n - new_keyless, new_keyed, keyed_n - new_keyed,
                     dup_of_accepted, {str(k): v for k, v in reject_counts.items()},
                     int((time.monotonic() - t0) * 1000))


def read_d_open(spark: SparkSession, cfg: SilverJobConfig,
                plan: BatchPlan) -> tuple[DataFrame | None, int]:
    """D_open's ids, persisted (release with unpersist), and the read's ms.

    Read AFTER the DLQ write: it then holds this batch's rejected ids too, which are
    never among its accepted ids (one copy per id), so every attempt of the batch
    sees the same set for its accepted rows. No accepted rows: no read."""
    if plan.split.late + plan.split.ontime == 0:
        return None, 0
    t0 = time.monotonic()
    ids = dlq.open_keyed_ids(spark, cfg.dlq_path, cfg.resolutions_path).persist()
    ids.count()
    return ids, int((time.monotonic() - t0) * 1000)


def _classify(src: DataFrame, other: DataFrame, d_open: DataFrame | None) -> DataFrame:
    """src + _in_other / _in_dlq flags (the first decision is final, plan 2.6)."""
    out = src.join(other.select("event_id").distinct().withColumn("_in_other", F.lit(True)),
                   "event_id", "left")
    if d_open is None:
        out = out.withColumn("_in_dlq", F.lit(False))
    else:
        out = out.join(d_open.withColumn("_in_dlq", F.lit(True)), "event_id", "left")
    return (out.withColumn("_in_other", F.coalesce(F.col("_in_other"), F.lit(False)))
            .withColumn("_in_dlq", F.coalesce(F.col("_in_dlq"), F.lit(False))))


def _classified_counts(df: DataFrame) -> tuple[int, int, int]:
    """(in the other table, in D_open only, candidates)."""
    one = lambda cond: F.coalesce(F.sum(F.when(cond, 1).otherwise(0)), F.lit(0))  # noqa: E731
    r = df.agg(one(F.col("_in_other")).alias("o"),
               one(~F.col("_in_other") & F.col("_in_dlq")).alias("d"),
               one(~F.col("_in_other") & ~F.col("_in_dlq")).alias("c")).first()
    assert r is not None
    return int(r["o"]), int(r["d"]), int(r["c"])


def _target_columns(rows: DataFrame, extra: tuple[str, ...] = ()) -> tuple[str, ...]:
    return (*[c for c in rows.columns if c not in INTERNAL_COLUMNS], *extra)


@dataclass(frozen=True)
class LateResult:
    dup_of_silver: int = 0
    dup_of_dlq: int = 0
    inserted: int = 0
    already_present: int = 0
    lo: date | None = None
    hi: date | None = None
    ms: int = 0


def write_late(spark: SparkSession, cfg: SilverJobConfig, plan: BatchPlan,
               d_open: DataFrame | None = None) -> LateResult:
    """Step 3: accepted late rows in neither silver nor D_open -> insert-if-absent into late.

    The silver lookup is pruned to the late rows' own dates +- H (late_merge_lo/hi),
    so one old late row never widens silver's MERGE window. No late rows: no read,
    no commit.
    """
    if plan.split.late == 0:
        return LateResult()
    assert plan.wm_us is not None   # late rows exist only with a finite watermark
    t0 = time.monotonic()
    lo, hi = merge_bounds(plan.split.late_min_date, plan.split.late_max_date,
                          cfg.dedupe_horizon_days)
    in_silver = (spark.read.format("delta").load(cfg.table_path)
                 .where(F.col(PARTITION_COLUMN).between(F.lit(lo), F.lit(hi))))
    late_rows = plan.rows.where(F.col(LATE_FLAG) & F.col(REASON).isNull())
    cols = _target_columns(plan.rows, tuple(f.name for f in LATE_EXTRA_FIELDS))
    src = (_classify(late_rows, in_silver, d_open)
           .withColumn("watermark_used", F.timestamp_micros(F.lit(plan.wm_us)))
           .withColumn("lag_behind_wm_ms",
                       F.floor((F.lit(plan.wm_us) - F.unix_micros("event_time")) / 1000)
                       .cast("long"))).persist()
    try:
        in_s, in_d, candidates = _classified_counts(src)
        inserted = 0
        if candidates > 0:
            src.where(~F.col("_in_other") & ~F.col("_in_dlq")).select(*cols) \
               .createOrReplaceTempView(LATE_VIEW)
            result = spark.sql(
                f"MERGE INTO delta.`{cfg.late_path}` t USING {LATE_VIEW} s "
                "ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT "
                f"({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})").first()
            inserted = int(result["num_inserted_rows"]) if result is not None else 0
    finally:
        src.unpersist()
    return LateResult(in_s, in_d, inserted, candidates - inserted, lo, hi,
                      int((time.monotonic() - t0) * 1000))


@dataclass(frozen=True)
class SilverResult:
    dup_of_late: int = 0
    dup_of_dlq: int = 0
    inserted: int = 0
    already_present: int = 0
    lo: date | None = None
    hi: date | None = None
    ms: int = 0
    skipped: bool = True


def write_silver(spark: SparkSession, cfg: SilverJobConfig, plan: BatchPlan,
                 d_open: DataFrame | None = None) -> SilverResult:
    """Step 4: accepted on-time rows in neither late nor D_open -> insert-only MERGE.

    Bounds: DATE literals from the on-time rows only (never NULL, D2.3). No candidate
    left after removing ids already in late or D_open: no MERGE, no commit.
    """
    if plan.split.ontime == 0:
        return SilverResult()
    lo, hi = merge_bounds(plan.split.ontime_min_date, plan.split.ontime_max_date,
                          cfg.dedupe_horizon_days)
    in_late = spark.read.format("delta").load(cfg.late_path)
    ontime = plan.rows.where(~F.col(LATE_FLAG) & F.col(REASON).isNull())
    cols = _target_columns(plan.rows)
    src = _classify(ontime, in_late, d_open).persist()
    try:
        in_l, in_d, candidates = _classified_counts(src)
        if candidates == 0:
            return SilverResult(dup_of_late=in_l, dup_of_dlq=in_d)
        src.where(~F.col("_in_other") & ~F.col("_in_dlq")).select(*cols) \
           .createOrReplaceTempView(MERGE_VIEW)
        t0 = time.monotonic()
        result = spark.sql(
            f"MERGE INTO delta.`{cfg.table_path}` t USING {MERGE_VIEW} s "
            f"ON {merge_condition(lo, hi)} WHEN NOT MATCHED THEN INSERT "
            f"({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})").first()
        ms = int((time.monotonic() - t0) * 1000)
    finally:
        src.unpersist()
    inserted = int(result["num_inserted_rows"]) if result is not None else 0
    return SilverResult(in_l, in_d, inserted, candidates - inserted, lo, hi, ms,
                        skipped=False)


@dataclass(frozen=True)
class Cut:
    """Table versions read after a batch's writes (addendum B): a consistent cut."""

    silver: int | None
    late: int | None
    dlq: int | None
    resolutions: int | None
    ms: int
    ms_each: dict[str, int]


def read_cut(cfg: SilverJobConfig) -> Cut:
    """The 4 versions, from the Delta logs (stdlib, no Spark job), each timed."""
    out: dict[str, int | None] = {}
    ms_each: dict[str, int] = {}
    t_all = time.monotonic()
    for name, path in (("silver", cfg.table_path), ("late", cfg.late_path),
                       ("dlq", cfg.dlq_path), ("resolutions", cfg.resolutions_path)):
        t0 = time.monotonic()
        out[name] = delta_log.current_version(path)
        ms_each[name] = int((time.monotonic() - t0) * 1000)
    return Cut(out["silver"], out["late"], out["dlq"], out["resolutions"],
               int((time.monotonic() - t_all) * 1000), ms_each)


def control_row(cfg: SilverJobConfig, plan: BatchPlan, late: LateResult,
                silver: SilverResult, dlq_res: DlqResult | None = None,
                cut: Cut | None = None, d_open_ms: int = 0) -> dict[str, Any]:
    """Step 5's row. Deterministic per batch except *_this_attempt, *_already_present,
    *_ms and processed_at (their sums per class are deterministic, plan 2.5)."""
    c = plan.counts
    dr = dlq_res or DlqResult()
    return {
        "silver_query_id": plan.query_id, "batch_id": plan.batch_id, "rows_in": c.rows_in,
        "excluded_unparsed": c.excluded_unparsed,
        "excluded_null_event_id": c.excluded_null_event_id,
        "excluded_null_event_time": c.excluded_null_event_time,
        "excluded_total": c.excluded_total, "dup_in_batch": c.dup_in_batch,
        "keyed_unique": c.keyed_unique, "merge_skipped": silver.skipped,
        "merge_lo": silver.lo, "merge_hi": silver.hi,
        "merge_window_days": (silver.hi - silver.lo).days + 1 if silver.lo and silver.hi
        else None,
        "inserted_this_attempt": silver.inserted, "merge_ms": silver.ms,
        "processed_at": plan.started,
        "in_initial_snapshot": plan.in_snapshot,
        "watermark_delay_s": cfg.watermark_delay_s,
        "watermark_used": us_to_datetime(plan.wm_us),
        "batch_max_clamped_event_time": us_to_datetime(c.clamped_max_us),
        "clamped_rows": c.clamped_rows,
        "late_rows": plan.split.late, "late_dup_of_silver": late.dup_of_silver,
        "late_inserted_this_attempt": late.inserted,
        "late_merge_lo": late.lo, "late_merge_hi": late.hi, "late_ms": late.ms,
        "ontime_rows": plan.split.ontime, "dup_of_late": silver.dup_of_late,
        "wm_read_ms": plan.wm_read_ms,
        # Phase 3 (plan 2.5, 2.7; addenda A and B)
        "rules_version": plan.rules_version,
        "excluded_blank_event_id": c.excluded_blank_event_id,
        "keyless_rows": c.excluded_total,
        "rejected_rows": plan.split.rejected,
        "dlq_keyless_inserted_this_attempt": dr.keyless_inserted,
        "dlq_keyless_already_present": dr.keyless_present,
        "dlq_keyed_inserted_this_attempt": dr.keyed_inserted,
        "dlq_keyed_already_present": dr.keyed_present,
        "rejected_dup_of_accepted": dr.rejected_dup_of_accepted,
        "late_dup_of_dlq": late.dup_of_dlq,
        "late_already_present": late.already_present,
        "dup_of_dlq": silver.dup_of_dlq,
        "ontime_already_present": silver.already_present,
        "reject_counts": dict(dr.reject_counts),
        "flag_counts": dict(plan.split.flag_counts),
        "dlq_ms": dr.ms, "d_open_read_ms": d_open_ms,
        "silver_version": cut.silver if cut else None,
        "late_version": cut.late if cut else None,
        "dlq_version": cut.dlq if cut else None,
        "resolutions_version": cut.resolutions if cut else None,
        "cut_ms": cut.ms if cut else None,
    }


def process_batch(df: DataFrame, batch_id: int, cfg: SilverJobConfig,
                  query_id: str) -> dict[str, Any]:
    """One bronze micro-batch -> DLQ, late, silver, cut, control row (in that order).

    Returns the control row as a dict (also logged as "silver batch").
    Raises (the batch fails, nothing more is written for it): SnapshotStatusError,
    SplitError, MergeBoundsError, DlqWriteError.
    """
    spark = df.sparkSession
    started = datetime.now(UTC)
    df = df.persist()
    try:
        plan = plan_batch(df, batch_id, cfg, query_id, started)
        n_late, n_ontime = plan.split.late, plan.split.ontime
        d_open: DataFrame | None = None
        try:
            faults.hit("after_read", batch_id, n_late, n_ontime)   # test-only hooks (D2.6)
            dlq_res = write_dlq(spark, cfg, plan)
            faults.hit("after_dlq", batch_id, n_late, n_ontime)
            d_open, d_open_ms = read_d_open(spark, cfg, plan)
            late = write_late(spark, cfg, plan, d_open)
            faults.hit("after_late", batch_id, n_late, n_ontime)
            silver = write_silver(spark, cfg, plan, d_open)
            faults.hit("after_silver", batch_id, n_late, n_ontime)
        finally:
            plan.release()
            if d_open is not None:
                d_open.unpersist()
        cut = read_cut(cfg)
        row = control_row(cfg, plan, late, silver, dlq_res, cut, d_open_ms)
        write_control_row(spark, cfg.control_path, row)
        faults.hit("after_control", batch_id, n_late, n_ontime)
        stranded = late.dup_of_dlq + silver.dup_of_dlq
        if stranded:
            log.warning("valid copy stranded in DLQ (D3.1: fixed in Phase 4)", extra={
                "fields": {"query": cfg.query_name, "batch_id": int(batch_id),
                           "dup_of_dlq": silver.dup_of_dlq,
                           "late_dup_of_dlq": late.dup_of_dlq}})
        log.info("silver batch", extra={"fields": {
            "query": cfg.query_name, "cut_ms_each": cut.ms_each,
            **{k: (v.isoformat() if isinstance(v, date) else v) for k, v in row.items()}}})
        return row
    finally:
        df.unpersist()


def write_control_row(spark: SparkSession, control_path: str, row: dict[str, Any]) -> None:
    """Append the control row with txn(appId = query id, version = batch id).

    Delta skips the write if this txn version is already in the table, so a
    replayed batch never adds a second control row.
    """
    (spark.createDataFrame([row], CONTROL_SCHEMA).write.format("delta").mode("append")
     .option("txnAppId", row["silver_query_id"]).option("txnVersion", row["batch_id"])
     .save(control_path))


def start_silver_query(spark: SparkSession, cfg: SilverJobConfig,
                       trigger: dict[str, Any] | None = None) -> StreamingQuery:
    """Start the streaming read of bronze with process_batch as the sink."""
    Path(cfg.checkpoint_path).parent.mkdir(parents=True, exist_ok=True)
    writer = (spark.readStream.format("delta")
              .option("maxFilesPerTrigger", cfg.max_files_per_trigger)
              .load(cfg.bronze_path)
              .writeStream.queryName(cfg.query_name)
              .option("checkpointLocation", cfg.checkpoint_path)
              .foreachBatch(BatchProcessor(cfg)))
    writer = writer.trigger(**(trigger or {"processingTime": cfg.trigger_interval}))
    return writer.start()
