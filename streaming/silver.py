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
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming import faults
from streaming.bronze import table_has_data, table_txn_version
from streaming.config import SilverJobConfig
from streaming.schemas import EVENT_JSON_SCHEMA
from streaming.silver_merge import merge_bounds, merge_condition
from streaming.start_guard import BronzeStartupError, decide, read_checkpoint_state
from streaming.watermark import (
    SnapshotStatusError,
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
])
SLICE3_CONTROL_FIELDS = 16   # columns before slice 4 (the upgrade adds the rest)
LATE_EXTRA_FIELDS = (StructField("watermark_used", TimestampType(), True),
                     StructField("lag_behind_wm_ms", LongType(), True))


class SilverStartupError(BronzeStartupError):
    """Silver refused to start (exit 2). Subclass so one except covers both jobs."""


# --- pure transformations ---------------------------------------------------------


def key_ok() -> Column:
    """D9 key check. parse_ok already implies the other two today; they are checked
    explicitly so a future change to parse_ok cannot let a NULL key through."""
    return (F.coalesce(F.col("parse_ok"), F.lit(False))
            & F.col("event_id").isNotNull() & F.col("event_time").isNotNull())


@dataclass(frozen=True)
class BatchCounts:
    """One aggregation pass over a bronze micro-batch."""

    rows_in: int
    excluded_unparsed: int
    excluded_null_event_id: int
    excluded_null_event_time: int
    excluded_total: int
    keyed_rows: int
    keyed_unique: int
    min_date: date | None
    max_date: date | None
    clamped_max_us: int | None   # m_N: max least(event_time, kafka_timestamp), keyed rows
    clamped_rows: int            # keyed rows with kafka_timestamp < event_time

    @property
    def dup_in_batch(self) -> int:
        return self.keyed_rows - self.keyed_unique


def clamped_event_time() -> Column:
    """least(event_time, kafka_timestamp) for keyed rows with a Kafka time, else NULL.

    The explicit NULL check matters: least() SKIPS NULL arguments (pinned in
    tests/spark/test_late_routing.py), so least(t, NULL) would be the unclamped t.
    A row without a Kafka time therefore never feeds the watermark.
    """
    kts = F.col("kafka_timestamp")
    return F.when(key_ok() & kts.isNotNull(), F.least(F.col("event_time"), kts))


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
    ).first()
    assert r is not None  # an aggregate without grouping always returns one row
    cmax = r["clamped_max_us"]
    return BatchCounts(int(r["rows_in"]), int(r["unparsed"]), int(r["null_id"]),
                       int(r["null_time"]), int(r["excluded"]), int(r["keyed"]),
                       int(r["keyed_unique"]), r["min_date"], r["max_date"],
                       None if cmax is None else int(cmax), int(r["clamped"]))


def dedupe_in_batch(df: DataFrame) -> DataFrame:
    """Keyed rows only, one per event_id: the lowest (topic, partition, offset)."""
    w = Window.partitionBy("event_id").orderBy(*ORDER_KEY)
    return (df.where(key_ok()).withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1).drop("_rn"))


def to_silver_rows(df: DataFrame, query_id: str, batch_id: int,
                   inserted_at: datetime) -> DataFrame:
    """Project bronze rows to the silver columns (no raw_value, no parse_ok: Q5)."""
    return df.select(
        *EVENT_COLUMNS,
        F.to_date("event_time").alias(PARTITION_COLUMN),
        *LINEAGE_COLUMNS,
        F.col("ingest_time").alias("bronze_ingest_time"),
        F.lit(query_id).alias("silver_query_id"),
        F.lit(batch_id).cast("long").alias("silver_batch_id"),
        F.lit(inserted_at).cast("timestamp").alias("silver_inserted_at"),
    )


def silver_schema(bronze_df: DataFrame) -> StructType:
    """Silver schema derived from bronze's, every column nullable (like bronze, D2.2)."""
    shape = to_silver_rows(bronze_df.limit(0), "q", 0, datetime(2026, 1, 1, tzinfo=UTC))
    return StructType([StructField(f.name, f.dataType, True, f.metadata)
                       for f in shape.schema.fields])


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
    """One aggregation over the deduped, flagged rows of a batch."""

    deduped: int
    late: int
    ontime: int
    late_min_date: date | None
    late_max_date: date | None
    ontime_min_date: date | None
    ontime_max_date: date | None


def split_counts(rows: DataFrame) -> SplitCounts:
    """Counts and date bounds per class (rows carry the LATE_FLAG column)."""
    late, d = F.col(LATE_FLAG), F.col(PARTITION_COLUMN)
    one = lambda cond: F.coalesce(F.sum(F.when(cond, 1).otherwise(0)), F.lit(0))  # noqa: E731
    r = rows.agg(F.count(F.lit(1)).alias("n"), one(late).alias("late"), one(~late).alias("ontime"),
                 F.min(F.when(late, d)).alias("llo"), F.max(F.when(late, d)).alias("lhi"),
                 F.min(F.when(~late, d)).alias("olo"), F.max(F.when(~late, d)).alias("ohi"),
                 ).first()
    assert r is not None
    return SplitCounts(int(r["n"]), int(r["late"]), int(r["ontime"]), r["llo"], r["lhi"],
                       r["olo"], r["ohi"])


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


def ensure_silver_tables(spark: SparkSession, cfg: SilverJobConfig) -> None:
    """Create/check silver (partitioned by event_date), control and late (not partitioned).

    One-time upgrade from slice 3 (Q1 (a), logged as WARNING "upgrade"):
      * control: missing slice 4 columns are added (ALTER TABLE ADD COLUMNS, nullable);
      * late: created although the control table already has rows.
    The guard (check_silver_guard) has already refused the cases where this would hide
    lost data (a late table that existed and is gone).
    """
    from delta.tables import DeltaTable

    bronze = spark.read.format("delta").load(cfg.bronze_path)
    _ensure_table(spark, cfg.table_path, silver_schema(bronze), PARTITION_COLUMN, "silver")
    control_had_rows = table_has_data(spark, cfg.control_path)
    _ensure_table(spark, cfg.control_path, CONTROL_SCHEMA, None, "silver control")
    have = set(spark.read.format("delta").load(cfg.control_path).columns)
    missing = [f for f in CONTROL_SCHEMA.fields if f.name not in have]
    if missing:
        cols = ", ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in missing)
        spark.sql(f"ALTER TABLE delta.`{cfg.control_path}` ADD COLUMNS ({cols})")
        log.warning("upgrade: control columns added (slice 4, D2.4)", extra={"fields": {
            "table_path": cfg.control_path, "columns": [f.name for f in missing]}})
    late_existed = DeltaTable.isDeltaTable(spark, cfg.late_path)
    _ensure_table(spark, cfg.late_path, late_schema(bronze), None, "silver late")
    if not late_existed and control_had_rows:
        log.warning("upgrade: late table created for an existing silver (slice 4, D2.4); "
                    "earlier batches had no watermark", extra={"fields": {
                        "table_path": cfg.late_path}})


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
      * the next batch's snapshot status must be readable from the checkpoint (A2).

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
        "watermark_delay_s": cfg.watermark_delay_s}
    recorded = recorded_delays(spark, cfg.control_path)
    problem = _late_table_problem(
        spark, cfg, state.query_id if state.last_planned is not None else None, recorded)
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
    rows: DataFrame          # deduped silver-shaped rows + LATE_FLAG

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
    """Snapshot status, wm(N), counts, dedupe and split; fails before any write.

    Raises:
        SnapshotStatusError: offsets/N-1 cannot say whether N is a snapshot batch.
        SplitError: late + on-time != keyed_unique (a row would be in neither table).
    """
    spark = df.sparkSession
    in_snapshot = batch_starts_in_snapshot(cfg.checkpoint_path, batch_id)
    t0 = time.monotonic()
    prior = None if in_snapshot else prior_max_us(spark, cfg.control_path, query_id, batch_id)
    wm_read_ms = int((time.monotonic() - t0) * 1000)
    wm = watermark_us(prior, cfg.watermark_delay_s, in_snapshot)
    counts = batch_counts(df)
    rows = (to_silver_rows(dedupe_in_batch(df), query_id, batch_id, started)
            .withColumn(LATE_FLAG, is_late(wm)).persist())
    try:
        split = split_counts(rows)
        check_split(counts.keyed_unique, split.deduped, split.late, split.ontime)
    except BaseException:
        rows.unpersist()
        raise
    return BatchPlan(int(batch_id), query_id, started, in_snapshot, wm, wm_read_ms, counts,
                     split, rows)


@dataclass(frozen=True)
class LateResult:
    dup_of_silver: int = 0
    inserted: int = 0
    lo: date | None = None
    hi: date | None = None
    ms: int = 0


def write_late(spark: SparkSession, cfg: SilverJobConfig, plan: BatchPlan) -> LateResult:
    """Step 5: late rows not already in silver -> insert-if-absent into late.

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
                 .where(F.col(PARTITION_COLUMN).between(F.lit(lo), F.lit(hi)))
                 .select("event_id"))
    wm_col = F.timestamp_micros(F.lit(plan.wm_us))
    src = (plan.rows.where(F.col(LATE_FLAG)).drop(LATE_FLAG)
           .join(in_silver, "event_id", "left_anti")
           .withColumn("watermark_used", wm_col)
           .withColumn("lag_behind_wm_ms",
                       F.floor((F.lit(plan.wm_us) - F.unix_micros("event_time")) / 1000)
                       .cast("long"))).persist()
    try:
        candidates = src.count()
        inserted = 0
        if candidates > 0:
            src.createOrReplaceTempView(LATE_VIEW)
            result = spark.sql(
                f"MERGE INTO delta.`{cfg.late_path}` t USING {LATE_VIEW} s "
                "ON t.event_id = s.event_id WHEN NOT MATCHED THEN INSERT *").first()
            inserted = int(result["num_inserted_rows"]) if result is not None else 0
    finally:
        src.unpersist()
    return LateResult(plan.split.late - candidates, inserted, lo, hi,
                      int((time.monotonic() - t0) * 1000))


@dataclass(frozen=True)
class SilverResult:
    dup_of_late: int = 0
    inserted: int = 0
    lo: date | None = None
    hi: date | None = None
    ms: int = 0
    skipped: bool = True


def write_silver(spark: SparkSession, cfg: SilverJobConfig, plan: BatchPlan) -> SilverResult:
    """Step 6: on-time rows not already in late -> insert-only MERGE into silver.

    Bounds: DATE literals from the on-time rows only (never NULL, D2.3). No on-time
    rows left after removing ids already in late: no MERGE, no commit.
    """
    if plan.split.ontime == 0:
        return SilverResult()
    lo, hi = merge_bounds(plan.split.ontime_min_date, plan.split.ontime_max_date,
                          cfg.dedupe_horizon_days)
    in_late = spark.read.format("delta").load(cfg.late_path).select("event_id")
    src = (plan.rows.where(~F.col(LATE_FLAG)).drop(LATE_FLAG)
           .join(in_late, "event_id", "left_anti")).persist()
    try:
        candidates = src.count()
        if candidates == 0:
            return SilverResult(dup_of_late=plan.split.ontime)   # all already in late
        src.createOrReplaceTempView(MERGE_VIEW)
        t0 = time.monotonic()
        result = spark.sql(
            f"MERGE INTO delta.`{cfg.table_path}` t USING {MERGE_VIEW} s "
            f"ON {merge_condition(lo, hi)} WHEN NOT MATCHED THEN INSERT *").first()
        ms = int((time.monotonic() - t0) * 1000)
    finally:
        src.unpersist()
    inserted = int(result["num_inserted_rows"]) if result is not None else 0
    return SilverResult(plan.split.ontime - candidates, inserted, lo, hi, ms, skipped=False)


def control_row(cfg: SilverJobConfig, plan: BatchPlan, late: LateResult,
                silver: SilverResult) -> dict[str, Any]:
    """Step 7's row. Deterministic per batch except *_this_attempt, *_ms, processed_at."""
    c = plan.counts
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
    }


def process_batch(df: DataFrame, batch_id: int, cfg: SilverJobConfig,
                  query_id: str) -> dict[str, Any]:
    """One bronze micro-batch -> late, silver, control row (in that order).

    Returns the control row as a dict (also logged as "silver batch").
    Raises (the batch fails, nothing more is written for it): SnapshotStatusError,
    SplitError, MergeBoundsError.
    """
    spark = df.sparkSession
    started = datetime.now(UTC)
    df = df.persist()
    try:
        plan = plan_batch(df, batch_id, cfg, query_id, started)
        n_late, n_ontime = plan.split.late, plan.split.ontime
        try:
            faults.hit("after_read", batch_id, n_late, n_ontime)   # test-only hooks (D2.6)
            late = write_late(spark, cfg, plan)
            faults.hit("after_late", batch_id, n_late, n_ontime)
            silver = write_silver(spark, cfg, plan)
            faults.hit("after_silver", batch_id, n_late, n_ontime)
        finally:
            plan.release()
        row = control_row(cfg, plan, late, silver)
        write_control_row(spark, cfg.control_path, row)
        faults.hit("after_control", batch_id, n_late, n_ontime)
        log.info("silver batch", extra={"fields": {
            "query": cfg.query_name, **{k: (v.isoformat() if isinstance(v, date) else v)
                                        for k, v in row.items()}}})
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
