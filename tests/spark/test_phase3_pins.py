"""Phase 3 slice 0: pin the Spark and Delta behaviours the Phase 3 design relies on.

Each test pins one answer from the slice 0 spikes (docs/plans/phase3-slice0-report.md),
like the least() pin in test_late_routing.py. If a future Spark or Delta changes one of
them, the test says so before the gates, the DLQ or the gold job silently change meaning.

    P1  from_json (PERMISSIVE) + to_raw_rows on odd messages: absent == null; one bad
        field type nulls only that field; '' and blank ids pass parse_ok (the C3 gap);
        how "not a JSON object" can be detected.
    P2  ALTER TABLE ADD COLUMNS array/map on an appendOnly table; a no-op insert-only
        MERGE commits nothing.
    P3  offsets vs commits after query.stop(): idle -> P == C; mid-batch -> P == C + 1.
    P4  a batch overwrite with txnAppId/txnVersion is skipped when the txn is present.
    P5  VACUUM: DRY RUN lists only unreferenced files older than the retention; a
        referenced file is kept whatever its mtime; START/END commits are commitInfo
        only; RETAIN below the default is refused; time travel still works.
    P7  where the 0-row orphan files come from (D2.6): an empty partition of an append,
        and an insert-only MERGE that inserts nothing.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common import delta_log as dl  # noqa: E402
from streaming.bronze import to_raw_rows  # noqa: E402

KAFKA_SCHEMA = StructType([
    StructField("key", BinaryType()), StructField("value", BinaryType()),
    StructField("topic", StringType()), StructField("partition", IntegerType()),
    StructField("offset", LongType()), StructField("timestamp", TimestampType()),
])
BASE = {"event_id": "e1", "event_type": "kill", "schema_version": 1,
        "event_time": "2026-11-26T10:00:00.123Z", "match_id": "m1", "match_seq": 7,
        "patch_version": "14.3", "region": "NA", "player_id": "p_000001",
        "champion_id": "ch_a", "team": None, "victim_id": "p_000002", "item_id": None,
        "gold": None, "winning_team": None, "duration_s": None}
ABSENT = object()
RETAIN_H = 168
# VACUUM lists files with one task per "partition discovery" slot: 10,000 by default,
# which takes ~6 min on local[1] (slice 0). Lowered for these tests only, then restored.
LISTING_PARALLELISM = "spark.sql.sources.parallelPartitionDiscovery.parallelism"


def event(**changes: object) -> str:
    d = dict(BASE)
    for k, v in changes.items():
        if v is ABSENT:
            d.pop(k)
        else:
            d[k] = v
    return json.dumps(d)


def parsed(spark: SparkSession, cases: dict[str, str]) -> dict[str, dict]:
    """to_raw_rows over fake Kafka records; the case name travels in `topic`."""
    ts = datetime(2026, 11, 26, 10, 0, 1, tzinfo=UTC)
    rows = [(b"m1", v.encode(), name, 0, i, ts) for i, (name, v) in enumerate(cases.items())]
    df = to_raw_rows(spark.createDataFrame(rows, KAFKA_SCHEMA)).withColumn(
        "no_object_keys", F.expr("json_object_keys(raw_value)").isNull())
    return {r["kafka_topic"]: r.asDict() for r in df.collect()}


# --- P1 ------------------------------------------------------------------------------


def test_p1_absent_and_null_parse_the_same(spark: SparkSession) -> None:
    r = parsed(spark, {"absent": event(victim_id=ABSENT), "null": event(victim_id=None)})
    assert r["absent"]["victim_id"] is None and r["null"]["victim_id"] is None
    assert r["absent"]["parse_ok"] and r["null"]["parse_ok"]   # not an envelope field


def test_p1_blank_event_ids_pass_parse_ok(spark: SparkSession) -> None:
    """The C3 gap: today key_ok would accept these as real keys."""
    r = parsed(spark, {"empty": event(event_id=""), "blank": event(event_id="   ")})
    assert r["empty"]["event_id"] == "" and r["empty"]["parse_ok"]
    assert r["blank"]["event_id"] == "   " and r["blank"]["parse_ok"]


def test_p1_one_bad_field_type_nulls_only_that_field(spark: SparkSession) -> None:
    r = parsed(spark, {"seq_abc": event(match_seq="abc"), "seq_str": event(match_seq="7"),
                       "seq_float": event(match_seq=1.5), "gold_str": event(gold="100"),
                       "sv_str": event(schema_version="1")})
    for case in ("seq_abc", "seq_str", "seq_float"):           # no string -> int coercion
        assert r[case]["match_seq"] is None and r[case]["event_id"] == "e1"
        assert r[case]["parse_ok"] is False                      # envelope field -> keyless
    assert r["gold_str"]["gold"] is None and r["gold_str"]["parse_ok"] is True
    assert r["gold_str"]["victim_id"] == "p_000002"              # the rest is kept
    assert r["sv_str"]["schema_version"] is None and r["sv_str"]["parse_ok"] is False


def test_p1_event_time_format_is_strict(spark: SparkSession) -> None:
    r = parsed(spark, {"garbage": event(event_time="garbage"),
                       "no_ms": event(event_time="2026-11-26T10:00:00Z"),
                       "number": event(event_time=1652500)})
    for case in r.values():
        assert case["event_time"] is None and case["parse_ok"] is False
        assert case["event_id"] == "e1"


def test_p1_unknown_keys_are_dropped(spark: SparkSession) -> None:
    r = parsed(spark, {"extra": json.dumps({**BASE, "unexpected": 1})})
    assert r["extra"]["parse_ok"] and "unexpected" not in r["extra"]


def test_p1_not_a_json_object(spark: SparkSession) -> None:
    """from_json gives a struct of NULLs (not a NULL struct) for malformed input, so H1
    cannot use `struct IS NULL`. json_object_keys is NULL exactly for non-objects."""
    r = parsed(spark, {"valid": event(), "malformed": '{"event_id": "e1", not json',
                       "array": "[1,2,3]", "number": "42", "empty": "",
                       "truncated": event()[:40], "empty_object": "{}"})
    assert r["valid"]["no_object_keys"] is False
    assert r["empty_object"]["no_object_keys"] is False
    for case in ("malformed", "array", "number", "empty", "truncated"):
        assert r[case]["no_object_keys"] is True, case
        assert r[case]["parse_ok"] is False and r[case]["event_id"] is None


# --- P2 ------------------------------------------------------------------------------


def _append_only(spark: SparkSession, path: str, df: DataFrame) -> None:
    df.coalesce(1).write.format("delta").option("delta.appendOnly", "true").save(path)


def test_p2_add_array_and_map_columns_on_append_only(spark: SparkSession,
                                                     tmp_path: Path) -> None:
    p = str(tmp_path / "t")
    _append_only(spark, p, spark.createDataFrame([("a", 1)], "id string, n long"))
    spark.sql(f"ALTER TABLE delta.`{p}` ADD COLUMNS (flags array<string>, "
              "counts map<string,bigint>)")
    spark.createDataFrame([("b", 2)], "id string, n long").coalesce(1).write \
        .format("delta").mode("append").save(p)                  # a writer without them
    spark.sql("SELECT 'c' AS id, 3L AS n, array('x', 'y') AS flags, map('r', 2L) AS counts"
              ).createOrReplaceTempView("p2_src")
    merge = f"MERGE INTO delta.`{p}` t USING p2_src s ON t.id = s.id " \
            "WHEN NOT MATCHED THEN INSERT *"
    assert spark.sql(merge).first()["num_inserted_rows"] == 1
    v = dl.current_version(p)
    assert spark.sql(merge).first()["num_inserted_rows"] == 0
    assert dl.current_version(p) == v                            # no-op MERGE: no commit
    rows = {r.id: r for r in spark.read.format("delta").load(p).collect()}
    assert rows["a"].flags is None and rows["b"].counts is None
    assert rows["c"].flags == ["x", "y"] and rows["c"].counts == {"r": 2}
    with pytest.raises(Exception, match="(?i)append"):
        spark.sql(f"DELETE FROM delta.`{p}` WHERE id = 'a'")    # still append-only


# --- P3 ------------------------------------------------------------------------------


def _stop_state(spark: SparkSession, tmp: Path, mid_batch: bool) -> tuple[int, int]:
    src, cp = str(tmp / "src"), tmp / "cp"
    spark.createDataFrame([(1,)], "x int").write.format("delta").save(src)
    in_batch_1 = threading.Event()

    def fn(df: DataFrame, batch_id: int) -> None:
        df.count()
        if batch_id == 1 and mid_batch:
            in_batch_1.set()
            time.sleep(6)

    q = (spark.readStream.format("delta").load(src).writeStream
         .option("checkpointLocation", str(cp)).foreachBatch(fn)
         .trigger(processingTime="1 second").start())
    try:
        deadline = time.monotonic() + 90
        while not (cp / "commits" / "0").exists():
            assert time.monotonic() < deadline, "batch 0 never committed"
            time.sleep(0.2)
        spark.createDataFrame([(2,)], "x int").write.format("delta").mode("append").save(src)
        if mid_batch:
            assert in_batch_1.wait(90)
            time.sleep(1)
        else:
            while not (cp / "commits" / "1").exists():
                assert time.monotonic() < deadline, "batch 1 never committed"
                time.sleep(0.2)
            time.sleep(1.5)
    finally:
        q.stop()
    newest = lambda d: max(int(f.name) for f in (cp / d).iterdir() if f.name.isdigit())  # noqa: E731
    return newest("offsets"), newest("commits")


def test_p3_idle_stop_leaves_p_equal_c(spark: SparkSession, tmp_path: Path) -> None:
    assert _stop_state(spark, tmp_path, mid_batch=False) == (1, 1)


def test_p3_stop_during_a_batch_leaves_it_in_flight(spark: SparkSession,
                                                    tmp_path: Path) -> None:
    """stop() waits for the running foreachBatch function, then does not write
    commits/N. So the rules-version guard and replay's P == C check are needed."""
    assert _stop_state(spark, tmp_path, mid_batch=True) == (1, 0)


# --- P4 ------------------------------------------------------------------------------


def test_p4_overwrite_with_a_present_txn_is_skipped(spark: SparkSession,
                                                    tmp_path: Path) -> None:
    p = str(tmp_path / "gold")

    def write(value: str, txn: int) -> None:
        (spark.createDataFrame([(value,)], "v string").coalesce(1).write.format("delta")
         .mode("overwrite").option("txnAppId", "gold").option("txnVersion", txn).save(p))

    write("A", 1)
    write("B", 1)                                                 # crash re-run of run 1
    assert dl.current_version(p) == 0
    assert [r.v for r in spark.read.format("delta").load(p).collect()] == ["A"]
    write("C", 2)
    assert dl.current_version(p) == 1
    assert [r.v for r in spark.read.format("delta").load(p).collect()] == ["C"]


# --- P5 / P7 -------------------------------------------------------------------------


def orphans(table: Path) -> list[Path]:
    """Data files on disk that no add/remove in any commit references."""
    commits = dl.read_commits(table)
    ref = {unquote(a[k]["path"]) for _, acts in commits for a in acts
           for k in ("add", "remove") if k in a}
    return sorted(f for f in table.rglob("*.parquet")
                  if "_delta_log" not in f.parts and str(f.relative_to(table)) not in ref)


@pytest.fixture
def small_listing(spark: SparkSession):  # noqa: ANN201
    before = spark.conf.get(LISTING_PARALLELISM, None)
    spark.conf.set(LISTING_PARALLELISM, "2")
    yield
    if before is None:
        spark.conf.unset(LISTING_PARALLELISM)
    else:
        spark.conf.set(LISTING_PARALLELISM, before)


def _make_old(f: Path) -> None:
    t = time.time() - 8 * 86400
    os.utime(f, (t, t))


def test_p5_vacuum_deletes_only_old_unreferenced_files(spark: SparkSession, tmp_path: Path,
                                                       small_listing: None) -> None:
    t = tmp_path / "t"
    _append_only(spark, str(t), spark.createDataFrame([(1,)], "x long"))
    spark.createDataFrame([(2,), (3,)], "x long").repartition(2).write.format("delta") \
        .mode("append").save(str(t))
    empty = tmp_path / "empty"
    spark.createDataFrame([], "x long").coalesce(1).write.parquet(str(empty))
    src = next(empty.glob("*.parquet"))
    old_orphan, new_orphan = t / "old-orphan.parquet", t / "new-orphan.parquet"
    old_orphan.write_bytes(src.read_bytes())
    new_orphan.write_bytes(src.read_bytes())
    _make_old(old_orphan)
    referenced = t / unquote(dl.live_adds(dl.read_commits(t))[0]["path"])
    _make_old(referenced)                                         # old mtime, but referenced
    assert old_orphan in orphans(t) and new_orphan in orphans(t)
    v0 = dl.current_version(t)

    dry = [r[0] for r in spark.sql(f"VACUUM delta.`{t}` RETAIN {RETAIN_H} HOURS DRY RUN")
           .collect()]
    assert [p.rsplit("/", 1)[-1] for p in dry] == [old_orphan.name]
    assert dl.current_version(t) == v0                            # a dry run writes nothing

    spark.sql(f"VACUUM delta.`{t}` RETAIN {RETAIN_H} HOURS").collect()
    assert not old_orphan.exists() and new_orphan.exists() and referenced.exists()
    new = [acts for v, acts in dl.read_commits(t) if v > v0]
    assert [sorted({k for a in acts for k in a}) for acts in new] == [["commitInfo"]] * 2
    assert [a["commitInfo"]["operation"] for acts in new for a in acts] == \
        ["VACUUM START", "VACUUM END"]
    assert dl.reconcile(dl.read_commits(t))["records"] == 3       # our log reader still works
    for v in range(v0 + 1):                                       # time travel intact
        n = spark.read.format("delta").option("versionAsOf", v).load(str(t)).count()
        assert n == dl.reconcile([c for c in dl.read_commits(t) if c[0] <= v])["records"]


def test_p5_vacuum_below_default_retention_is_refused(spark: SparkSession, tmp_path: Path,
                                                      small_listing: None) -> None:
    t = str(tmp_path / "t")
    _append_only(spark, t, spark.createDataFrame([(1,)], "x long"))
    with pytest.raises(Exception, match="DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT"):
        spark.sql(f"VACUUM delta.`{t}` RETAIN 0 HOURS DRY RUN").collect()


def test_p7_an_empty_partition_of_an_append_leaves_a_0_row_orphan(spark: SparkSession,
                                                                  tmp_path: Path) -> None:
    """The D2.6 0-row files: a 1-row DataFrame in 2 partitions writes one empty file
    that the commit does not list. coalesce(1) writes none."""
    for parts, expected in ((2, 1), (1, 0)):
        t = tmp_path / f"p{parts}"
        _append_only(spark, str(t), spark.createDataFrame([("seed",)], "id string"))
        spark.createDataFrame([("a",)], "id string").repartition(parts).write \
            .format("delta").mode("append").save(str(t))
        found = orphans(t)
        assert len(found) == expected, parts
        assert all(spark.read.parquet(str(f)).count() == 0 for f in found)


def test_p7_a_merge_that_inserts_nothing_leaves_a_0_row_orphan(spark: SparkSession,
                                                               tmp_path: Path) -> None:
    t = tmp_path / "t"
    _append_only(spark, str(t), spark.createDataFrame([("seed",)], "id string"))
    v = dl.current_version(t)
    spark.sql(f"MERGE INTO delta.`{t}` x USING (SELECT 'seed' AS id) s ON x.id = s.id "
              "WHEN NOT MATCHED THEN INSERT *").collect()
    assert dl.current_version(t) == v                             # no commit ...
    found = orphans(t)
    assert len(found) == 1                                        # ... but one empty file
    assert spark.read.parquet(str(found[0])).count() == 0
