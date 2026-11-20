"""wm read and control write cost vs control-table size (DECISIONS D2.4, timing M3).

Runs in the `spark-tools` container (./data mounted READ ONLY) and calls the job's
own functions unchanged: streaming.silver.prior_max_us, write_control_row and
_ensure_table, with the job's session settings (local[4], 2g driver,
snapshotPartitions 4). Writes only to the container's /tmp, which is gone when the
container exits. Never touches the lake, the checkpoints or Kafka.

Two parts:
  live  the live control table, read only: one cold read (DeltaLog cache cleared,
        so a full snapshot load) and the median of 5 warm reads.
  test  a new control table in /tmp, built like the job builds it (appendOnly, one
        row and one commit per batch, Delta's default checkpoint every 10 commits).
        For each row: append the row, then read wm, which is the job's own pattern.
        At a few sizes also a cold read (cache cleared) and a warm read after it.

Output: one JSON document on stdout (every measurement, plus summaries per size
band); progress on stderr.

Usage (from the repo root):
    SPARK_TOOLS_MEM_LIMIT=3g docker compose --profile tools run --rm -T \
      -v ./bench:/probe:ro spark-tools python /probe/wm_probe.py 500 \
      > docs/plans/t1-probe/wm_probe_500.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from statistics import median
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.silver import CONTROL_SCHEMA, _ensure_table, prior_max_us, write_control_row
from streaming.spark_session import build_spark

LIVE_CONTROL = os.environ.get("SILVER_CONTROL_PATH", "/data/lake/_control/silver_batches")
TEST_CONTROL = "/tmp/wm_probe/control"
COLD_AT = (1, 25, 50, 100, 250, 500, 1000, 2000, 5000)
QUERY_ID = "wm-probe"
T0 = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


def clear_delta_cache(spark: SparkSession) -> None:
    """Drop every cached DeltaLog, so the next read loads the snapshot from disk."""
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()  # noqa: SLF001


def timed_ms(fn: Any) -> int:
    t0 = time.monotonic()
    fn()
    return int((time.monotonic() - t0) * 1000)


def p90(values: list[int]) -> int:
    s = sorted(values)
    return s[min(len(s) - 1, int(0.9 * len(s)))]


def live_part(spark: SparkSession) -> dict[str, Any]:
    """Cold + warm reads of the live control table (read only)."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, LIVE_CONTROL):
        return {"skipped": f"{LIVE_CONTROL} is not a Delta table"}
    df = spark.read.format("delta").load(LIVE_CONTROL)
    last = df.agg(F.max_by("silver_query_id", "batch_id").alias("q"),
                  F.max("batch_id").alias("b"), F.count(F.lit(1)).alias("n")).first()
    qid, nxt = str(last["q"]), int(last["b"]) + 1
    hist = DeltaTable.forPath(spark, LIVE_CONTROL).history(1).first()
    clear_delta_cache(spark)
    cold = timed_ms(lambda: prior_max_us(spark, LIVE_CONTROL, qid, nxt))
    warm = [timed_ms(lambda: prior_max_us(spark, LIVE_CONTROL, qid, nxt)) for _ in range(5)]
    return {"rows": int(last["n"]), "version": int(hist["version"]), "cold_ms": cold,
            "warm_ms": warm, "warm_median_ms": median(warm)}


def control_row(i: int) -> dict[str, Any]:
    """A control row for batch i: only the fields the wm read uses carry values."""
    row: dict[str, Any] = {f.name: None for f in CONTROL_SCHEMA.fields}
    row.update(silver_query_id=QUERY_ID, batch_id=i, rows_in=1, processed_at=T0,
               watermark_delay_s=60,
               batch_max_clamped_event_time=T0 + timedelta(seconds=10 * i))
    return row


def test_part(spark: SparkSession, n: int) -> dict[str, Any]:
    """Append n rows one commit each; after each append, read wm(i + 1)."""
    _ensure_table(spark, TEST_CONTROL, CONTROL_SCHEMA, None, "probe control")
    records: list[dict[str, Any]] = []
    for i in range(n):
        write_ms = timed_ms(lambda i=i: write_control_row(spark, TEST_CONTROL, control_row(i)))
        got: list[int | None] = []
        read_ms = timed_ms(lambda i=i, got=got: got.append(
            prior_max_us(spark, TEST_CONTROL, QUERY_ID, i + 1)))
        want = int((T0 + timedelta(seconds=10 * i)).timestamp()) * 1_000_000
        rec: dict[str, Any] = {"rows": i + 1, "version": i + 1, "write_ms": write_ms,
                               "read_ms": read_ms, "checkpoint": (i + 1) % 10 == 0,
                               "value_ok": got[0] == want}
        if i + 1 in COLD_AT:
            clear_delta_cache(spark)
            rec["cold_read_ms"] = timed_ms(lambda i=i: prior_max_us(spark, TEST_CONTROL,
                                                                   QUERY_ID, i + 1))
            rec["warm_after_cold_ms"] = timed_ms(lambda i=i: prior_max_us(
                spark, TEST_CONTROL, QUERY_ID, i + 1))
        records.append(rec)
        if (i + 1) % 50 == 0:
            print(f"wm_probe: {i + 1}/{n} rows", file=sys.stderr, flush=True)
    return {"records": records, "bands": bands(records)}


def bands(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Median/p90 of the read after an append, and median write, per size band."""
    edges = [1, 11, 26, 51, 101, 151, 201, 251, 301, 351, 401, 451, 501, 1001, 2001, 5001]
    out = []
    for lo, hi in zip(edges, edges[1:], strict=False):
        rs = [r for r in records if lo <= r["rows"] < hi]
        if not rs:
            continue
        reads = [r["read_ms"] for r in rs]
        writes = [r["write_ms"] for r in rs if not r["checkpoint"]]
        cp = [r["write_ms"] for r in rs if r["checkpoint"]]
        out.append({"rows": f"{lo}-{hi - 1}", "read_median_ms": median(reads),
                    "read_p90_ms": p90(reads),
                    "write_median_ms": median(writes) if writes else None,
                    "checkpoint_write_median_ms": median(cp) if cp else None})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="wm read cost vs control-table size")
    ap.add_argument("rows", type=int, nargs="?", default=500, help="test table size")
    ap.add_argument("--skip-live", action="store_true", help="do not read the live table")
    args = ap.parse_args(argv)
    if not args.rows >= 1:
        ap.error("rows must be >= 1")
    spark = build_spark("valkyrie-wm-probe", "local[4]", "2g", delta_snapshot_partitions=4)
    spark.sparkContext.setLogLevel("WARN")
    out: dict[str, Any] = {"taken_at": datetime.now(UTC).isoformat(), "spark": spark.version,
                           "settings": {"master": "local[4]", "driver_memory": "2g",
                                        "snapshot_partitions": 4}}
    out["live"] = {"skipped": "--skip-live"} if args.skip_live else live_part(spark)
    out["test"] = test_part(spark, args.rows)
    out["test"]["all_values_ok"] = all(r["value_ok"] for r in out["test"]["records"])
    print(json.dumps(out, indent=1, default=str))
    spark.stop()
    return 0 if out["test"]["all_values_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
