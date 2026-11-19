"""Compare the bronze Delta table against one simulator run manifest.

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

Accepts manifest v1, v2 and v3 (see common/manifest_reader.py).

Duplicate event ids are always *reported*. They only cause FAIL with
--fail-on-duplicates: bronze deliberately keeps duplicates (same seed sent
twice, D1.14) until silver dedupes them, so plain verification must not hide
them behind a failure. Missing ids and duplicate offsets always FAIL.

Usage (inside the Spark container):
    python -m streaming.verify_manifest --manifest manifests/run_X.json
    python -m streaming.verify_manifest --manifest ... --fail-on-duplicates

Exit code: 0 PASS, 1 FAIL, 2 unusable manifest or unusable arguments.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.logging_setup import setup_logging
from common.manifest_reader import ManifestError, load_manifest

log = logging.getLogger("streaming.verify_manifest")

DEFAULT_TABLE = "/data/lake/bronze/game_events"
OFFSET_KEY = ("kafka_topic", "kafka_partition", "kafka_offset")


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
    p = argparse.ArgumentParser(description="Verify the bronze Delta table against a manifest")
    p.add_argument("--manifest", required=True, type=Path)
    p.add_argument("--table-path", default=DEFAULT_TABLE)
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
    try:
        report = verify(spark, args.manifest, args.table_path, args.versions)
    finally:
        spark.stop()

    result, notes = evaluate(report, args.fail_on_duplicates)
    report["fail_on_duplicates"] = args.fail_on_duplicates
    report["notes"] = notes
    report["result"] = result
    print(json.dumps(report, indent=2))
    return 0 if result == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
