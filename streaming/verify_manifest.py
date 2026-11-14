"""Compare the raw Parquet output against one simulator run manifest.

The topic contains many runs (and test messages), so raw row counts are
meaningless. Instead we take the manifest's acknowledged event_ids (ground
truth) and check, for exactly those ids:

    missing     ids in the manifest but not in Parquet         -> data loss
    duplicates  ids that appear in more than one Parquet row   -> double counting
    per-match   rows per match vs manifest counts

Parquet is read with Spark, so only files committed in the sink's
`_spark_metadata` log are seen (no half-written files from a killed batch).

Usage (inside the Spark container):
    python -m streaming.verify_manifest --manifest manifests/run_X.json
    python -m streaming.verify_manifest --manifest ... --fail-on-duplicates

Exit code: 0 = no missing ids (and no duplicates if --fail-on-duplicates), 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.logging_setup import setup_logging

log = logging.getLogger("streaming.verify_manifest")


def load_manifest_ids(path: Path) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    """Return the manifest and its (match_id, event_id) pairs.

    Raises:
        ValueError: The manifest was written without event ids.
    """
    manifest = json.loads(path.read_text(encoding="utf-8"))
    pairs: list[tuple[str, str]] = []
    for match_id, entry in manifest["matches"].items():
        if "event_ids" not in entry:
            raise ValueError(f"{path} has no event_ids (run used --no-manifest-ids)")
        pairs.extend((match_id, event_id) for event_id in entry["event_ids"])
    return manifest, pairs


def verify(spark: SparkSession, manifest_path: Path, parquet_path: str) -> dict[str, Any]:
    """Compute the comparison report for one manifest."""
    manifest, pairs = load_manifest_ids(manifest_path)
    expected = spark.createDataFrame(pairs, "match_id string, event_id string")
    rows = spark.read.parquet(parquet_path)

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

    expected_unique = len({event_id for _, event_id in pairs})
    found_unique = int(stats["found_unique"])
    rows_matched = int(stats["rows_matched"])
    return {
        "manifest": str(manifest_path),
        "run_id": manifest["run_id"],
        "seed": manifest["seed"],
        "expected_unique_ids": expected_unique,
        "found_unique_ids": found_unique,
        "missing_ids": expected_unique - found_unique,
        "missing_examples": missing_ids,
        "rows_for_these_ids": rows_matched,
        "duplicate_rows": rows_matched - found_unique,
        "ids_with_duplicates": int(stats["ids_duplicated"]),
        "max_copies_of_one_id": int(stats["max_copies"]),
        "parquet_total_rows": rows.count(),
        "parquet_unparsed_rows": rows.filter(~F.col("parse_ok")).count(),
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    p = argparse.ArgumentParser(description="Verify raw Parquet against a run manifest")
    p.add_argument("--manifest", required=True, type=Path)
    p.add_argument("--parquet-path", default="/data/lake/raw/game_events")
    p.add_argument("--fail-on-duplicates", action="store_true")
    args = p.parse_args(argv)

    setup_logging("INFO", component="verify_manifest")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = (
        SparkSession.builder.appName("verify_manifest").master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        report = verify(spark, args.manifest, args.parquet_path)
    finally:
        spark.stop()

    ok = report["missing_ids"] == 0 and (
        not args.fail_on_duplicates or report["duplicate_rows"] == 0
    )
    report["result"] = "PASS" if ok else "FAIL"
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
