"""Measure one smoke target: silver/bronze addBatch per batch + memory (Phase 3 slice 1).

Wraps ONE make target (the target itself does the Kafka writes; this script does none):

    1. starts e2e.memory_sampler (docker stats + Spark REST /executors), stopped by a file;
    2. runs `make <target>` and keeps its full output;
    3. reads `docker logs valkyrie-spark` for the run's window and keeps every
       "batch progress" line with input rows (silver and bronze);
    4. prints one JSON document and appends it to --out.

Method, fixed before the Phase 3 baseline (docs/plans/phase3-slice1-report.md):
    median silver addBatch over the batches with rows, excluding the first silver batch
    after a Spark start (reported separately as `first_after_start_ms`); spread = max - min
    of the per-run medians; memory = container peak over the run, heap peak since JVM start.

Usage (repo root, Spark stopped before the first run of a sequence):
    .venv/bin/python -m bench.smoke_measure smoke --label base-1 --out docs/plans/s1/base.jsonl
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CONTAINER = "valkyrie-spark"


def spark_started_at() -> str | None:
    """Container start time (RFC 3339) or None if there is no container."""
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.StartedAt}}", CONTAINER],
                       capture_output=True, text=True, check=False)
    return r.stdout.strip() if r.returncode == 0 else None


def progress_lines(since: str, until: str) -> list[dict[str, Any]]:
    r = subprocess.run(["docker", "logs", "--since", since, "--until", until, CONTAINER],
                       capture_output=True, text=True, check=False)
    out = []
    for line in (r.stdout + r.stderr).splitlines():
        if '"batch progress"' not in line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("num_input_rows", 0) > 0:
            out.append({k: rec.get(k) for k in ("ts", "query", "batch_id", "num_input_rows",
                                                 "add_batch_ms", "trigger_ms")})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("target", help="make target, e.g. smoke, smoke-late, smoke-dupes")
    p.add_argument("--label", required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--peak-limit-gib", type=float, default=2.8)
    p.add_argument("--no-sampler", action="store_true",
                   help="the target samples memory itself (smoke-late)")
    a = p.parse_args(argv)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    started_before = spark_started_at()
    t0 = datetime.now(UTC)
    stop = Path(tempfile.mkdtemp()) / "stop"
    sampler = None if a.no_sampler else subprocess.Popen(
        [sys.executable, "-m", "e2e.memory_sampler", "--until-file", str(stop),
         "--seconds", "3600", "--label", a.label,
         "--container-peak-gib", str(a.peak_limit_gib)],
        stdout=subprocess.PIPE, text=True)
    m0 = time.monotonic()
    run = subprocess.run(["make", a.target], capture_output=True, text=True, check=False)
    seconds = round(time.monotonic() - m0, 1)
    memory: dict[str, Any] | None = None
    if sampler is not None:
        stop.touch()
        out, _ = sampler.communicate(timeout=120)
        line = next((x for x in out.splitlines() if x.startswith("MEMORY ")), None)
        memory = json.loads(line[len("MEMORY "):]) if line else None
    t1 = datetime.now(UTC)
    started_after = spark_started_at()
    batches = progress_lines(t0.isoformat(), t1.isoformat())
    silver = [b for b in batches if b["query"] == "silver"]
    first_after_start = None
    if started_before != started_after and silver:       # this run started Spark
        first_after_start = silver[0]["add_batch_ms"]
        steady = silver[1:]
    else:
        steady = silver
    ms = [b["add_batch_ms"] for b in steady if b["add_batch_ms"] is not None]
    text = run.stdout + run.stderr
    summary = next((x for x in reversed(text.splitlines()) if x.startswith("SUMMARY")), None)
    doc = {
        "label": a.label, "target": a.target, "exit_code": run.returncode, "seconds": seconds,
        "window": [t0.isoformat(), t1.isoformat()],
        "spark_started_by_this_run": started_before != started_after,
        "silver_batches": len(silver), "first_after_start_ms": first_after_start,
        "silver_add_batch_ms": ms,
        "silver_median_ms": statistics.median(ms) if ms else None,
        "silver_max_ms": max(ms) if ms else None,
        "silver_over_10s": sum(1 for x in ms if x > 10_000),
        "bronze_add_batch_ms": [b["add_batch_ms"] for b in batches if b["query"] == "bronze"],
        "memory": memory, "summary_line": summary,
        "output_tail": text.splitlines()[-25:],
    }
    with a.out.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(doc) + "\n")
    print(json.dumps({k: v for k, v in doc.items() if k != "output_tail"}, indent=1))
    return run.returncode


if __name__ == "__main__":
    raise SystemExit(main())
