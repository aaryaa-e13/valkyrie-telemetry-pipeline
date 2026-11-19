"""Sample the Spark container's memory while a scenario runs (slice 3, plan S3.10).

Two sources, reported side by side:
    * container memory: `docker stats` every --interval seconds (what the
      laptop actually pays, includes off-heap and Python workers)
    * JVM: Spark's REST API /executors peakMemoryMetrics (JVMHeapMemory,
      JVMOffHeapMemory) and totalGCTime, read once at the end

Stop-and-ask thresholds (plan S3.10): container peak > 2.5 GiB, heap peak
> 1.6 GiB, GC time > 10 % of uptime, or the container OOM-killed / exit 137.
The sampler only REPORTS them (`over_thresholds`); it changes nothing.

Usage:
    python -m e2e.memory_sampler --seconds 120 --label first-start
    python -m e2e.memory_sampler --until-file /tmp/stop --label smoke   # stop on file
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from common import spark_rest

GIB = 1024 ** 3
UNITS = {"B": 1, "KIB": 1024, "MIB": 1024 ** 2, "GIB": GIB, "KB": 1000, "MB": 1000 ** 2,
         "GB": 1000 ** 3}
THRESHOLDS = {"container_peak_gib": 2.5, "heap_peak_gib": 1.6, "gc_share": 0.10}


def parse_mem(text: str) -> int:
    """'1.09GiB / 3GiB' -> bytes used (first number)."""
    m = re.match(r"\s*([\d.]+)\s*([KMG]?i?B)", text, re.IGNORECASE)
    if not m:
        raise ValueError(f"cannot parse memory {text!r}")
    return int(float(m.group(1)) * UNITS[m.group(2).upper()])


def sample(container: str) -> int | None:
    res = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}",
                          container], capture_output=True, text=True, timeout=30)
    if res.returncode != 0 or not res.stdout.strip():
        return None
    return parse_mem(res.stdout.strip())


def jvm_metrics(ui: str) -> dict[str, Any]:
    try:
        app = spark_rest.get_json(f"{ui}/api/v1/applications")[0]
        ex = spark_rest.get_json(f"{ui}/api/v1/applications/{app['id']}/executors")[0]
    except (spark_rest.SparkRestError, IndexError, KeyError) as exc:
        return {"measured": False, "reason": str(exc)}
    peak = ex.get("peakMemoryMetrics") or {}
    uptime_ms = int(app["attempts"][0].get("duration") or 0)
    return {"measured": True, "heap_peak_bytes": peak.get("JVMHeapMemory"),
            "offheap_peak_bytes": peak.get("JVMOffHeapMemory"),
            "max_storage_memory_bytes": ex.get("maxMemory"),
            "gc_ms": ex.get("totalGCTime"), "uptime_ms": uptime_ms}


def summarize(samples: list[int], jvm: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    """Peaks, end value and which thresholds were crossed (pure, unit-tested)."""
    out: dict[str, Any] = {
        "samples": len(samples),
        "container_peak_gib": round(max(samples) / GIB, 3) if samples else None,
        "container_end_gib": round(samples[-1] / GIB, 3) if samples else None,
        "jvm": jvm, "container_state": state}
    over = []
    if out["container_peak_gib"] is not None and \
            out["container_peak_gib"] > THRESHOLDS["container_peak_gib"]:
        over.append(f"container peak {out['container_peak_gib']} GiB > 2.5")
    if jvm.get("measured"):
        heap = (jvm.get("heap_peak_bytes") or 0) / GIB
        out["heap_peak_gib"] = round(heap, 3)
        if heap > THRESHOLDS["heap_peak_gib"]:
            over.append(f"heap peak {heap:.2f} GiB > 1.6")
        if jvm.get("uptime_ms"):
            share = (jvm.get("gc_ms") or 0) / jvm["uptime_ms"]
            out["gc_share"] = round(share, 4)
            if share > THRESHOLDS["gc_share"]:
                over.append(f"GC {share:.1%} of uptime > 10%")
    if state.get("oom_killed") or state.get("exit_code") == 137:
        over.append(f"container OOM / exit 137: {state}")
    out["over_thresholds"] = over
    return out


def container_state(container: str) -> dict[str, Any]:
    res = subprocess.run(["docker", "inspect", "--format",
                          "{{json .State}}", container], capture_output=True, text=True)
    try:
        st = json.loads(res.stdout)
    except ValueError:
        return {"exists": False}
    return {"status": st.get("Status"), "oom_killed": st.get("OOMKilled"),
            "exit_code": st.get("ExitCode")}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="sample Spark container + JVM memory")
    p.add_argument("--container", default="valkyrie-spark")
    p.add_argument("--ui", default="http://localhost:4040")
    p.add_argument("--seconds", type=float, default=120.0)
    p.add_argument("--until-file", type=Path, help="stop early when this file exists")
    p.add_argument("--interval", type=float, default=2.0)
    p.add_argument("--label", default="")
    p.add_argument("--out", type=Path, help="also append the JSON line to this file")
    args = p.parse_args(argv)
    samples: list[int] = []
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        if args.until_file and args.until_file.exists():
            break
        if (b := sample(args.container)) is not None:
            samples.append(b)
        time.sleep(args.interval)
    result = {"label": args.label, **summarize(samples, jvm_metrics(args.ui),
                                               container_state(args.container))}
    line = json.dumps(result, separators=(",", ":"))
    print(f"MEMORY {line}", flush=True)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
