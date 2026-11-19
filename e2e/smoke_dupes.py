"""smoke-dupes: one fresh seed sent twice; silver must keep exactly the first send.

Steps:
    1. send the seed (tiny config); wait for bronze, then for silver
    2. send the SAME seed again: same event_ids, new Kafka offsets, re-stamped
       event_time (D1.14). Because step 1 waited for silver, the second copies
       reach silver in a LATER batch: this exercises the cross-batch MERGE,
       not the in-batch dedupe.
    3. verify against the FIRST manifest (ground truth), bronze duplicates allowed:
         bronze: every id exactly twice
         silver: every id exactly once, and every silver row is a copy from
                 the first send's offsets (rows_from_this_send == ids)

Exit codes: as e2e.orchestrator (0 PASS, 1 checks failed, 2 setup, 3 catch-up, 4 contiguity).

Usage:
    python -m e2e.smoke_dupes
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from e2e.docker_ops import CommandError
from e2e.orchestrator import (
    EXIT_PASS,
    EXIT_SETUP,
    EXIT_VERIFY,
    MODES,
    Abort,
    Run,
    add_common_args,
    ensure_services,
    say,
    send_and_wait,
    silver_evidence,
    summarize_verify,
    verify_report,
)


def dupes_problems(report: dict[str, Any], expected: int) -> list[str]:
    """Pure check of the verify report for the first manifest (unit-tested)."""
    problems = []
    if report.get("missing_ids"):
        problems.append(f"bronze is missing {report['missing_ids']} ids")
    if (report.get("duplicate_rows"), report.get("max_copies_of_one_id")) != (expected, 2):
        problems.append(f"bronze should hold every id exactly twice: duplicate_rows="
                        f"{report.get('duplicate_rows')} (expected {expected}), max copies "
                        f"{report.get('max_copies_of_one_id')}")
    sv = report.get("silver") or {}
    if sv.get("result") != "PASS":
        problems.append(f"silver verify {sv.get('result')}: {sv.get('notes')}")
    if (sv.get("found_unique_ids"), sv.get("duplicate_rows")) != (expected, 0):
        problems.append(f"silver should hold every id once: found {sv.get('found_unique_ids')}"
                        f"/{expected}, duplicate rows {sv.get('duplicate_rows')}")
    if sv.get("rows_from_this_send") != expected:
        problems.append(f"silver kept {sv.get('rows_from_this_send')} of {expected} rows from "
                        "the FIRST send (the lower offsets); the rest came from the second")
    return problems


def run_dupes(run: Run, args: argparse.Namespace) -> None:
    timeout = args.timeout if args.timeout is not None else MODES["smoke"]["timeout"]
    t0, t0_wall = time.monotonic(), time.time()
    say(f"[1/7] seed {run.seed}, sent twice")
    say("[2/7] checking services and starting Spark")
    ensure_services(start_spark=not args.no_start_spark)
    say("--- send 1 ---")
    m1 = send_and_wait(run, args, timeout, False, key="send1_")
    say("--- send 2 (same seed) ---")
    m2 = send_and_wait(run, args, timeout, False, key="send2_")
    if {e for _, e in m1.event_pairs} != {e for _, e in m2.event_pairs}:
        raise Abort(EXIT_SETUP, "the two sends of one seed have different event ids")
    say("[7/7] verifying against the FIRST manifest (bronze duplicates expected)")
    code, report = verify_report(m1, fail_on_duplicates=False)
    expected = report.get("expected_unique_ids") or 0
    run.summary["verify"] = summarize_verify(report)
    run.summary["verify"]["silver"]["rows_from_this_send"] = (
        (report.get("silver") or {}).get("rows_from_this_send"))
    run.summary["silver_evidence"] = silver_evidence(args, t0_wall)
    problems = dupes_problems(report, expected)
    if code != 0 and not problems:
        problems.append(f"verify exited {code}: {report.get('notes')}")
    run.summary["total_s"] = round(time.monotonic() - t0, 1)
    if problems:
        say(json.dumps(report, indent=2))
        raise Abort(EXIT_VERIFY, "smoke-dupes failed:\n  " + "\n  ".join(problems),
                    short="; ".join(problems))
    sv = report["silver"]
    say(f"PASS: {expected} ids; bronze {expected * 2} rows (each id twice); silver "
        f"{sv['found_unique_ids']} rows, all from the first send; "
        f"{run.summary['total_s']}s total")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="VALKYRIE smoke-dupes (same seed sent twice)")
    add_common_args(p)
    args = p.parse_args(argv)
    seed = args.seed if args.seed is not None else time.time_ns() // 1_000_000
    run = Run(mode="smoke", seed=seed, summary={"mode": "smoke-dupes", "seed": seed})
    code = EXIT_PASS
    try:
        run_dupes(run, args)
    except Abort as exc:
        code = exc.code
        say(f"\nFAIL (exit {code}): {exc}")
        run.summary["error"] = exc.short
    except CommandError as exc:
        code = EXIT_SETUP
        say(f"\nFAIL (exit {code}): {exc}")
        run.summary["error"] = str(exc)
    except KeyboardInterrupt:
        code = 130
        run.summary["error"] = "interrupted"
    run.summary.update(result="PASS" if code == 0 else "FAIL", exit_code=code)
    line = json.dumps(run.summary, separators=(",", ":"))
    if args.json_summary:
        args.json_summary.parent.mkdir(parents=True, exist_ok=True)
        args.json_summary.write_text(line + "\n", encoding="utf-8")
    say(f"SUMMARY {line}")
    return code


if __name__ == "__main__":
    sys.exit(main())
