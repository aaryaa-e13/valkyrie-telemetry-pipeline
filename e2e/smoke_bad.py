"""smoke-bad: one REAL backfill run with deliberate bad data; every gate must FIRE (D3.7).

Steps:
    1. fresh base seed
    2. dry run (no Kafka): generator + corrupt.py only. Search seeds upward from the base
       until every gate H1-H10 and S1-S4, every enabled kind, both wrong_type variants
       and both H7 key kinds have >= 1 event (at most MAX_SEEDS, else exit 2 naming the
       uncovered units). The seed used and the number tried go into SUMMARY.
    3. send simulator/config.bad.yaml with that seed (backfill); bronze, then silver
       catch-up (as e2e.orchestrator)
    4. verify --layer all in spark-tools: R1-R3 (v4), R8, R9, R10a-d, R11, R14 and the
       consistency section
    5. the evidence table (judge below): one row per gate, 5 pieces each, all equal k > 0

Exit codes:
    0 PASS: every gate has all 5 pieces equal to k > 0 and every rule PASS
    1 verify FAIL (a rule failed, or missing / duplicate ids)
    2 setup, or "not checkable" (never PASS; Q2: batches shared with another producer)
    3, 4 as e2e.orchestrator (catch-up, contiguity)
    6 a gate not exercised, or a piece that does not equal k (EXIT_NOT_FIRED)

Memory is measured from outside: bench/smoke_measure.py wraps `make smoke-bad`.

Usage:
    python -m e2e.smoke_bad
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from common.manifest_reader import BadData
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
    summarize_verify,
    union_line,
    verify_report,
)
from simulator import corrupt
from simulator.config import load_config
from simulator.generator import generate_events

CONFIG = MODES["bad"]["config"]
EXIT_NOT_CHECKABLE = 2
EXIT_NOT_FIRED = 6               # as e2e.smoke_faults
MAX_SEEDS = 1000
RULES = ("R8", "R9", "R10a", "R10b", "R10c", "R10d", "R11", "R14")
PIECES = ("manifest", "tables", "control", "recompute", "not_elsewhere")


# --- 2. the dry-run seed search ----------------------------------------------------------

def search_seed(base_seed: int, config_path: str = CONFIG,
                max_seeds: int = MAX_SEEDS) -> corrupt.SeedSearch:
    """The first seed >= base_seed whose run covers every unit (no Kafka, deterministic)."""
    cfg = load_config(config_path)
    start = datetime(2026, 1, 1, tzinfo=UTC)      # event ids do not depend on the start
    return corrupt.find_covering_seed(
        lambda s: generate_events(dataclasses.replace(cfg, seed=s), start),
        base_seed, cfg.bad_fraction, cfg.enabled_bad_kinds(), max_seeds=max_seeds)


# --- 5. the evidence table (pure, unit-tested) --------------------------------------------

def evidence_table(dl: Mapping[str, Any], bad: BadData) -> list[dict[str, Any]]:
    """One row per gate (hard codes, then soft flags): k and pieces 2-5.

    Each piece is the number the source gives for this gate; "fired" needs all == k > 0.
    Piece 2 is "exactly those k": `tables` = the expected rows found there with this
    reason / flag, `extra` = the run's other rows carrying it (must be 0).
    Piece 5 is k minus the rows found on the other side (S ∪ L for a hard gate, the DLQ
    for a soft one), so it too equals k when none leaked.
    """
    r10c, r10d = dl.get("r10c") or {}, dl.get("r10d") or {}
    r11, ev = dl.get("r11") or {}, dl.get("bad_evidence") or {}
    rows = []
    for code in corrupt.HARD_CODES:
        k = int(bad.expected_rejections.get(code, 0))
        rows.append({"gate": code, "kind": "hard", "manifest": k,
                     "tables": int((r10c.get("found_by_reason") or {}).get(code, 0)),
                     "extra": int((r10c.get("extra_by_reason") or {}).get(code, 0)),
                     "control": (ev.get("control_reject_counts") or {}).get(code, 0),
                     "recompute": (r11.get("by_reason") or {}).get(code, 0)
                     if r11.get("checkable") else None,
                     "not_elsewhere": k - int((ev.get("dlq_expected_in_accepted") or {})
                                              .get(code, 0))})
    for flag in corrupt.SOFT_CODES:
        k = int(bad.expected_flags.get(flag, 0))
        rows.append({"gate": flag, "kind": "soft", "manifest": k,
                     "tables": int((r10d.get("found_by_flag") or {}).get(flag, 0)),
                     "extra": int((r10d.get("others_by_flag") or {}).get(flag, 0)),
                     "control": (ev.get("control_flag_counts") or {}).get(flag, 0),
                     "recompute": (r11.get("by_flag") or {}).get(flag, 0)
                     if r11.get("checkable") else None,
                     "not_elsewhere": k - int((ev.get("flag_expected_in_dlq") or {})
                                              .get(flag, 0))})
    return rows


def coverage_problems(bad: BadData) -> list[str]:
    """Units the live manifest does not cover (kinds, wrong_type variants, H7 key kinds)."""
    out = []
    events = list((bad.bad_events or {}).values())
    for kind in bad.enabled_kinds:
        if not bad.bad_counts.get(kind):
            out.append(f"kind {kind}: 0 events")
    if "wrong_type" in bad.enabled_kinds:
        for variant in ("envelope", "field"):
            if not any(e.kind == "wrong_type" and e.variant == variant for e in events):
                out.append(f"wrong_type variant {variant}: 0 events")
    for key_kind in ("id", "offset"):
        if not any(e.expected_code == "missing_required_field"
                   and e.expected_key_kind == key_kind for e in events):
            out.append(f"missing_required_field with key kind {key_kind}: 0 events")
    return out


def judge(report: Mapping[str, Any], bad: BadData | None
          ) -> tuple[int, list[str], list[dict[str, Any]]]:
    """(exit code, problems, evidence table) from verify's report and the manifest."""
    if bad is None or bad.bad_events is None:
        return EXIT_SETUP, ["the manifest has no bad_data list (not a smoke-bad run)"], []
    sv = report.get("silver") or {}
    dl = sv.get("dlq") or {}
    verdicts = dl.get("verdicts") or {}
    problems: list[str] = []
    if report.get("result") != "PASS":
        problems.append(f"verify {report.get('result')}: {report.get('notes')}")
    for key in ("missing_ids", "duplicate_rows"):
        if report.get(key):
            problems.append(f"bronze {key} {report.get(key)}")
        if sv.get(key):
            problems.append(f"silver {key} {sv.get(key)}")
    failed = [f"{r} {verdicts.get(r)}" for r in RULES if verdicts.get(r) == "FAIL"]
    if problems or failed:
        return EXIT_VERIFY, problems + failed, []
    not_checkable = [f"{r} {verdicts.get(r)}" for r in RULES if verdicts.get(r) != "PASS"]
    if not (sv.get("consistency") or {}).get("checkable"):
        not_checkable.append(f"consistency not checkable: "
                             f"{(sv.get('consistency') or {}).get('reason')}")
    ev = dl.get("bad_evidence")
    if not ev:
        not_checkable.append("verify gave no bad_evidence section")
    elif ev.get("batches_shared"):
        not_checkable.append(
            f"piece 3: the run's batches hold {ev['control_rows_in']} rows, "
            f"{ev['run_rows_in_batches']} of them the run's (another producer, Q2)")
    if not_checkable:
        return EXIT_NOT_CHECKABLE, not_checkable, []
    table = evidence_table(dl, bad)
    for row in table:
        k = row["manifest"]
        if k <= 0:
            problems.append(f"{row['gate']}: not exercised (the manifest expects 0)")
            continue
        off = [f"{p} {row[p]}" for p in PIECES[1:] if row[p] != k]
        if row["extra"]:
            off.append(f"{row['extra']} other rows of the run carry it")
        if off:
            problems.append(f"{row['gate']}: k {k}, but " + ", ".join(off))
    problems += coverage_problems(bad)
    return (EXIT_NOT_FIRED if problems else EXIT_PASS), problems, table


def table_lines(table: list[dict[str, Any]]) -> list[str]:
    out = ["      gate                         k  tables extra control recompute not_else"
           "  fired"]
    for r in table:
        fired = (r["manifest"] > 0 and not r["extra"]
                 and all(r[p] == r["manifest"] for p in PIECES[1:]))
        out.append(f"      {r['gate']:<26} {r['manifest']:>4} {r['tables']!s:>7} "
                   f"{r['extra']!s:>5} "
                   f"{r['control']!s:>7} {r['recompute']!s:>9} {r['not_elsewhere']!s:>8}  "
                   f"{'yes' if fired else 'NO'}")
    return out


# --- the run -----------------------------------------------------------------------------

def run_bad(run: Run, args: argparse.Namespace) -> int:
    t0 = time.monotonic()
    say(f"[1/8] base seed {run.summary['base_seed']} (fresh), config {CONFIG} (backfill)")
    say(f"[2/8] dry-run seed search (no Kafka, at most {MAX_SEEDS} seeds)")
    found = search_seed(run.summary["base_seed"])
    run.summary["seed_search"] = {"seed": found.seed, "tried": found.tried,
                                  "missing": found.missing}
    if found.seed is None:
        raise Abort(EXIT_SETUP, f"no seed in {found.tried} covers every unit; uncovered: "
                                f"{found.missing}")
    run.seed = found.seed
    run.summary["seed"] = found.seed
    say(f"      seed {found.seed} covers every unit ({found.tried} seeds tried)")
    ensure_services(start_spark=not args.no_start_spark)
    manifest = send_and_wait(run, args, args.timeout or MODES["bad"]["timeout"], verbose=True)
    bad = manifest.bad_data
    if bad is None or bad.bad_events is None:
        raise Abort(EXIT_SETUP, f"{manifest.path}: no bad_data list (manifest "
                                f"v{manifest.version})")
    run.summary["bad"] = {"events": len(bad.bad_events), "dlq": bad.expected_dlq_count,
                          "flags": sum(bad.expected_flags.values()),
                          "by_key_kind": bad.expected_by_key_kind,
                          "new_region_match": bad.new_region_match}
    say(f"      {len(bad.bad_events)} bad events: {bad.expected_dlq_count} to the DLQ "
        f"({bad.expected_by_key_kind}), {sum(bad.expected_flags.values())} flagged")
    say("[7/8] verify --layer all (spark-tools, read-only)")
    code, report = verify_report(manifest, fail_on_duplicates=True)
    run.summary["verify"] = summarize_verify(report)
    dl = (report.get("silver") or {}).get("dlq") or {}
    run.summary["dlq_checks"] = {k: dl.get(k) for k in ("r10c", "r10d", "r11", "r14",
                                                       "bad_evidence")}
    say("[8/8] the evidence table (5 pieces per gate)")
    jcode, problems, table = judge(report, bad)
    if code not in (0, 1) and jcode == EXIT_PASS:
        jcode, problems = EXIT_SETUP, [f"verify exited {code}"]
    run.summary["evidence"] = table
    for line in table_lines(table):
        say(line)
    run.summary["total_s"] = round(time.monotonic() - t0, 1)
    if jcode != EXIT_PASS:
        run.summary["not_checkable"] = jcode == EXIT_NOT_CHECKABLE
        say(json.dumps(report, indent=2))
        word = {EXIT_NOT_CHECKABLE: "NOT CHECKABLE", EXIT_NOT_FIRED: "NOT FIRED"}.get(
            jcode, "failed")
        raise Abort(jcode, f"smoke-bad {word}:\n  " + "\n  ".join(problems),
                    short="; ".join(problems))
    say(f"PASS: every gate fired ({len(table)} gates, 5 pieces each); "
        f"{run.summary['total_s']}s total")
    say("      " + union_line(report.get("silver") or {}))
    return EXIT_PASS


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="VALKYRIE smoke-bad (every quality gate fires)")
    add_common_args(p)
    args = p.parse_args(argv)
    base = args.seed if args.seed is not None else time.time_ns() // 1_000_000
    run = Run(mode="bad", seed=base, summary={"mode": "smoke-bad", "base_seed": base})
    code = EXIT_PASS
    try:
        code = run_bad(run, args)
    except Abort as exc:
        code = exc.code
        word = ("NOT CHECKABLE" if run.summary.get("not_checkable") else
                "NOT FIRED" if code == EXIT_NOT_FIRED else "FAIL")
        say(f"\n{word} (exit {code}): {exc}")
        run.summary["error"] = exc.short
    except CommandError as exc:
        code = EXIT_SETUP
        say(f"\nFAIL (exit {code}): {exc}")
        run.summary["error"] = str(exc)
    except KeyboardInterrupt:
        code = 130
        run.summary["error"] = "interrupted"
    run.summary.update(result={0: "PASS", EXIT_NOT_FIRED: "NOT FIRED"}.get(
        code, "NOT CHECKABLE" if run.summary.get("not_checkable") else "FAIL"),
        exit_code=code)
    line = json.dumps(run.summary, separators=(",", ":"), default=str)
    if args.json_summary:
        args.json_summary.parent.mkdir(parents=True, exist_ok=True)
        args.json_summary.write_text(line + "\n", encoding="utf-8")
    say(f"SUMMARY {line}")
    return code


if __name__ == "__main__":
    sys.exit(main())
