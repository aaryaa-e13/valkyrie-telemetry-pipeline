"""e2e/smoke_bad.py: the evidence table judge (exit 0 / 1 / 2 / 6) and the seed search."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from common.manifest_reader import BadData, parse_manifest
from e2e import smoke_bad as sb
from simulator import corrupt
from tests.fixtures import build_bad_run

FIXTURE = json.loads(build_bad_run.PATH.read_text(encoding="utf-8"))


def _bad() -> BadData:
    m = parse_manifest(copy.deepcopy(FIXTURE["manifest"]))
    assert m.bad_data is not None
    return m.bad_data


def report(bad: BadData) -> dict[str, Any]:
    """A verify report for a run where every gate fired exactly as the manifest says."""
    rej, flags = dict(bad.expected_rejections), dict(bad.expected_flags)
    dl = {
        "verdicts": dict.fromkeys(sb.RULES, "PASS"),
        "r10c": {"checkable": True, "found_by_reason": dict(rej), "extra_by_reason": {}},
        "r10d": {"found_by_flag": dict(flags), "others_by_flag": dict.fromkeys(flags, 0)},
        "r11": {"checkable": True, "by_reason": dict(rej), "by_flag": dict(flags)},
        "bad_evidence": {"batches_shared": False, "control_rows_in": 239,
                         "run_rows_in_batches": 239, "control_reject_counts": dict(rej),
                         "control_flag_counts": dict(flags), "dlq_expected_in_accepted": {},
                         "flag_expected_in_dlq": {}},
    }
    return {"result": "PASS", "missing_ids": 0, "duplicate_rows": 0,
            "silver": {"result": "PASS", "missing_ids": 0, "duplicate_rows": 0,
                       "consistency": {"checkable": True}, "dlq": dl}}


def test_every_gate_fired_is_exit_0_with_a_full_table() -> None:
    bad = _bad()
    code, problems, table = sb.judge(report(bad), bad)
    assert (code, problems) == (0, [])
    assert [r["gate"] for r in table] == [*corrupt.HARD_CODES, *corrupt.SOFT_CODES]
    for r in table:
        assert r["manifest"] > 0 and r["extra"] == 0
        assert all(r[p] == r["manifest"] for p in sb.PIECES[1:]), r
    assert any("fired" in line for line in sb.table_lines(table))
    assert all(line.endswith("yes") for line in sb.table_lines(table)[1:])


@pytest.mark.parametrize("tamper,piece", [
    (lambda d: d["r10c"]["found_by_reason"].update(bad_enum=0), "tables 0"),
    (lambda d: d["r10c"]["extra_by_reason"].update(bad_enum=1), "1 other rows"),
    (lambda d: d["r10d"]["others_by_flag"].update(future_event_time=2), "2 other rows"),
    (lambda d: d["bad_evidence"]["control_reject_counts"].update(not_json=99), "control 99"),
    (lambda d: d["bad_evidence"]["control_flag_counts"].pop("self_kill"), "control 0"),
    (lambda d: d["r11"]["by_flag"].update(player_id_sentinel=0), "recompute 0"),
    (lambda d: d["bad_evidence"]["dlq_expected_in_accepted"].update(out_of_range=1),
     "not_elsewhere"),
    (lambda d: d["bad_evidence"]["flag_expected_in_dlq"].update(unexpected_field=1),
     "not_elsewhere"),
], ids=["tables", "extra-dlq", "extra-flag", "control", "control-flag", "recompute",
        "leak-to-accepted", "leak-to-dlq"])
def test_one_piece_off_is_not_fired_exit_6(tamper: Any, piece: str) -> None:
    bad = _bad()
    rep = report(bad)
    tamper(rep["silver"]["dlq"])
    code, problems, _ = sb.judge(rep, bad)
    assert code == sb.EXIT_NOT_FIRED and len(problems) == 1 and piece in problems[0], problems


def test_a_gate_the_manifest_does_not_exercise_is_exit_6() -> None:
    bad = _bad()
    rep = report(bad)
    bad.expected_rejections["bad_patch_format"] = 0
    rep["silver"]["dlq"]["r10c"]["found_by_reason"]["bad_patch_format"] = 0
    code, problems, _ = sb.judge(rep, bad)
    assert code == 6 and problems == ["bad_patch_format: not exercised (the manifest expects 0)"]


def test_coverage_needs_both_wrong_type_variants_and_both_h7_key_kinds() -> None:
    bad = _bad()
    assert sb.coverage_problems(bad) == []
    assert bad.bad_events is not None
    for e in [e for e in bad.bad_events.values() if e.kind == "wrong_type"
              and e.variant == "field"]:
        del bad.bad_events[e.event_id]
    assert sb.coverage_problems(bad) == ["wrong_type variant field: 0 events"]


@pytest.mark.parametrize("tamper,code", [
    (lambda r: r.update(result="FAIL", notes=["R1: x"]), 1),
    (lambda r: r.update(missing_ids=1), 1),
    (lambda r: r["silver"].update(duplicate_rows=2), 1),
    (lambda r: r["silver"]["dlq"]["verdicts"].update(R14="FAIL"), 1),
    (lambda r: r["silver"]["dlq"]["verdicts"].update(R11="not checkable: 2 ids have more"), 2),
    (lambda r: r["silver"]["consistency"].update(checkable=False, reason="snapshot"), 2),
    (lambda r: r["silver"]["dlq"]["bad_evidence"].update(batches_shared=True,
                                                         control_rows_in=300), 2),
    (lambda r: r["silver"]["dlq"].pop("bad_evidence"), 2),
], ids=["verify-fail", "bronze-missing", "silver-dupes", "rule-fail", "r11-not-checkable",
        "consistency", "shared-batches-q2", "no-evidence"])
def test_fail_and_not_checkable_come_before_the_table(tamper: Any, code: int) -> None:
    bad = _bad()
    rep = report(bad)
    tamper(rep)
    got, problems, table = sb.judge(rep, bad)
    assert (got, table) == (code, []) and problems


def test_a_manifest_without_the_bad_list_is_setup() -> None:
    bad = _bad()
    assert sb.judge(report(bad), None)[0] == 2
    import dataclasses
    assert sb.judge(report(bad), dataclasses.replace(bad, bad_events=None))[0] == 2


def test_the_seed_search_is_the_fixtures_and_gives_up_naming_units() -> None:
    found = sb.search_seed(42)
    assert (found.seed, found.tried) == (FIXTURE["seed"], FIXTURE["tried"])
    none = sb.search_seed(42, max_seeds=2)
    assert none.seed is None and none.tried == 2 and none.missing
