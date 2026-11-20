"""The smoke-late D7 judge (e2e.smoke_late.judge): one passing case, one per assertion."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from e2e.smoke_late import EXIT_NOT_CHECKABLE, judge

PASS = dict.fromkeys(("R4", "R5", "R6a", "R6b", "R6c", "R6d"), "PASS")
REPORT: dict[str, Any] = {
    "result": "PASS", "notes": [],
    "silver": {
        "expected_unique_ids": 10, "silver_found_ids": 9, "late_found_ids": 1,
        "oracle": {
            "verdicts": PASS, "late_counts": {"slight": 2, "very": 2},
            "w_check": {"checkable": True}, "isolation": {"checkable": True},
            "bands": {"ontime": {"expected": 6, "silver": 6, "late": 0},
                      "slight": {"expected": 2, "silver": 2, "late": 0},
                      "very": {"expected": 2, "silver": 1, "late": 1}},
            "timing": {"g_ms": {"rows": 10, "median": 12000, "max": 20000},
                       "slight_margin_ms": 25000, "very_events": [],
                       "run_batches": [{"query_id": "q", "batch_id": 5}],
                       "control_before_run": None},
        },
    },
}


def report(**changes: Any) -> dict[str, Any]:
    """REPORT with dotted-path changes, e.g. report(**{"silver.late_found_ids": 0})."""
    r = copy.deepcopy(REPORT)
    for path, value in changes.items():
        *parents, leaf = path.split(".")
        node = r
        for k in parents:
            node = node[k]
        node[leaf] = value
    return r


def test_passing_report() -> None:
    assert judge(REPORT, ("q", 4)) == (0, [], [])
    assert judge(REPORT, None) == (0, [], [])


def test_verify_fail_is_exit_1_before_anything_else() -> None:
    code, problems, _ = judge(report(result="FAIL", notes=["silver: 1 R1: ..."]), None)
    assert code == 1 and problems == ["verify FAIL: ['silver: 1 R1: ...']"]


def test_no_oracle_section_is_exit_1() -> None:
    assert judge(report(**{"silver.oracle": None}), None)[0] == 1


@pytest.mark.parametrize("reason", [
    "not checkable: W check: W differs: manifest 30 s, control 60 s",
    "not checkable: isolation: not isolated: a foreign bronze row ... (margin -5 ms)"])
def test_a_gate_not_checkable_is_exit_2_never_pass(reason: str) -> None:
    v = {**PASS, "R4": reason, "R5": reason}
    code, problems, _ = judge(report(**{"silver.oracle.verdicts": v}), None)
    assert (code, problems) == (EXIT_NOT_CHECKABLE, [f"R4/R5 {reason}"])


@pytest.mark.parametrize(("changes", "text"), [
    ({"silver.silver_found_ids": 8}, "|S| 8 != delivered 10 - |L| 1"),
    ({"silver.oracle.bands.slight.silver": 1}, "late events found 3 != late_counts 2 + 2"),
    ({"silver.oracle.bands.ontime.late": 1}, "1 on-time events in late"),
    ({"silver.oracle.bands.ontime.silver": 5}, "on-time events in silver 5 != 10 - 2 - 2"),
    ({"silver.oracle.bands.slight.late": 1}, "1 slight events in late"),
    ({"silver.oracle.bands.slight.silver": 1, "silver.oracle.bands.slight.late": 1},
     "slight in silver 1 != 2"),
    ({"silver.oracle.bands.very.late": 3}, "very in late 3 > 2"),
    ({"silver.oracle.bands.very.silver": 0}, "very in silver 0 + late 1 != 2"),
    ({"silver.late_found_ids": 2, "silver.silver_found_ids": 8},
     "|L| 2 != very events in late 1 (R4)"),
    ({"silver.oracle.verdicts.R5": "FAIL"}, "R5 FAIL"),
], ids=["S", "found", "ontime-late", "ontime-silver", "slight-late", "slight-silver",
        "very-late", "very-sum", "L-not-very", "R5"])
def test_each_assertion_fails_alone(changes: dict[str, Any], text: str) -> None:
    code, problems, _ = judge(report(**changes), None)
    assert code == 1 and any(text in p for p in problems), problems


def test_run_rows_from_a_batch_at_or_before_the_baseline_fail() -> None:
    code, problems, _ = judge(REPORT, ("q", 5))
    assert code == 1 and "batches [5] <= baseline batch 5" in problems[0]
    assert judge(REPORT, ("other-query", 9))[0] == 0     # a new query id starts afresh


def test_warnings_never_fail() -> None:
    r = report(**{"silver.late_found_ids": 0, "silver.silver_found_ids": 10,
                  "silver.oracle.bands.very.late": 0, "silver.oracle.bands.very.silver": 2,
                  "silver.oracle.timing.g_ms.max": 31000,
                  "silver.oracle.timing.slight_margin_ms": 14000})
    code, problems, warnings = judge(r, None)
    assert (code, problems) == (0, [])
    assert [w.split(" ")[0:2] for w in warnings] == [["no", "very"], ["max", "G"],
                                                     ["slight", "margin"]]
