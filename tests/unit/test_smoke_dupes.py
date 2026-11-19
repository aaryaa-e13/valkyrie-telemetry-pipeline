"""Unit tests for e2e.smoke_dupes.dupes_problems (pure check of the verify report)."""

from __future__ import annotations

from e2e.smoke_dupes import dupes_problems

GOOD = {"missing_ids": 0, "duplicate_rows": 13, "max_copies_of_one_id": 2,
        "silver": {"result": "PASS", "found_unique_ids": 13, "duplicate_rows": 0,
                   "rows_from_this_send": 13}}


def test_good_report_passes() -> None:
    assert dupes_problems(GOOD, 13) == []


def test_each_failure_is_named() -> None:
    once = {**GOOD, "duplicate_rows": 0, "max_copies_of_one_id": 1}
    assert "exactly twice" in dupes_problems(once, 13)[0]
    second = {**GOOD, "silver": {**GOOD["silver"], "rows_from_this_send": 10}}
    assert "FIRST send" in dupes_problems(second, 13)[0]
    doubled = {**GOOD, "silver": {**GOOD["silver"], "result": "FAIL", "duplicate_rows": 2,
                                  "notes": ["R3"]}}
    assert len(dupes_problems(doubled, 13)) == 2
