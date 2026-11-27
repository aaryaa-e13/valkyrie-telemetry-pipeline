"""Unit tests for streaming/oracle_dlq.py (R8 equations, R9 and R10 verdicts). No Spark."""

from __future__ import annotations

from typing import Any

import pytest

from streaming import oracle_dlq as od


def _row(**kw: Any) -> dict[str, Any]:
    """A consistent Phase 3 control row: 10 rows in = 2 keyless + 8 keyed
    (1 in-batch dup, 7 unique = 2 rejected + 2 late + 3 on time)."""
    base: dict[str, Any] = dict(
        silver_query_id="q", batch_id=3, rules_version=1, rows_in=10,
        keyless_rows=2, keyed_unique=7, dup_in_batch=1,
        rejected_rows=2, late_rows=2, ontime_rows=3,
        dlq_keyless_inserted_this_attempt=1, dlq_keyless_already_present=1,
        dlq_keyed_inserted_this_attempt=1, dlq_keyed_already_present=0,
        rejected_dup_of_accepted=1,
        late_inserted_this_attempt=0, late_already_present=0, late_dup_of_silver=1,
        late_dup_of_dlq=1,
        inserted_this_attempt=1, ontime_already_present=1, dup_of_late=0, dup_of_dlq=1,
        reject_counts={"not_json": 2, "bad_enum": 2})
    base.update(kw)
    return base


def test_a_consistent_row_has_no_problem() -> None:
    assert od.r8_row_problems(_row()) == []


@pytest.mark.parametrize(("change", "left"), [
    ({"rows_in": 11}, "rows_in"),
    ({"rejected_rows": 3, "rejected_dup_of_accepted": 2,
      "reject_counts": {"not_json": 2, "bad_enum": 3}}, "keyed_rows"),
    ({"dlq_keyless_already_present": 0}, "keyless_rows"),
    ({"rejected_dup_of_accepted": 0}, "rejected_rows"),
    ({"late_dup_of_dlq": 0}, "late_rows"),
    ({"dup_of_dlq": 0}, "ontime_rows"),
])
def test_each_equation_catches_its_own_tamper(change: dict[str, Any], left: str) -> None:
    problems = od.r8_row_problems(_row(**change))
    assert [p.split(" ")[0] for p in problems] == [left], problems


def test_reject_counts_must_sum_to_keyless_plus_rejected() -> None:
    problems = od.r8_row_problems(_row(reject_counts={"not_json": 2, "bad_enum": 1}))
    assert problems == ["sum(reject_counts) 3 != keyless_rows + rejected_rows = 4"]


def test_null_fields_are_a_problem_not_a_pass() -> None:
    assert od.r8_row_problems(_row(dup_of_dlq=None)) == ["NULL field(s) ['dup_of_dlq']"]


def test_r8_skips_legacy_rows_and_counts_violations() -> None:
    rep = od.r8([_row(), _row(rules_version=None, rows_in=99), _row(batch_id=4, rows_in=9)])
    assert (rep["batches_checked"], rep["legacy_batches_skipped"], rep["violations"]) == (2, 1, 1)
    assert rep["examples"][0]["batch_id"] == 4


def _r9(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = {k: 0 for k, _ in od.R9_COUNTS}
    base.update(checkable=True)
    base.update(kw)
    return base


def test_r9_pass_and_each_count_fails() -> None:
    assert od.r9_verdict(_r9()) == ("PASS", [])
    for key, _ in od.R9_COUNTS:
        verdict, notes = od.r9_verdict(_r9(**{key: 1}))
        assert verdict == "FAIL" and len(notes) == 1 and notes[0].startswith("R9: 1 "), notes


def test_r9_not_checkable_is_never_a_pass() -> None:
    assert od.r9_verdict({"checkable": False, "reason": "no cut"}) == (
        "not checkable: no cut", ["R9 not checkable: no cut"])


# --- R10: good data is never dead-lettered ----------------------------------------------

R10_CLEAN: dict[str, Any] = {
    "ids_listed": 7, "by_id_rows": 0, "by_id_examples": [], "offsets_checkable": True,
    "own_rows": 0, "own_keyless": 0, "own_examples": [], "shared_rows": 0,
    "shared_keyless": 0}


def test_own_range_means_no_gap_for_other_producers() -> None:
    assert od.own_range(0, 6, 7) and not od.own_range(0, 6, 6)


def test_r10_clean_passes_both_parts() -> None:
    assert od.r10_verdicts(R10_CLEAN) == ({"R10a": "PASS", "R10b": "PASS"}, [])


def test_r10a_a_run_id_in_the_dlq_fails() -> None:
    v, notes = od.r10_verdicts({**R10_CLEAN, "by_id_rows": 1, "by_id_examples": ["id:b0"]})
    assert v == {"R10a": "FAIL", "R10b": "PASS"}
    assert notes[0].startswith("R10a: 1 DLQ rows carry this run's event ids")


def test_r10b_a_row_at_the_runs_own_offsets_fails() -> None:
    v, notes = od.r10_verdicts({**R10_CLEAN, "own_rows": 2, "own_keyless": 1})
    assert v == {"R10a": "PASS", "R10b": "FAIL"}
    assert notes[0].startswith("R10b: 2 DLQ rows (1 keyless) at offsets only this run")


def test_r10b_shared_offsets_are_counted_not_passed() -> None:
    v, notes = od.r10_verdicts({**R10_CLEAN, "shared_rows": 3, "shared_keyless": 3})
    assert v["R10b"].startswith("not checkable: 3 DLQ rows (3 keyless) in offset ranges")
    assert notes[0].startswith("R10b not checkable:")      # not a "R10b:" FAIL note


def test_r10b_own_rows_win_over_shared_ones() -> None:
    v, _ = od.r10_verdicts({**R10_CLEAN, "own_rows": 1, "shared_rows": 3})
    assert v["R10b"] == "FAIL"


def test_r10_without_ids_or_offsets_is_never_a_pass() -> None:
    v, notes = od.r10_verdicts({**R10_CLEAN, "ids_listed": 0, "offsets_checkable": False,
                                 "offsets_reason": "the manifest has no kafka_offsets (v1)"})
    assert v == {"R10a": "not checkable: the manifest lists no event ids",
                 "R10b": "not checkable: the manifest has no kafka_offsets (v1)"}
    assert len(notes) == 2 and all(" not checkable: " in n for n in notes)
