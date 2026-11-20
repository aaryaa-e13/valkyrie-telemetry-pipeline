"""Unit tests for streaming.silver_merge (the D9 NULL-bound rule, no Spark)."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from streaming.silver_merge import MergeBoundsError, merge_bounds, merge_condition

D1, D2 = date(2026, 11, 14), date(2026, 11, 19)


def test_bounds_add_horizon_on_both_sides_across_month_end() -> None:
    assert merge_bounds(date(2026, 11, 28), date(2026, 11, 29), 7) == (
        date(2026, 11, 21), date(2026, 12, 6))
    assert merge_bounds(D1, D1, 0) == (D1, D1)


@pytest.mark.parametrize(("lo", "hi"), [(None, D2), (D1, None), (None, None)])
def test_none_bound_is_refused(lo: object, hi: object) -> None:
    with pytest.raises(MergeBoundsError, match="NULL bound"):
        merge_bounds(lo, hi, 7)
    with pytest.raises(MergeBoundsError, match="NULL bound"):
        merge_condition(lo, hi)


@pytest.mark.parametrize("bad", [datetime(2026, 11, 14, 12), "2026-11-14", 20261114])
def test_non_date_is_refused(bad: object) -> None:
    with pytest.raises(MergeBoundsError, match=r"must be a datetime\.date"):
        merge_bounds(bad, D2, 7)
    with pytest.raises(MergeBoundsError, match=r"must be a datetime\.date"):
        merge_condition(D1, bad)


def test_min_after_max_and_bad_horizon_are_refused() -> None:
    with pytest.raises(MergeBoundsError, match="after"):
        merge_bounds(D2, D1, 7)
    with pytest.raises(MergeBoundsError, match="after"):
        merge_condition(D2, D1)
    for h in (-1, 1.5, True, None):
        with pytest.raises(MergeBoundsError, match="horizon_days"):
            merge_bounds(D1, D2, h)  # type: ignore[arg-type]


def test_condition_is_exact_and_literal() -> None:
    assert merge_condition(*merge_bounds(D1, D2, 7)) == (
        "t.event_id = s.event_id AND t.event_date BETWEEN DATE'2026-11-07' "
        "AND DATE'2026-11-26'")
