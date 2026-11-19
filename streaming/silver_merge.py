"""Bounds and ON-clause of silver's insert-only MERGE (standard library only).

The rule this module enforces (DECISIONS D2.3, parent plan D2/D9): the MERGE's
partition-pruning bounds are DATE *literals* computed from keyed rows only, and
can never be NULL.

Why it matters (VERIFIED in the slice 3 planning spike): with
`ON t.event_id = s.event_id AND t.event_date BETWEEN NULL AND NULL` the whole
ON clause is NULL, nothing matches, and every source row is inserted again,
including ids already in silver. That is silent double counting, which is far
worse than a MERGE without pruning. So a missing bound is an error, never a
NULL in the SQL.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta


class MergeBoundsError(ValueError):
    """The MERGE bounds cannot be built safely; the batch must fail loudly."""


def _check_date(name: str, value: object) -> date:
    # datetime is a subclass of date: reject it explicitly, its time part would
    # silently be dropped by the DATE literal.
    if value is None:
        raise MergeBoundsError(f"{name} is None: refusing to build a MERGE with a NULL bound")
    if isinstance(value, datetime) or not isinstance(value, date):
        raise MergeBoundsError(f"{name} must be a datetime.date, got {type(value).__name__}")
    return value


def merge_bounds(min_date: object, max_date: object, horizon_days: int) -> tuple[date, date]:
    """(lo, hi) = (min_date - H, max_date + H) for the target `event_date` filter.

    `+ H` on the upper side too: a re-stamped duplicate can be OLDER than the
    copy already in silver (parent plan D2).

    Raises:
        MergeBoundsError: a bound is None or not a date, min > max, or H < 0.
    """
    lo = _check_date("min_date", min_date)
    hi = _check_date("max_date", max_date)
    if isinstance(horizon_days, bool) or not isinstance(horizon_days, int) or horizon_days < 0:
        raise MergeBoundsError(f"horizon_days must be an int >= 0, got {horizon_days!r}")
    if lo > hi:
        raise MergeBoundsError(f"min_date {lo} is after max_date {hi}")
    return lo - timedelta(days=horizon_days), hi + timedelta(days=horizon_days)


def merge_condition(lo: object, hi: object) -> str:
    """ON clause for silver's MERGE, with DATE literals rendered from date objects only."""
    lo_d, hi_d = _check_date("lo", lo), _check_date("hi", hi)
    if lo_d > hi_d:
        raise MergeBoundsError(f"lo {lo_d} is after hi {hi_d}")
    return (f"t.event_id = s.event_id AND t.event_date BETWEEN DATE'{lo_d.isoformat()}' "
            f"AND DATE'{hi_d.isoformat()}'")
