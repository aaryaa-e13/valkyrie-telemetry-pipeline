"""Unit tests for streaming.watermark (pure rules, no Spark)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from streaming.watermark import (
    SnapshotStatusError,
    SplitError,
    batch_starts_in_snapshot,
    check_split,
    recorded_delay_problem,
    us_to_datetime,
    watermark_us,
)

SNAP = ('{"sourceVersion":1,"reservoirId":"r","reservoirVersion":1,"index":1,'
        '"isStartingVersion":true}')
TAIL = ('{"sourceVersion":1,"reservoirId":"r","reservoirVersion":2,"index":-1,'
        '"isStartingVersion":false}')
META = '{"batchWatermarkMs":0,"batchTimestampMs":1,"conf":{}}'


def _offsets(cp: Path, batch: int, text: str) -> None:
    d = cp / "offsets"
    d.mkdir(parents=True, exist_ok=True)
    (d / str(batch)).write_text(text, encoding="utf-8")


def test_batch_zero_always_starts_in_the_snapshot(tmp_path: Path) -> None:
    assert batch_starts_in_snapshot(tmp_path, 0) is True   # no file needed


def test_start_of_batch_n_is_the_end_of_batch_n_minus_1(tmp_path: Path) -> None:
    """A1: offsets/1 ends with isStartingVersion false (real Delta 4.4.1 shape), but
    batch 1 STARTS at offsets/0 (inside the snapshot), so it is a snapshot batch."""
    _offsets(tmp_path, 0, f"v1\n{META}\n{SNAP}")
    _offsets(tmp_path, 1, f"v1\n{META}\n{TAIL}")
    assert batch_starts_in_snapshot(tmp_path, 1) is True
    assert batch_starts_in_snapshot(tmp_path, 2) is False


@pytest.mark.parametrize("text, match", [
    (None, "cannot read"),                                         # file missing
    ("", "exactly one source line"),                                # empty file
    (f"v1\n{META}", "exactly one source line"),                     # no source line
    (f"v1\n{META}\n{SNAP}\n{SNAP}", "exactly one source line"),     # two sources
    (f"v2\n{META}\n{SNAP}", "exactly one source line"),             # unknown format
    (f"v1\n{META}\n{{not json", "not JSON"),
    (f"v1\n{META}\n[1, 2]", "not a JSON object"),
    (f'v1\n{META}\n{{"reservoirVersion": 3, "index": -1}}', "isStartingVersion is missing"),
    (f'v1\n{META}\n{{"isStartingVersion": "false"}}', "isStartingVersion is 'false'"),
    (f'v1\n{META}\n{{"isStartingVersion": null}}', "isStartingVersion is None"),
])
def test_bad_offset_fails_loudly_never_defaults(tmp_path: Path, text: str | None,
                                                match: str) -> None:
    """A2: anything but a readable boolean isStartingVersion raises."""
    if text is not None:
        _offsets(tmp_path, 4, text)
    with pytest.raises(SnapshotStatusError, match=match) as exc:
        batch_starts_in_snapshot(tmp_path, 5)
    assert str(exc.value).startswith("SnapshotStatusError: ")   # pipeline maps it to exit 2


@pytest.mark.parametrize("bad", [-1, True, 1.0, "1"])
def test_bad_batch_id_is_refused(tmp_path: Path, bad: object) -> None:
    with pytest.raises(SnapshotStatusError):
        batch_starts_in_snapshot(tmp_path, bad)  # type: ignore[arg-type]


def test_watermark_is_minus_infinity_in_snapshot_or_without_a_prior_max() -> None:
    assert watermark_us(None, 60, in_snapshot=False) is None
    assert watermark_us(5_000_000_000, 60, in_snapshot=True) is None
    assert watermark_us(5_000_000_000, 60, in_snapshot=False) == 5_000_000_000 - 60_000_000


@pytest.mark.parametrize("bad", [0, -5, True, 60.0])
def test_watermark_delay_must_be_a_positive_int(bad: object) -> None:
    with pytest.raises(ValueError):
        watermark_us(1, bad, in_snapshot=False)  # type: ignore[arg-type]


def test_microseconds_round_trip_exactly() -> None:
    t = datetime(2026, 11, 19, 12, 0, 0, 123456, tzinfo=UTC)
    us = (t - datetime(1970, 1, 1, tzinfo=UTC)) // __import__("datetime").timedelta(
        microseconds=1)
    assert us_to_datetime(us) == t and us_to_datetime(None) is None


def test_split_check() -> None:
    check_split(keyed_unique=5, deduped=5, late=2, ontime=3)
    check_split(keyed_unique=0, deduped=0, late=0, ontime=0)
    with pytest.raises(SplitError, match="neither table"):
        check_split(keyed_unique=5, deduped=5, late=2, ontime=2)
    with pytest.raises(SplitError, match="dedupe kept"):
        check_split(keyed_unique=5, deduped=6, late=2, ontime=3)
    with pytest.raises(SplitError, match="negative"):
        check_split(keyed_unique=5, deduped=5, late=-1, ontime=6)


def test_recorded_delay_problem() -> None:
    assert recorded_delay_problem(None, 60, "q") is None       # nothing recorded yet
    assert recorded_delay_problem(60, 60, "q") is None
    msg = recorded_delay_problem(60, 61, "q-7")
    assert msg is not None and "WATERMARK_DELAY_S=61" in msg and "watermark_delay_s=60" in msg
    assert "q-7" in msg and "reset the silver checkpoint" in msg
