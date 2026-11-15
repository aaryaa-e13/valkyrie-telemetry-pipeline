"""Unit tests for send planning (retime + deliberate lateness)."""

from __future__ import annotations

import dataclasses
import hashlib
import os
import subprocess
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from simulator.config import SimulatorConfig
from simulator.events import EventType, GameEvent
from simulator.generator import generate_events
from simulator.schedule import (
    SLIGHT_BAND,
    VERY_BAND,
    LatenessConfig,
    PlannedSend,
    backfill_start,
    build_schedule,
    lateness_uniforms,
    plan_delay,
    retime,
)

REF = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)
NOW = datetime(2026, 11, 15, 9, 30, 0, 123456, tzinfo=UTC)
LATE = LatenessConfig(late_fraction=0.3, too_late_share=0.2, assumed_watermark_s=30.0)

# plan_delay for the first five events of seed 42 with late_fraction=1.0 (captured 2026-11-15).
GOLDEN_DELAYS: list[tuple[int, str | None]] = [
    (9784, "slight"), (3185, "slight"), (7432, "slight"), (3315, "slight"), (7369, "slight"),
]


def _events(seed: int = 42, num_matches: int = 20) -> list[GameEvent]:
    return generate_events(SimulatorConfig(seed=seed, num_matches=num_matches), REF)


def _without_time(e: GameEvent) -> dict[str, object]:
    d = dataclasses.asdict(e)
    del d["event_time"]
    return d


def _content_by_id(events: list[GameEvent]) -> dict[str, dict[str, object]]:
    return {e.event_id: _without_time(e) for e in events}


# --- retime ------------------------------------------------------------------

def test_retime_with_same_start_and_scale_1_changes_nothing() -> None:
    events = _events()
    assert retime(events, ref_start=REF, new_start=REF) == events


def test_retime_shift_moves_every_event_time_by_the_same_amount() -> None:
    events = _events()
    shift = timedelta(days=-3, seconds=17)
    moved = retime(events, ref_start=REF, new_start=REF + shift)
    assert [e.event_id for e in moved] == [e.event_id for e in events]
    assert all(m.event_time - e.event_time == shift for m, e in zip(moved, events, strict=True))


def test_retime_changes_only_event_time() -> None:
    events = _events()
    moved = retime(events, ref_start=REF, new_start=NOW, time_scale=7.0)
    assert _content_by_id(moved) == _content_by_id(events)


def test_retime_scale_divides_offsets_and_rounds_to_whole_ms() -> None:
    events = _events()
    original = {e.event_id: e.event_time for e in events}
    for e in retime(events, ref_start=REF, new_start=NOW, time_scale=10.0):
        scaled = e.event_time - NOW
        exact_ms = (original[e.event_id] - REF) / timedelta(milliseconds=1) / 10.0
        assert scaled % timedelta(milliseconds=1) == timedelta(0)
        assert abs(scaled / timedelta(milliseconds=1) - exact_ms) <= 0.5


def test_retime_does_not_scale_duration_s() -> None:
    events = _events()
    durations = {e.event_id: e.duration_s for e in events if e.event_type is EventType.MATCH_END}
    moved = retime(events, ref_start=REF, new_start=NOW, time_scale=30.0)
    assert durations
    assert {e.event_id: e.duration_s for e in moved
            if e.event_type is EventType.MATCH_END} == durations


@pytest.mark.parametrize("scale", [1.0, 10.0, 1000.0])
def test_retime_keeps_global_sort_and_per_match_seq_order(scale: float) -> None:
    moved = retime(_events(), ref_start=REF, new_start=NOW, time_scale=scale)
    keys = [(e.event_time, e.match_id, e.match_seq) for e in moved]
    assert keys == sorted(keys)
    seqs: dict[str, list[int]] = defaultdict(list)
    for e in moved:
        seqs[e.match_id].append(e.match_seq)
    assert all(s == list(range(1, len(s) + 1)) for s in seqs.values())


def test_retime_rejects_bad_input() -> None:
    events = _events(num_matches=1)
    with pytest.raises(ValueError, match="time_scale"):
        retime(events, ref_start=REF, new_start=NOW, time_scale=0)
    with pytest.raises(ValueError, match="timezone-aware"):
        retime(events, ref_start=REF, new_start=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="before ref_start"):
        retime(events, ref_start=REF + timedelta(days=1), new_start=NOW)


# --- backfill_start ----------------------------------------------------------

@pytest.mark.parametrize("scale", [1.0, 3.0, 10.0, 77.7])
def test_backfill_puts_last_event_at_or_just_before_now(scale: float) -> None:
    events = _events()
    start = backfill_start(events, ref_start=REF, now=NOW, time_scale=scale)
    moved = retime(events, ref_start=REF, new_start=start, time_scale=scale)
    last = max(e.event_time for e in moved)
    assert last <= NOW                                   # nothing from the future
    assert NOW - last < timedelta(milliseconds=1)        # and no needless gap
    assert min(e.event_time for e in moved) >= start


def test_backfill_start_with_no_events_is_now_rounded_down_to_ms() -> None:
    start = backfill_start([], ref_start=REF, now=NOW)
    assert start == NOW.replace(microsecond=123000)


# --- lateness hash -----------------------------------------------------------

def test_lateness_uniforms_follow_the_documented_formula() -> None:
    event_id = "b94f52aa-6a40-5de2-a491-f9642c310b98"
    digest = hashlib.sha256(f"42:lateness:{event_id}".encode()).digest()
    expected = tuple(int.from_bytes(digest[i:i + 8], "big") / 2**64 for i in (0, 8, 16))
    assert lateness_uniforms(42, event_id) == expected
    assert all(0.0 <= u < 1.0 for u in expected)


def test_lateness_golden_values_are_stable() -> None:
    """Pinned outputs: fails if the formula (or its byte layout) ever changes."""
    ids = [e.event_id for e in _events(seed=42, num_matches=2)[:5]]
    assert [plan_delay(42, i, LatenessConfig(late_fraction=1.0)) for i in ids] == GOLDEN_DELAYS


def test_lateness_is_the_same_in_a_fresh_process_with_another_hash_seed() -> None:
    """Python's built-in hash() changes per process; our sha256-based plan must not."""
    code = (
        "from simulator.schedule import lateness_uniforms;"
        "print(repr(lateness_uniforms(42, 'b94f52aa-6a40-5de2-a491-f9642c310b98')))"
    )
    outputs = set()
    for hash_seed in ("1", "2"):
        res = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True,
            env={**os.environ, "PYTHONHASHSEED": hash_seed},
        )
        outputs.add(res.stdout.strip())
    assert outputs == {repr(lateness_uniforms(42, "b94f52aa-6a40-5de2-a491-f9642c310b98"))}


def test_late_fraction_zero_means_no_late_events_and_one_means_all() -> None:
    ids = [e.event_id for e in _events()]
    assert all(plan_delay(42, i, LatenessConfig(late_fraction=0.0)) == (0, None) for i in ids)
    assert all(plan_delay(42, i, LatenessConfig(late_fraction=1.0))[1] is not None for i in ids)


def test_delays_fall_only_inside_the_two_bands() -> None:
    w_ms = LATE.assumed_watermark_s * 1000
    seen = set()
    for e in _events():
        delay_ms, band = plan_delay(42, e.event_id, LATE)
        seen.add(band)
        if band is None:
            assert delay_ms == 0
        else:
            lo, hi = SLIGHT_BAND if band == "slight" else VERY_BAND
            assert lo * w_ms <= delay_ms <= hi * w_ms
    assert seen == {None, "slight", "very"}


def test_late_fraction_and_band_share_are_close_to_the_settings() -> None:
    plans = [plan_delay(42, e.event_id, LATE) for e in _events(num_matches=200)]
    late = [band for _, band in plans if band is not None]
    assert len(plans) > 10_000
    assert len(late) / len(plans) == pytest.approx(0.3, abs=0.02)
    assert late.count("very") / len(late) == pytest.approx(0.2, abs=0.03)


def test_delays_scale_with_the_assumed_watermark() -> None:
    ids = [e.event_id for e in _events(num_matches=2)]
    w30 = LatenessConfig(late_fraction=1.0, assumed_watermark_s=30.0)
    w60 = LatenessConfig(late_fraction=1.0, assumed_watermark_s=60.0)
    for i in ids:
        d30, b30 = plan_delay(42, i, w30)
        d60, b60 = plan_delay(42, i, w60)
        assert b30 == b60
        assert abs(d60 - 2 * d30) <= 1          # rounding to whole ms


def test_raising_late_fraction_only_adds_late_events() -> None:
    ids = [e.event_id for e in _events()]
    low = {i: plan_delay(42, i, LatenessConfig(late_fraction=0.1)) for i in ids}
    high = {i: plan_delay(42, i, LatenessConfig(late_fraction=0.4)) for i in ids}
    late_low = {i for i, (_, band) in low.items() if band}
    assert late_low
    assert all(high[i] == low[i] for i in late_low)


def test_an_events_lateness_does_not_depend_on_how_many_matches_run() -> None:
    small = {p.event.event_id: (p.delay_ms, p.band)
             for p in build_schedule(_events(num_matches=3), 42, LATE)}
    big = {p.event.event_id: (p.delay_ms, p.band)
           for p in build_schedule(_events(num_matches=20), 42, LATE)}
    assert small.items() <= big.items()


def test_different_seed_gives_a_different_lateness_plan() -> None:
    ids = [e.event_id for e in _events()]
    assert [plan_delay(42, i, LATE) for i in ids] != [plan_delay(43, i, LATE) for i in ids]


@pytest.mark.parametrize("kwargs", [
    {"late_fraction": -0.1}, {"late_fraction": 1.1}, {"too_late_share": 2.0},
    {"assumed_watermark_s": 0.0},
])
def test_invalid_lateness_config_is_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="invalid lateness config"):
        LatenessConfig(**kwargs)


# --- build_schedule ----------------------------------------------------------

def _schedule(lateness: LatenessConfig = LATE, **kw: int) -> list[PlannedSend]:
    return build_schedule(_events(**kw), 42, lateness)


def test_schedule_without_lateness_is_the_plain_event_time_order() -> None:
    events = _events()
    plan = build_schedule(events, 42, LatenessConfig())
    assert [p.event for p in plan] == events
    assert all(p.delay_ms == 0 and p.band is None and p.send_at == p.event.event_time
               and p.stream_lag_ms == 0 and p.expected == "keep" for p in plan)


def test_schedule_contains_every_event_exactly_once_and_unchanged() -> None:
    events = _events()
    plan = _schedule()
    assert sorted(p.event.event_id for p in plan) == sorted(e.event_id for e in events)
    assert {p.event.event_id: p.event for p in plan} == {e.event_id: e for e in events}


def test_schedule_is_deterministic() -> None:
    assert _schedule() == _schedule()


def test_schedule_is_sorted_by_send_time_and_send_at_is_event_time_plus_delay() -> None:
    plan = _schedule()
    assert [p.send_at for p in plan] == sorted(p.send_at for p in plan)
    assert all(p.send_at == p.event.event_time + timedelta(milliseconds=p.delay_ms)
               for p in plan)


def test_no_event_is_planned_before_its_event_time() -> None:
    assert all(p.send_at >= p.event.event_time for p in _schedule())


def test_late_events_really_arrive_out_of_order_within_their_match() -> None:
    """A late event is sent after a newer event (higher match_seq) of the same match."""
    highest_seq_sent: dict[str, int] = defaultdict(int)
    overtaken = 0
    for p in _schedule():
        e = p.event
        if e.match_seq < highest_seq_sent[e.match_id]:
            overtaken += 1
            assert p.band is not None       # only late events can be overtaken
        highest_seq_sent[e.match_id] = max(highest_seq_sent[e.match_id], e.match_seq)
    assert overtaken > 0


def test_stream_lag_is_zero_for_on_time_events_and_never_exceeds_the_delay() -> None:
    plan = _schedule()
    assert all(p.stream_lag_ms == 0 for p in plan if p.band is None)
    assert all(0 <= p.stream_lag_ms <= p.delay_ms for p in plan)
    assert any(p.stream_lag_ms > 0 for p in plan)


def test_stream_lag_matches_a_brute_force_calculation() -> None:
    plan = _schedule(num_matches=3)
    for i, p in enumerate(plan):
        newer = [q.event.event_time for q in plan[:i] if q.event.event_time > p.event.event_time]
        want = (max(newer) - p.event.event_time) // timedelta(milliseconds=1) if newer else 0
        assert p.stream_lag_ms == want


def test_expected_is_may_drop_only_for_the_very_band() -> None:
    plan = _schedule()
    assert {p.expected for p in plan if p.band == "very"} == {"may_drop"}
    assert {p.expected for p in plan if p.band != "very"} == {"keep"}
    w_ms = LATE.assumed_watermark_s * 1000
    # "keep" events are at most half a watermark behind the stream: a safe margin.
    assert all(p.stream_lag_ms <= 0.5 * w_ms for p in plan if p.expected == "keep")


def test_lateness_does_not_change_event_ids_or_content() -> None:
    events = _events()
    assert _content_by_id([p.event for p in _schedule()]) == _content_by_id(events)

