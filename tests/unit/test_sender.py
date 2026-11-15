"""Unit tests for the send loops, using a fake clock (no real sleeping)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from simulator.config import SimulatorConfig
from simulator.events import GameEvent
from simulator.generator import generate_events
from simulator.schedule import LatenessConfig, PlannedSend, build_schedule
from simulator.sender import SendStats, send_backfill, send_realtime

T0 = datetime(2026, 11, 15, 12, 0, 0, tzinfo=UTC)


class FakeClock:
    """Wall clock + monotonic clock that only move when someone waits.

    `wall_offset_s` simulates a wall clock that disagrees with the monotonic
    clock (for example after a clock adjustment).
    """

    def __init__(self, start: datetime = T0, early_return: float = 1.0) -> None:
        self.start = start
        self.mono = 1000.0
        self.wall_offset_s = 0.0
        self.early_return = early_return   # < 1: waits return early, like Producer.poll
        self.waits: list[float] = []

    def monotonic(self) -> float:
        return self.mono

    def wall_now(self) -> datetime:
        return self.start + timedelta(seconds=self.mono - 1000.0 + self.wall_offset_s)

    def wait(self, seconds: float) -> None:
        assert seconds > 0, "never wait for zero or negative time"
        self.waits.append(seconds)
        # An early return still lets some real time pass (at least 0.5 ms here);
        # without the floor, ever smaller steps would never reach the due time.
        self.mono += min(seconds, max(seconds * self.early_return, 0.0005))


def _plan(start: datetime, late_fraction: float = 0.0, num_matches: int = 3) -> list[PlannedSend]:
    cfg = SimulatorConfig(seed=42, num_matches=num_matches, match_duration_s_min=60,
                          match_duration_s_max=120, match_interval_s=5)
    lateness = LatenessConfig(late_fraction=late_fraction)
    return build_schedule(generate_events(cfg, start), 42, lateness)


class Recorder:
    """Stands in for EventProducer.send and notes the wall clock of each send."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.sent: list[tuple[GameEvent, datetime]] = []

    def __call__(self, event: GameEvent) -> None:
        self.sent.append((event, self.clock.wall_now()))


# --- realtime ----------------------------------------------------------------

@pytest.mark.parametrize("late_fraction", [0.0, 0.4])
def test_realtime_never_sends_an_event_before_its_send_time(late_fraction: float) -> None:
    clock = FakeClock()
    plan = _plan(T0 + timedelta(seconds=1), late_fraction)
    rec = Recorder(clock)
    stats = send_realtime(plan, rec, wait=clock.wait, wall_now=clock.wall_now,
                          monotonic=clock.monotonic)
    assert [e for e, _ in rec.sent] == [p.event for p in plan]      # same order, all sent
    for p, (_, sent_at) in zip(plan, rec.sent, strict=True):
        assert sent_at >= p.send_at
        assert sent_at >= p.event.event_time
    assert stats.sent == len(plan)
    assert stats.future_events == 0
    assert stats.max_ahead_ms is not None and stats.max_ahead_ms <= 0
    assert stats.max_behind_ms is not None and 0 <= stats.max_behind_ms <= 1


def test_realtime_takes_as_long_as_the_plan_and_waits_in_small_pieces() -> None:
    clock = FakeClock()
    plan = _plan(T0 + timedelta(seconds=1), late_fraction=0.4)
    send_realtime(plan, Recorder(clock), wait=clock.wait, wall_now=clock.wall_now,
                  monotonic=clock.monotonic, max_wait_s=0.5)
    elapsed = clock.wall_now() - T0
    assert elapsed == pytest.approx(plan[-1].send_at - T0, abs=timedelta(milliseconds=1))
    assert max(clock.waits) <= 0.5


def test_realtime_copes_with_waits_that_return_early() -> None:
    """Producer.poll may return before the timeout; the loop must re-check the clock."""
    clock = FakeClock(early_return=0.3)
    plan = _plan(T0 + timedelta(seconds=1))
    rec = Recorder(clock)
    stats = send_realtime(plan, rec, wait=clock.wait, wall_now=clock.wall_now,
                          monotonic=clock.monotonic)
    assert all(sent_at >= p.send_at for p, (_, sent_at) in zip(plan, rec.sent, strict=True))
    assert stats.future_events == 0


def test_realtime_when_behind_sends_immediately_in_order_and_reports_it() -> None:
    clock = FakeClock()
    plan = _plan(T0 - timedelta(seconds=30))        # the whole timeline started 30 s ago
    overdue = [p for p in plan if p.send_at <= T0]
    rec = Recorder(clock)
    stats = send_realtime(plan, rec, wait=clock.wait, wall_now=clock.wall_now,
                          monotonic=clock.monotonic)
    assert overdue
    assert all(sent_at == T0 for _, sent_at in rec.sent[:len(overdue)])   # no waiting
    assert [e for e, _ in rec.sent] == [p.event for p in plan]
    # The first event is the most overdue one (the match starts a little after -30 s).
    assert stats.max_behind_ms == (T0 - plan[0].send_at) // timedelta(milliseconds=1)
    assert 25_000 <= stats.max_behind_ms <= 30_000
    assert stats.future_events == 0


def test_realtime_wall_clock_guard_when_the_wall_clock_is_behind_the_monotonic_clock() -> None:
    """If the wall clock was set back 2 s mid-run, due times on the monotonic clock
    arrive 2 s too early. The guard must still hold every event until its event_time."""
    clock = FakeClock()
    plan = _plan(T0 + timedelta(seconds=1))
    rec = Recorder(clock)

    def wall_now() -> datetime:
        return clock.wall_now()

    anchored = False

    def monotonic() -> float:
        nonlocal anchored
        value = clock.monotonic()
        if not anchored:                 # first call = the anchor; then the clock jumps back
            anchored = True
            clock.wall_offset_s = -2.0
        return value

    stats = send_realtime(plan, rec, wait=clock.wait, wall_now=wall_now, monotonic=monotonic)
    for p, (_, sent_at) in zip(plan, rec.sent, strict=True):
        assert sent_at >= p.event.event_time
    assert stats.future_events == 0


def test_realtime_interrupt_stops_cleanly_and_keeps_the_stats() -> None:
    clock = FakeClock()
    plan = _plan(T0 + timedelta(seconds=1))
    rec = Recorder(clock)
    stats = SendStats()

    def wait(seconds: float) -> None:
        if len(rec.sent) >= 5:
            raise KeyboardInterrupt
        clock.wait(seconds)

    with pytest.raises(KeyboardInterrupt):
        send_realtime(plan, rec, wait=wait, stats=stats, wall_now=clock.wall_now,
                      monotonic=clock.monotonic)
    assert 5 <= len(rec.sent) < len(plan)
    assert stats.sent == len(rec.sent)


def test_realtime_with_an_empty_plan_does_nothing() -> None:
    clock = FakeClock()
    stats = send_realtime([], Recorder(clock), wait=clock.wait, wall_now=clock.wall_now,
                          monotonic=clock.monotonic)
    assert stats == SendStats()
    assert clock.waits == []


# --- backfill ----------------------------------------------------------------

def test_backfill_sends_everything_in_plan_order_without_sleeping() -> None:
    clock = FakeClock()
    plan = _plan(T0 - timedelta(hours=1), late_fraction=0.4)   # the past, with late events
    rec = Recorder(clock)
    stats = send_backfill(plan, rec, wall_now=clock.wall_now, monotonic=clock.monotonic,
                          sleep=clock.wait)
    assert [e for e, _ in rec.sent] == [p.event for p in plan]
    assert clock.waits == []
    assert stats.sent == len(plan)
    assert stats.future_events == 0
    assert stats.max_ahead_ms is not None and stats.max_ahead_ms <= 0
    assert stats.max_behind_ms is None            # not meaningful in backfill


def test_backfill_reports_events_from_the_future() -> None:
    """The D1.7c bug, on purpose: a timeline starting now is sent instantly."""
    clock = FakeClock()
    plan = _plan(T0)
    stats = send_backfill(plan, Recorder(clock), wall_now=clock.wall_now,
                          monotonic=clock.monotonic, sleep=clock.wait)
    assert stats.future_events > 0
    assert stats.max_ahead_ms is not None and stats.max_ahead_ms > 60_000


def test_backfill_rate_limit_spaces_the_sends() -> None:
    clock = FakeClock()
    plan = _plan(T0 - timedelta(hours=1), num_matches=1)
    rec = Recorder(clock)
    send_backfill(plan, rec, rate_per_s=10.0, wall_now=clock.wall_now,
                  monotonic=clock.monotonic, sleep=clock.wait)
    elapsed = (rec.sent[-1][1] - rec.sent[0][1]).total_seconds()
    assert elapsed == pytest.approx((len(plan) - 1) / 10.0, abs=1e-6)
