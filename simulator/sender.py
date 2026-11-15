"""Send loops for the two pace modes.

    backfill  Send in planned order as fast as possible (optionally rate-limited).
              The plan's `send_at` is used only for ordering; nothing waits for it.
    realtime  Send each event when the wall clock reaches its `send_at`.

Both loops measure, per event, how the event_time compares with the wall clock
at the moment of sending. `future_events` must stay 0: an event sent before its
own event_time is "from the future" (the bug recorded in DECISIONS D1.7c).

All clocks are parameters, so unit tests use a fake clock and never sleep.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from simulator.events import GameEvent
from simulator.schedule import PlannedSend

_ONE_MS = timedelta(milliseconds=1)


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class SendStats:
    """What the send loop measured. Updated in place, so it survives Ctrl+C.

    Attributes:
        sent: Events handed to the producer.
        future_events: Events sent before their own event_time (should be 0).
        max_ahead_ms: Largest `event_time - wall clock at send`. Zero or negative
            means every event_time was already in the past when it was sent.
        max_behind_ms: Realtime only. Largest `wall clock at send - send_at`:
            how far the simulator fell behind its plan (a busy laptop).
    """

    sent: int = 0
    future_events: int = 0
    max_ahead_ms: int | None = None
    max_behind_ms: int | None = None

    def record(self, planned: PlannedSend, wall: datetime, *, track_behind: bool) -> None:
        """Record one event that is about to be sent at wall-clock time `wall`."""
        ahead_ms = -((wall - planned.event.event_time) // _ONE_MS)  # rounds toward "ahead"
        if self.max_ahead_ms is None or ahead_ms > self.max_ahead_ms:
            self.max_ahead_ms = ahead_ms
        if ahead_ms > 0:
            self.future_events += 1
        if track_behind:
            behind_ms = (wall - planned.send_at) // _ONE_MS
            if self.max_behind_ms is None or behind_ms > self.max_behind_ms:
                self.max_behind_ms = behind_ms
        self.sent += 1


def send_backfill(
    plan: Sequence[PlannedSend],
    send: Callable[[GameEvent], None],
    *,
    rate_per_s: float = 0.0,
    stats: SendStats | None = None,
    wall_now: Callable[[], datetime] = _utc_now,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> SendStats:
    """Send every planned event in order without waiting for its `send_at`.

    Args:
        plan: Output of `build_schedule`.
        send: Called once per event (normally `EventProducer.send`).
        rate_per_s: Optional throttle in events/second; 0 = unlimited.
        stats: Object to update; a new one is created if omitted.
        wall_now, monotonic, sleep: Clock functions (replaced in tests).
    """
    stats = stats if stats is not None else SendStats()
    started = monotonic()
    for i, planned in enumerate(plan):
        if rate_per_s > 0:
            delay = started + i / rate_per_s - monotonic()
            if delay > 0:
                sleep(delay)
        stats.record(planned, wall_now(), track_behind=False)
        send(planned.event)
    return stats


def send_realtime(
    plan: Sequence[PlannedSend],
    send: Callable[[GameEvent], None],
    *,
    wait: Callable[[float], None],
    stats: SendStats | None = None,
    wall_now: Callable[[], datetime] = _utc_now,
    monotonic: Callable[[], float] = time.monotonic,
    max_wait_s: float = 0.5,
) -> SendStats:
    """Send each planned event when the wall clock reaches its `send_at`.

    How the waiting works:
      * The plan is anchored once: "wall clock now" is paired with "monotonic
        clock now". Due times are then tracked on the monotonic clock, which
        never jumps (the wall clock can, e.g. after WSL2 resumes from sleep).
      * `wait` may return early, so the loop always re-checks the clock.
      * Waits are cut into pieces of at most `max_wait_s`, so Ctrl+C is noticed
        quickly and delivery callbacks keep being served.
      * Just before sending, the wall clock is checked too: an event is never
        sent while its own `event_time` is still in the future.

    If the machine falls behind, events go out late but in the same order;
    `stats.max_behind_ms` shows by how much.

    Args:
        plan: Output of `build_schedule` (sorted by `send_at`).
        send: Called once per event (normally `EventProducer.send`).
        wait: Waits up to the given seconds (normally `EventProducer.poll`).
        stats: Object to update; a new one is created if omitted.
        wall_now, monotonic: Clock functions (replaced in tests).
        max_wait_s: Longest single wait.
    """
    stats = stats if stats is not None else SendStats()
    wall_anchor = wall_now()
    mono_anchor = monotonic()
    for planned in plan:
        due = mono_anchor + (planned.send_at - wall_anchor).total_seconds()
        while (remaining := due - monotonic()) > 0:
            wait(min(remaining, max_wait_s))
        while (early := (planned.event.event_time - (wall := wall_now())).total_seconds()) > 0:
            wait(min(early, max_wait_s))
        stats.record(planned, wall, track_behind=True)
        send(planned.event)
    return stats
