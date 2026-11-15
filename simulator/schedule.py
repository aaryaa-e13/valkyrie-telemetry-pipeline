"""Send planning: when each event is sent, and which events are deliberately late.

Rule: **what an event is never changes here; only when it is sent.** The
generator (simulator/generator.py) stays the single source of event content.
This module is pure: no Kafka, no clock, no sleeping. Callers pass in "now".

Two steps:

1. `retime` places the generated timeline somewhere else in time (shift) and
   optionally compresses it (`time_scale`). event_id, match_seq and all payload
   fields are untouched; only `event_time` moves.
2. `build_schedule` decides the send order. An on-time event is sent at its
   `event_time`. A late event keeps its `event_time` but is sent `delay` later,
   so it arrives after newer events (out of order), like a client that was
   offline for a while.

Lateness formula (deterministic, independent of the generator's RNG):

    digest = sha256(f"{seed}:lateness:{event_id}".encode("utf-8")).digest()
    u1, u2, u3 = the first three 8-byte big-endian integers of digest, each / 2**64
    late   = u1 < late_fraction
    band   = "very" if u2 < too_late_share else "slight"
    lo, hi = VERY_BAND if band == "very" else SLIGHT_BAND
    delay  = round((lo + u3 * (hi - lo)) * assumed_watermark_s * 1000)   # ms

`hashlib.sha256` is used on purpose. Python's built-in `hash()` is salted per
process (PYTHONHASHSEED), so it would give different answers on every run.

Because each event's lateness depends only on (seed, event_id, settings):
  * adding matches, changing pace or time_scale never changes it;
  * raising `late_fraction` only adds late events, it never un-lates one.
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from simulator.events import GameEvent

# Delay bands as multiples of the assumed watermark W. The gap between them
# (0.5 W .. 2 W) is deliberate: a delay close to W would make "was it accepted?"
# depend on millisecond timing, and tests would be flaky.
SLIGHT_BAND: tuple[float, float] = (0.1, 0.5)   # late, but must still be accepted
VERY_BAND: tuple[float, float] = (2.0, 3.0)     # late enough that it may be rejected

BAND_SLIGHT = "slight"
BAND_VERY = "very"
EXPECT_KEEP = "keep"
EXPECT_MAY_DROP = "may_drop"

_ONE_MS = timedelta(milliseconds=1)
_ONE_US = timedelta(microseconds=1)


@dataclass(frozen=True)
class LatenessConfig:
    """Deliberate-lateness settings.

    Attributes:
        late_fraction: Share of events that are sent late (0 = feature off).
        too_late_share: Share of the late events that fall in the "very" band.
        assumed_watermark_s: The Phase 2 watermark we assume, in seconds. It is a
            placeholder: the bands are multiples of it, so changing this one
            number rescales every delay.
    """

    late_fraction: float = 0.0
    too_late_share: float = 0.2
    assumed_watermark_s: float = 30.0

    def __post_init__(self) -> None:
        errors: list[str] = []
        if not 0.0 <= self.late_fraction <= 1.0:
            errors.append("late_fraction must be between 0 and 1")
        if not 0.0 <= self.too_late_share <= 1.0:
            errors.append("too_late_share must be between 0 and 1")
        if not self.assumed_watermark_s > 0:
            errors.append("assumed_watermark_s must be > 0")
        if errors:
            raise ValueError("invalid lateness config: " + "; ".join(errors))


@dataclass(frozen=True, slots=True)
class PlannedSend:
    """One event plus the plan for sending it.

    Attributes:
        event: The event, unchanged.
        delay_ms: Planned delay after `event_time` (0 = on time).
        band: None (on time), "slight" or "very".
        send_at: `event_time + delay`. Realtime pace waits for this moment;
            backfill pace only uses it for ordering.
        stream_lag_ms: How far the newest `event_time` sent *before* this event
            is ahead of this event's `event_time` (0 if nothing newer was sent).
            This, not the delay, is what a watermark actually compares. It is
            never larger than `delay_ms`.
        expected: "keep" (the pipeline must accept it) or "may_drop" (a
            watermark of `assumed_watermark_s` is allowed, not obliged, to
            reject it).
    """

    event: GameEvent
    delay_ms: int
    band: str | None
    send_at: datetime
    stream_lag_ms: int
    expected: str


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")


def _scaled_offset_ms(event_time: datetime, ref_start: datetime, time_scale: float) -> int:
    """Offset of an event from the timeline start, compressed and rounded to whole ms."""
    offset_us = (event_time - ref_start) // _ONE_US
    if offset_us < 0:
        raise ValueError("event_time is before ref_start; pass the start_time used to generate")
    return round(offset_us / (1000.0 * time_scale))


def retime(
    events: Sequence[GameEvent],
    *,
    ref_start: datetime,
    new_start: datetime,
    time_scale: float = 1.0,
) -> list[GameEvent]:
    """Move (and optionally compress) the timeline. Only `event_time` changes.

    new event_time = new_start + (event_time - ref_start) / time_scale,
    with the offset rounded to whole milliseconds (the wire format has
    millisecond precision).

    `duration_s` is deliberately NOT scaled: it is game-clock data ("this match
    lasted 30 minutes"), not a timestamp.

    Args:
        events: Events as generated with `ref_start`.
        ref_start: The start_time that was passed to `generate_events`.
        new_start: Where the timeline should start now.
        time_scale: > 0. 10 means ten times faster (gaps are divided by 10).

    Returns:
        New event objects sorted by (event_time, match_id, match_seq), the same
        rule as `generate_events`. Each match's events stay in match_seq order.
    """
    _require_aware("ref_start", ref_start)
    _require_aware("new_start", new_start)
    if not time_scale > 0:
        raise ValueError("time_scale must be > 0")
    retimed = []
    for e in events:
        offset_ms = _scaled_offset_ms(e.event_time, ref_start, time_scale)
        retimed.append(dataclasses.replace(e, event_time=new_start + offset_ms * _ONE_MS))
    retimed.sort(key=lambda e: (e.event_time, e.match_id, e.match_seq))
    return retimed


def backfill_start(
    events: Sequence[GameEvent],
    *,
    ref_start: datetime,
    now: datetime,
    time_scale: float = 1.0,
) -> datetime:
    """Start time that puts the *last* event at or just before `now`.

    Use the result as `new_start` in `retime` (with the same `time_scale`):
    every event_time is then in the past, like replaying history.

    `now` is rounded down to a whole millisecond first, so rounding can never
    push the last event past the real `now`. With no events, returns that
    rounded `now`.
    """
    _require_aware("ref_start", ref_start)
    _require_aware("now", now)
    if not time_scale > 0:
        raise ValueError("time_scale must be > 0")
    now_ms = now.replace(microsecond=now.microsecond // 1000 * 1000)
    if not events:
        return now_ms
    span_ms = max(_scaled_offset_ms(e.event_time, ref_start, time_scale) for e in events)
    return now_ms - span_ms * _ONE_MS


def lateness_uniforms(seed: int, event_id: str) -> tuple[float, float, float]:
    """Three deterministic numbers in [0, 1) for one event (see module docstring)."""
    digest = hashlib.sha256(f"{seed}:lateness:{event_id}".encode()).digest()
    u1, u2, u3 = (int.from_bytes(digest[i:i + 8], "big") / 2**64 for i in (0, 8, 16))
    return u1, u2, u3


def plan_delay(seed: int, event_id: str, lateness: LatenessConfig) -> tuple[int, str | None]:
    """Decide one event's planned delay.

    Returns:
        (delay_ms, band): (0, None) for an on-time event, otherwise the delay in
        milliseconds and "slight" or "very".
    """
    u_late, u_band, u_delay = lateness_uniforms(seed, event_id)
    if u_late >= lateness.late_fraction:
        return 0, None
    band = BAND_VERY if u_band < lateness.too_late_share else BAND_SLIGHT
    lo, hi = VERY_BAND if band == BAND_VERY else SLIGHT_BAND
    delay_ms = round((lo + u_delay * (hi - lo)) * lateness.assumed_watermark_s * 1000)
    return delay_ms, band


def build_schedule(
    events: Sequence[GameEvent], seed: int, lateness: LatenessConfig
) -> list[PlannedSend]:
    """Plan the send order for a run.

    Args:
        events: Events with their final event_times (after `retime`).
        seed: The run's seed (feeds the lateness hash, not the generator RNG).
        lateness: Lateness settings; `late_fraction=0` gives the plain
            event_time order with no delays.

    Returns:
        Planned sends sorted by (send_at, event_time, match_id, match_seq).
        Every input event appears exactly once.
    """
    drafts: list[tuple[datetime, GameEvent, int, str | None]] = []
    for e in events:
        delay_ms, band = plan_delay(seed, e.event_id, lateness)
        drafts.append((e.event_time + delay_ms * _ONE_MS, e, delay_ms, band))
    drafts.sort(key=lambda d: (d[0], d[1].event_time, d[1].match_id, d[1].match_seq))

    planned: list[PlannedSend] = []
    newest_sent: datetime | None = None
    for send_at, e, delay_ms, band in drafts:
        lag_ms = 0
        if newest_sent is not None and newest_sent > e.event_time:
            lag_ms = (newest_sent - e.event_time) // _ONE_MS
        planned.append(PlannedSend(
            event=e,
            delay_ms=delay_ms,
            band=band,
            send_at=send_at,
            stream_lag_ms=lag_ms,
            # Decided by the band (the planned delay), not by stream_lag_ms. A very
            # late event with a small lag is only safe if nothing newer overtakes
            # it on the way to Kafka, which async sending cannot promise.
            expected=EXPECT_MAY_DROP if band == BAND_VERY else EXPECT_KEEP,
        ))
        if newest_sent is None or e.event_time > newest_sent:
            newest_sent = e.event_time
    return planned
