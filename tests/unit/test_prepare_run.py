"""Unit tests for simulator.main.prepare_run and the new CLI flags."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from simulator.config import SimulatorConfig
from simulator.generator import generate_events
from simulator.main import REALTIME_LEAD, parse_args, prepare_run, run
from simulator.manifest import event_ids_checksum

NOW = datetime(2026, 11, 15, 9, 30, 0, 123456, tzinfo=UTC)
PAST = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)

# Same pinned values as tests/unit/test_event_id_pins.py (default config, 2 matches).
SEED_42_CHECKSUM = "811087b5b6bfadeb5df774b5880ed49126daeaa959066446d4974be729d19261"


def _cfg(**kw: object) -> SimulatorConfig:
    base: dict[str, object] = {"seed": 42, "num_matches": 2}
    base.update(kw)
    return SimulatorConfig(**base)  # type: ignore[arg-type]


def test_default_pace_is_backfill() -> None:
    assert SimulatorConfig().pace == "backfill"


def test_backfill_puts_every_event_in_the_past_and_the_last_one_at_now() -> None:
    start, plan = prepare_run(_cfg(), now=NOW)
    times = [p.event.event_time for p in plan]
    assert max(times) <= NOW
    assert NOW - max(times) < timedelta(milliseconds=1)
    assert min(times) >= start
    assert start < NOW - timedelta(minutes=20)       # default matches last 20-40 min


@pytest.mark.parametrize("overrides", [
    {},
    {"pace": "realtime"},
    {"pace": "realtime", "time_scale": 30.0},
    {"time_scale": 5.0},
    {"late_fraction": 0.5},
    {"pace": "realtime", "late_fraction": 1.0, "assumed_watermark_s": 10.0},
])
def test_no_setting_changes_the_event_ids(overrides: dict[str, object]) -> None:
    _, plan = prepare_run(_cfg(**overrides), now=NOW)
    assert len(plan) == 109
    assert event_ids_checksum(p.event.event_id for p in plan) == SEED_42_CHECKSUM


def test_backfill_with_explicit_past_start_and_scale_1_matches_the_old_behaviour() -> None:
    """Byte-identical to what the simulator sent before slice e for a fixed start_time."""
    cfg = _cfg(start_time=PAST)
    start, plan = prepare_run(cfg, now=NOW)
    assert start == PAST
    old = generate_events(cfg, PAST)
    assert [p.event.to_json_bytes() for p in plan] == [e.to_json_bytes() for e in old]


def test_backfill_with_start_time_that_ends_in_the_future_is_rejected() -> None:
    cfg = _cfg(start_time=NOW - timedelta(minutes=1))     # matches last 20+ minutes
    with pytest.raises(ValueError, match="in the future"):
        prepare_run(cfg, now=NOW)
    prepare_run(cfg, now=NOW, dry_run=True)               # printing only: allowed


def test_realtime_starts_one_second_after_now_on_a_whole_millisecond() -> None:
    start, plan = prepare_run(_cfg(pace="realtime"), now=NOW)
    assert start == NOW.replace(microsecond=124000) + REALTIME_LEAD
    assert all(p.event.event_time >= start for p in plan)
    assert all(p.send_at >= p.event.event_time for p in plan)


def test_realtime_time_scale_shortens_the_run() -> None:
    _, slow = prepare_run(_cfg(pace="realtime"), now=NOW)
    _, fast = prepare_run(_cfg(pace="realtime", time_scale=10.0), now=NOW)
    slow_span = slow[-1].send_at - NOW
    fast_span = fast[-1].send_at - NOW
    assert fast_span < slow_span / 9


def test_realtime_rejects_start_time_except_in_dry_run() -> None:
    cfg = _cfg(pace="realtime", start_time=PAST)
    with pytest.raises(ValueError, match="pace=realtime"):
        prepare_run(cfg, now=NOW)
    start, _ = prepare_run(cfg, now=NOW, dry_run=True)
    assert start == PAST


def test_lateness_settings_reach_the_schedule() -> None:
    _, plan = prepare_run(_cfg(late_fraction=1.0, late_too_late_share=1.0,
                               assumed_watermark_s=10.0), now=NOW)
    assert {p.band for p in plan} == {"very"}
    assert all(20_000 <= p.delay_ms <= 30_000 for p in plan)


def test_prepare_run_is_deterministic_for_the_same_now() -> None:
    cfg = _cfg(pace="realtime", time_scale=10.0, late_fraction=0.3)
    assert prepare_run(cfg, now=NOW) == prepare_run(cfg, now=NOW)


def test_zero_matches_gives_an_empty_plan() -> None:
    assert prepare_run(_cfg(num_matches=0), now=NOW)[1] == []
    assert prepare_run(_cfg(num_matches=0, pace="realtime"), now=NOW)[1] == []


def test_cli_flags_map_to_config_fields() -> None:
    args = parse_args(["--pace", "realtime", "--time-scale", "10", "--late-fraction", "0.3",
                       "--late-too-late-share", "0.5", "--assumed-watermark-s", "20"])
    assert (args.pace, args.time_scale, args.late_fraction, args.late_too_late_share,
            args.assumed_watermark_s) == ("realtime", 10.0, 0.3, 0.5, 20.0)
    assert parse_args([]).pace is None            # not given = keep YAML/env/default


# --- "last event would be in the future": fixed clock, never today's date --------

def _last_offset(cfg: SimulatorConfig) -> timedelta:
    """How long after start_time the last event of this config happens."""
    events = generate_events(cfg, PAST)
    return max(e.event_time for e in events) - PAST


def test_future_check_boundary_last_event_exactly_now_is_accepted() -> None:
    span = _last_offset(_cfg())
    start, plan = prepare_run(_cfg(start_time=NOW - span), now=NOW)
    assert max(p.event.event_time for p in plan) == NOW


def test_future_check_boundary_last_event_one_ms_after_now_is_rejected() -> None:
    span = _last_offset(_cfg())
    cfg = _cfg(start_time=NOW - span + timedelta(milliseconds=1))
    with pytest.raises(ValueError, match="in the future"):
        prepare_run(cfg, now=NOW)


def test_future_check_respects_time_scale() -> None:
    """A start 5 minutes ago is too late for 20-40 minute matches, but fine at 10x speed."""
    start = NOW - timedelta(minutes=5)
    with pytest.raises(ValueError, match="in the future"):
        prepare_run(_cfg(start_time=start), now=NOW)
    prepare_run(_cfg(start_time=start, time_scale=10.0), now=NOW)


class _FakeProducer:
    """Stands in for EventProducer: records sends, never touches Kafka."""

    instances: list[_FakeProducer] = []

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.sent: list[object] = []
        _FakeProducer.instances.append(self)

    def check_topic(self) -> int:
        return 6

    def send(self, event: object) -> None:
        self.sent.append(event)


def test_run_exits_2_and_sends_nothing_when_the_timeline_would_end_in_the_future(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Whole `run()` with a fixed clock: the check works on any calendar date."""
    _FakeProducer.instances.clear()
    monkeypatch.setattr("simulator.main.EventProducer", _FakeProducer)
    cfg = _cfg(start_time=NOW - timedelta(minutes=1), manifest_dir=str(tmp_path))

    assert run(cfg, clock=lambda: NOW) == 2

    assert len(_FakeProducer.instances) == 1
    assert _FakeProducer.instances[0].sent == []      # nothing reached "Kafka"
    assert list(tmp_path.iterdir()) == []             # and no manifest was written
