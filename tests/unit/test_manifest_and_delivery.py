"""Unit tests for the run manifest and the delivery callback (no Kafka needed)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from simulator.config import SimulatorConfig
from simulator.generator import generate_events
from simulator.manifest import (
    MAX_LATE_EVENTS_LISTED,
    build_manifest,
    event_ids_checksum,
    write_manifest,
)
from simulator.producer import DeliveryTracker
from simulator.schedule import PlannedSend, build_schedule
from simulator.sender import SendStats

START = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)


class _FakeMsg:
    def __init__(self, partition: int, offset: int) -> None:
        self._p, self._o = partition, offset

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class _FakeErr:
    def __str__(self) -> str:
        return "KafkaError{code=_MSG_TIMED_OUT}"

    def retriable(self) -> bool:
        return True


def test_checksum_is_order_independent_and_ignores_duplicates() -> None:
    assert event_ids_checksum(["b", "a", "c"]) == event_ids_checksum(["c", "a", "b", "a"])
    assert event_ids_checksum(["a"]) != event_ids_checksum(["b"])


def test_delivery_callback_records_success_and_logs_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tracker = DeliveryTracker()
    tracker.on_delivery("e1", "m1", None, _FakeMsg(partition=3, offset=10))  # type: ignore[arg-type]
    with caplog.at_level(logging.ERROR):
        tracker.on_delivery("e2", "m1", _FakeErr(), None)  # type: ignore[arg-type]
    assert tracker.delivered_ids == ["e1"]
    assert tracker.failed_ids == ["e2"]
    assert tracker.partitions_by_match["m1"] == {3}
    failure = [r for r in caplog.records if r.getMessage() == "delivery failed"]
    assert failure and failure[0].fields["event_id"] == "e2"  # type: ignore[attr-defined]


def _manifest(include_ids: bool = True, drop: int = 0) -> tuple[dict, list]:
    cfg = SimulatorConfig(num_matches=3)
    events = generate_events(cfg, START)
    delivered = [e.event_id for e in events][drop:]
    failed = [e.event_id for e in events][:drop]
    by_match: dict[str, set[int]] = {e.match_id: {0} for e in events}
    m = build_manifest(
        run_id="test", config=cfg.public_dict(), start_time=START, events=events,
        delivered_ids=delivered, failed_ids=failed, partitions_by_match=by_match,
        include_event_ids=include_ids,
    )
    return m, events


def test_manifest_counts_and_ids_match_events() -> None:
    m, events = _manifest()
    assert m["totals"] == {"matches": 3, "events_generated": len(events),
                           "events_delivered": len(events), "events_failed": 0}
    assert sum(x["events_delivered"] for x in m["matches"].values()) == len(events)
    all_ids = [i for x in m["matches"].values() for i in x["event_ids"]]
    assert sorted(all_ids) == sorted(e.event_id for e in events)
    assert m["delivered_checksum"]["value"] == event_ids_checksum(all_ids)
    for x in m["matches"].values():
        assert x["max_match_seq"] == x["events_generated"]


def test_manifest_counts_only_acknowledged_events_as_delivered() -> None:
    m, events = _manifest(drop=2)
    assert m["totals"]["events_delivered"] == len(events) - 2
    assert m["totals"]["events_failed"] == 2
    assert len(m["failed_event_ids"]) == 2


def test_manifest_can_omit_event_ids() -> None:
    m, _ = _manifest(include_ids=False)
    assert all("event_ids" not in x for x in m["matches"].values())
    assert m["delivered_checksum"]["value"]  # checksum still present


def test_write_manifest_is_valid_json_and_leaves_no_temp_files(tmp_path: Path) -> None:
    m, _ = _manifest()
    path = write_manifest(m, tmp_path)
    assert path.name == "run_test.json"
    assert json.loads(path.read_text())["run_id"] == "test"
    assert [p.name for p in tmp_path.iterdir()] == ["run_test.json"]


# --- Kafka offsets (manifest v2) ----------------------------------------------------

def test_tracker_records_min_max_count_per_partition() -> None:
    tracker = DeliveryTracker()
    for i, (partition, offset) in enumerate([(3, 20), (3, 18), (0, 5), (3, 25)]):
        tracker.on_delivery(f"e{i}", "m1", None, _FakeMsg(partition, offset))  # type: ignore[arg-type]
    o3, o0 = tracker.offsets_by_partition[3], tracker.offsets_by_partition[0]
    assert (o3.min, o3.max, o3.count) == (18, 25, 3)
    assert (o0.min, o0.max, o0.count) == (5, 5, 1)


def test_failed_deliveries_do_not_record_offsets() -> None:
    tracker = DeliveryTracker()
    tracker.on_delivery("e1", "m1", _FakeErr(), None)  # type: ignore[arg-type]
    assert tracker.offsets_by_partition == {}


def test_manifest_writes_offsets_with_string_partition_keys() -> None:
    cfg = SimulatorConfig(num_matches=2)
    events = generate_events(cfg, START)
    tracker = DeliveryTracker()
    for i, e in enumerate(events):
        tracker.on_delivery(e.event_id, e.match_id, None, _FakeMsg(i % 2, 100 + i))  # type: ignore[arg-type]
    m = build_manifest(
        run_id="t", config=cfg.public_dict(), start_time=START, events=events,
        delivered_ids=tracker.delivered_ids, failed_ids=[],
        partitions_by_match=tracker.partitions_by_match,
        offsets_by_partition=tracker.offsets_by_partition,
    )
    assert m["manifest_version"] == 4   # v4 since slice 2 (bad_data null when off)
    assert set(m["kafka_offsets"]) == {"0", "1"}
    assert sum(o["count"] for o in m["kafka_offsets"].values()) == len(events)
    assert m["kafka_offsets"]["0"]["min"] == 100
    json.dumps(m)  # must stay JSON-serialisable


def test_manifest_without_offsets_still_has_empty_section() -> None:
    m, _ = _manifest()
    assert m["kafka_offsets"] == {}


# --- manifest v3: pacing and late events ---------------------------------------

def _v3(
    late_fraction: float = 0.4, include_ids: bool = True, drop: int = 0,
    with_stats: bool = True, **cfg_kw: object,
) -> tuple[dict, list[PlannedSend]]:
    """Build a v3 manifest from a real send plan. The first `drop` planned sends fail."""
    cfg = SimulatorConfig(num_matches=3, late_fraction=late_fraction, **cfg_kw)  # type: ignore[arg-type]
    events = generate_events(cfg, START)
    plan = build_schedule(events, cfg.seed, cfg.lateness())
    delivered = [p.event.event_id for p in plan][drop:]
    stats = SendStats(sent=len(plan), future_events=0, max_ahead_ms=-3, max_behind_ms=7)
    m = build_manifest(
        run_id="v3", config=cfg.public_dict(), start_time=START, events=events,
        delivered_ids=delivered, failed_ids=[p.event.event_id for p in plan][:drop],
        partitions_by_match={e.match_id: {0} for e in events},
        include_event_ids=include_ids, planned=plan, stats=stats if with_stats else None,
    )
    return m, plan


def test_v3_pacing_section_records_settings_bands_and_measurements() -> None:
    m, plan = _v3(pace="realtime", time_scale=10.0, late_too_late_share=0.5,
                  assumed_watermark_s=20.0)
    assert m["manifest_version"] == 4
    assert m["pacing"] == {
        "mode": "realtime",
        "time_scale": 10.0,
        "assumed_watermark_s": 20.0,
        "late_fraction": 0.4,
        "late_too_late_share": 0.5,
        "bands": {
            "slight": {"min_x_watermark": 0.1, "max_x_watermark": 0.5,
                       "min_delay_ms": 2000, "max_delay_ms": 10000, "expected": "keep"},
            "very": {"min_x_watermark": 2.0, "max_x_watermark": 3.0,
                     "min_delay_ms": 40000, "max_delay_ms": 60000, "expected": "may_drop"},
        },
        "measured": {"sent": len(plan), "future_events": 0, "max_ahead_ms": -3,
                     "max_behind_ms": 7},
    }
    json.dumps(m)


def test_v3_pacing_defaults_and_missing_stats() -> None:
    m, _ = _manifest()                      # no plan, no stats: an old-style caller
    assert m["pacing"]["mode"] == "backfill"
    assert m["pacing"]["time_scale"] == 1.0
    assert m["pacing"]["assumed_watermark_s"] == 60.0
    assert m["pacing"]["measured"] == {"sent": None, "future_events": None,
                                       "max_ahead_ms": None, "max_behind_ms": None}
    assert m["late_counts"] == {"slight": 0, "very": 0}
    assert m["late_events"] == []           # listed, and nobody was late
    assert m["late_events_omitted_reason"] is None


def test_v3_late_events_list_matches_the_plan_in_send_order() -> None:
    m, plan = _v3()
    late = [p for p in plan if p.band is not None]
    assert late and len(late) < len(plan)
    assert m["late_events"] == [
        {"event_id": p.event.event_id, "match_id": p.event.match_id, "delay_ms": p.delay_ms,
         "band": p.band, "expected": p.expected, "stream_lag_ms": p.stream_lag_ms}
        for p in late
    ]
    assert m["late_counts"] == {"slight": sum(p.band == "slight" for p in late),
                                "very": sum(p.band == "very" for p in late)}
    assert m["late_counts"]["slight"] > 0 and m["late_counts"]["very"] > 0
    assert m["late_events_omitted_reason"] is None


def test_v3_late_events_do_not_change_ids_totals_or_checksum() -> None:
    """Lateness only annotates: the ground truth is the same with and without it."""
    with_late, _ = _v3(late_fraction=0.4)
    without, _ = _v3(late_fraction=0.0)
    assert without["late_events"] == []
    for key in ("totals", "delivered_checksum", "matches", "failed_event_ids"):
        assert with_late[key] == without[key]


def test_v3_late_events_cover_only_delivered_events() -> None:
    m, plan = _v3(late_fraction=1.0, drop=10)
    failed = {p.event.event_id for p in plan[:10]}
    listed = {e["event_id"] for e in m["late_events"]}
    delivered = {i for match in m["matches"].values() for i in match["event_ids"]}
    assert listed == delivered              # everything delivered is late here ...
    assert not listed & failed              # ... and nothing undelivered is listed
    assert sum(m["late_counts"].values()) == len(plan) - 10


def test_v3_list_is_omitted_with_reason_when_ids_are_off_but_counts_stay() -> None:
    m, plan = _v3(include_ids=False)
    assert m["late_events"] is None
    assert m["late_events_omitted_reason"] == "no_manifest_ids"
    assert sum(m["late_counts"].values()) == sum(p.band is not None for p in plan) > 0


def test_v3_list_is_omitted_with_reason_above_the_size_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    n_late = sum(p.band is not None for p in _v3()[1])
    monkeypatch.setattr("simulator.manifest.MAX_LATE_EVENTS_LISTED", n_late)
    at_limit, _ = _v3()
    assert len(at_limit["late_events"]) == n_late            # exactly at the limit: listed
    monkeypatch.setattr("simulator.manifest.MAX_LATE_EVENTS_LISTED", n_late - 1)
    over, _ = _v3()
    assert over["late_events"] is None
    assert over["late_events_omitted_reason"] == "too_many_late_events"
    assert sum(over["late_counts"].values()) == n_late       # counts are always written


def test_the_size_limit_is_100_000_late_events() -> None:
    assert MAX_LATE_EVENTS_LISTED == 100_000
