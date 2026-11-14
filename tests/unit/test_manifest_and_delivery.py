"""Unit tests for the run manifest and the delivery callback (no Kafka needed)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from simulator.config import SimulatorConfig
from simulator.generator import generate_events
from simulator.manifest import build_manifest, event_ids_checksum, write_manifest
from simulator.producer import DeliveryTracker

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
