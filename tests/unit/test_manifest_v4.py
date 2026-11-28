"""Manifest v4 (bad_data): writer, reader, cap, cross-checks, the simulator run end to end."""

from __future__ import annotations

import copy
import dataclasses
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from common.manifest_reader import ManifestError, load_manifest, parse_manifest
from simulator import corrupt
from simulator import manifest as mf
from simulator.config import load_config
from simulator.events import GameEvent
from simulator.generator import generate_events
from simulator.main import run
from simulator.producer import DeliveryTracker, EventProducer, PartitionOffsets

START = datetime(2026, 11, 28, 12, 0, 0, tzinfo=UTC)
CFG = dataclasses.replace(load_config("simulator/config.bad.yaml"), seed=157)


def _build(cfg: Any = CFG, include_ids: bool = True, drop_offset: str | None = None,
           undelivered: int = 0) -> tuple[dict[str, Any], list[GameEvent], dict]:
    events = generate_events(cfg, START)
    bad = corrupt.corrupt_run(events, cfg.seed, cfg.bad_fraction, cfg.enabled_bad_kinds())
    delivered = [e.event_id for e in events][undelivered:]
    offsets = {e.event_id: (0, i) for i, e in enumerate(events)
               if e.event_id in bad and e.event_id != drop_offset}
    m = mf.build_manifest(
        run_id="v4", config=cfg.public_dict(), start_time=START, events=events,
        delivered_ids=delivered, failed_ids=[e.event_id for e in events][:undelivered],
        partitions_by_match={e.match_id: {0} for e in events},
        offsets_by_partition={0: PartitionOffsets(0, len(events) - 1, len(events))},
        include_event_ids=include_ids, bad=bad if cfg.bad_fraction > 0 else None,
        bad_offsets=offsets)
    return m, events, bad


def test_v4_roundtrip_with_bad_events(tmp_path: Path) -> None:
    m, events, bad = _build()
    assert m["manifest_version"] == 4
    bd = m["bad_data"]
    assert bd["bad_fraction"] == 0.2 and bd["enabled_kinds"] == list(corrupt.KINDS)
    assert bd["rules_version_expected"] == 1 and bd["bad_events_omitted_reason"] is None
    assert len(bd["bad_events"]) == len(bad)
    r = load_manifest(mf.write_manifest(m, tmp_path))
    assert r.version == 4 and r.bad_data is not None and r.bad_data.bad_events is not None
    rb = r.bad_data
    assert set(rb.bad_events) == set(bad) and set(rb.bad_events) <= r.event_ids
    assert r.event_ids == frozenset(e.event_id for e in events)       # logical ids
    region = corrupt.region_match((e.match_id for e in events), CFG.seed)
    assert rb.new_region_match == region
    n_dlq = sum(1 for b in bad.values() if b.expected.cls == "dlq")
    assert rb.expected_dlq_count == n_dlq == len(r.expected_dlq_ids)
    assert sum(rb.expected_flags.values()) == len(bad) - n_dlq
    assert sum(rb.bad_counts.values()) == len(bad)
    for eid, b in bad.items():
        e = rb.bad_events[eid]
        assert (e.kind, e.expected_class, e.expected_code, e.expected_key_kind) == (
            b.kind, b.expected.cls, b.expected.code, b.expected.key_kind)
        assert e.sent_event_id == b.sent_event_id
        if e.expected_class == "flag":
            assert e.dlq_key("t") is None
        elif e.expected_key_kind == "id":
            assert e.dlq_key("t") == f"id:{eid}"
        else:
            assert e.dlq_key("t") == f"off:t/{e.kafka_partition}/{e.kafka_offset}"
        assert e.id_readable == (b.kind not in {"malformed_json", "drop_event_id",
                                                "empty_event_id"})


def test_bad_data_is_null_when_bad_mode_is_off(tmp_path: Path) -> None:
    m, _, _ = _build(dataclasses.replace(CFG, bad_fraction=0.0))
    assert m["bad_data"] is None
    r = load_manifest(mf.write_manifest(m, tmp_path))
    assert r.version == 4 and r.bad_data is None and r.expected_dlq_ids == frozenset()


def test_only_delivered_bad_events_are_counted_and_listed() -> None:
    m, events, bad = _build(undelivered=40)
    listed = {e["event_id"] for e in m["bad_data"]["bad_events"]}
    assert listed == set(bad) - {e.event_id for e in events[:40]}
    assert sum(m["bad_data"]["bad_counts"].values()) == len(listed)
    parse_manifest(m)                                                   # consistent


def test_a_delivered_bad_event_without_an_offset_is_refused() -> None:
    events = generate_events(CFG, START)
    bad = corrupt.corrupt_run(events, CFG.seed, 0.2, corrupt.KINDS)
    with pytest.raises(ValueError, match="no recorded offset"):
        _build(drop_offset=next(iter(bad)))


def test_the_cap_and_no_ids_leave_the_list_out_with_a_reason(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mf, "MAX_BAD_EVENTS_LISTED", 3)
    m, _, bad = _build()
    assert m["bad_data"]["bad_events"] is None
    assert m["bad_data"]["bad_events_omitted_reason"] == mf.OMITTED_TOO_MANY_BAD
    assert sum(m["bad_data"]["bad_counts"].values()) == len(bad)        # counts still there
    r = parse_manifest(m)
    assert r.bad_data is not None and r.bad_data.bad_events is None and r.bad_list_missing
    m2, _, _ = _build(include_ids=False)
    assert m2["bad_data"]["bad_events_omitted_reason"] == mf.OMITTED_NO_IDS


@pytest.mark.parametrize("tamper,match", [
    (lambda bd: bd["bad_counts"].update(bad_patch=bd["bad_counts"].get("bad_patch", 0) + 1),
     "bad_counts"),
    (lambda bd: bd["expected_rejections"].update(
        bad_enum=bd["expected_rejections"]["bad_enum"] + 1), "expected_by_key_kind|rejections"),
    (lambda bd: bd["expected_by_key_kind"].update(id=bd["expected_by_key_kind"]["id"] + 1,
                                                  offset=bd["expected_by_key_kind"]["offset"]
                                                  - 1), "expected_by_key_kind"),
    (lambda bd: bd["bad_events"].append(dict(bd["bad_events"][0])), "listed twice"),
    (lambda bd: bd["bad_events"][0].update(event_id="nope"), "not a delivered event"),
    (lambda bd: bd["bad_events"][1].update(kafka_offset=bd["bad_events"][0]["kafka_offset"]),
     "share"),
    (lambda bd: bd["bad_events"][0].update(expected_class="maybe"), "unknown class"),
    (lambda bd: bd.update(bad_events=None), "no bad_events_omitted_reason"),
    (lambda bd: bd.update(bad_events_omitted_reason="x"), "omitted_reason is set"),
])
def test_the_reader_cross_checks_bad_data(tamper: Any, match: str) -> None:
    m, _, _ = _build()
    data = copy.deepcopy(m)
    tamper(data["bad_data"])
    with pytest.raises(ManifestError, match=match):
        parse_manifest(data)


def test_v4_needs_the_bad_data_key() -> None:
    m, _, _ = _build()
    data = copy.deepcopy(m)
    del data["bad_data"]
    with pytest.raises(ManifestError, match="bad_data"):
        parse_manifest(data)


def test_older_fixture_versions_still_read() -> None:
    for f in sorted(Path("tests/fixtures/manifests").glob("v*.json")):
        r = load_manifest(f)
        assert r.version in (1, 2, 3) and r.bad_data is None and not r.bad_list_missing


# --- delivery: per-message offsets for bad ids only, the corrupted value is sent ---------

class _Msg:
    def __init__(self, p: int, o: int) -> None:
        self._p, self._o = p, o

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


def test_the_tracker_keeps_offsets_for_bad_ids_only() -> None:
    t = DeliveryTracker(offsets_for=frozenset({"b"}))
    t.on_delivery("a", "m", None, _Msg(1, 5))  # type: ignore[arg-type]
    t.on_delivery("b", "m", None, _Msg(2, 9))  # type: ignore[arg-type]
    assert t.message_offsets == {"b": (2, 9)} and t.delivered_ids == ["a", "b"]


class _InnerProducer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value, "cb": on_delivery})

    def poll(self, timeout: float) -> int:
        return 0


def test_send_uses_the_given_value_and_keeps_the_key() -> None:
    p = EventProducer.__new__(EventProducer)
    p.topic, p.tracker, inner = "t", DeliveryTracker(), _InnerProducer()
    p._producer = inner  # type: ignore[assignment]
    e = generate_events(CFG, START)[0]
    p.send(e)
    p.send(e, b"{half")
    assert [c["value"] for c in inner.calls] == [e.to_json_bytes(), b"{half"]
    assert {c["key"] for c in inner.calls} == {e.match_id.encode()}
    inner.calls[1]["cb"](None, _Msg(0, 3))       # the callback still carries the logical id
    assert p.tracker.delivered_ids == [e.event_id]


class _FakeProducer:
    """EventProducer stand-in: records (event, value), acks partition 0, next offset."""

    last: _FakeProducer | None = None

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.sent: list[tuple[GameEvent, bytes | None]] = []
        self.tracker = DeliveryTracker()
        _FakeProducer.last = self

    def check_topic(self) -> int:
        return 6

    def send(self, event: GameEvent, value: bytes | None = None) -> None:
        self.sent.append((event, value))
        self.tracker.on_delivery(event.event_id, event.match_id, None,
                                 _Msg(0, len(self.sent) - 1))  # type: ignore[arg-type]

    def poll(self, timeout_s: float) -> None:
        return None

    def flush(self, timeout_s: float) -> int:
        return 0


def test_a_bad_run_sends_the_corrupted_bytes_and_writes_their_offsets(
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("simulator.main.EventProducer", _FakeProducer)
    cfg = dataclasses.replace(CFG, manifest_dir=str(tmp_path))
    now = START + timedelta(hours=1)
    assert run(cfg, clock=lambda: now) == 0
    fake = _FakeProducer.last
    assert fake is not None
    (path,) = tmp_path.glob("run_*.json")
    r = load_manifest(path)
    assert r.bad_data is not None and r.bad_data.bad_events is not None
    by_offset = {i: (e, v) for i, (e, v) in enumerate(fake.sent)}
    for b in r.bad_data.bad_events.values():
        e, v = by_offset[b.kafka_offset]
        assert e.event_id == b.event_id and v is not None and v != e.to_json_bytes()
        if b.kind != "malformed_json":
            assert json.loads(v).get("event_id") == b.sent_event_id
    good = [(e, v) for e, v in fake.sent if e.event_id not in r.bad_data.bad_events]
    assert good and all(v is None for _, v in good)                    # plain JSON
    assert len(r.bad_data.bad_events) == len(fake.tracker.message_offsets)
