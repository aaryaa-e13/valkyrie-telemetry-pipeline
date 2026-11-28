"""Unit tests for common.manifest_reader: manifest v1, v2 and v3 must all be accepted."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from common.manifest_reader import (
    Band,
    LateEvent,
    ManifestError,
    OffsetRange,
    load_manifest,
    parse_manifest,
)

# Shape of a real v1 manifest (as written for seeds 501/600/601): no kafka_offsets.
V1: dict[str, Any] = {
    "manifest_version": 1,
    "run_id": "20261114T172700Z_seed501",
    "seed": 501,
    "topic": "game-events",
    "totals": {"matches": 2, "events_generated": 3, "events_delivered": 3, "events_failed": 0},
    "matches": {
        "m-a": {"events_generated": 2, "events_delivered": 2, "max_match_seq": 2,
                "partitions": [3], "event_ids": ["e-a1", "e-a2"]},
        "m-b": {"events_generated": 1, "events_delivered": 1, "max_match_seq": 1,
                "partitions": [0], "event_ids": ["e-b1"]},
    },
    "failed_event_ids": [],
}

V2: dict[str, Any] = {
    **copy.deepcopy(V1),
    "manifest_version": 2,
    "run_id": "20261114T180000Z_seed1794672345123",
    "seed": 1794672345123,
    "kafka_offsets": {"3": {"min": 20, "max": 21, "count": 2},
                      "0": {"min": 13, "max": 13, "count": 1}},
}

# Shape of a v3 manifest: v2 plus pacing, late_counts, late_events.
V3: dict[str, Any] = {
    **copy.deepcopy(V2),
    "manifest_version": 3,
    "pacing": {
        "mode": "realtime", "time_scale": 10.0, "assumed_watermark_s": 30.0,
        "late_fraction": 0.3, "late_too_late_share": 0.2,
        "bands": {
            "slight": {"min_x_watermark": 0.1, "max_x_watermark": 0.5,
                       "min_delay_ms": 3000, "max_delay_ms": 15000, "expected": "keep"},
            "very": {"min_x_watermark": 2.0, "max_x_watermark": 3.0,
                     "min_delay_ms": 60000, "max_delay_ms": 90000, "expected": "may_drop"},
        },
        "measured": {"sent": 3, "future_events": 0, "max_ahead_ms": 0, "max_behind_ms": 2},
    },
    "late_counts": {"slight": 1, "very": 1},
    "late_events_omitted_reason": None,
    "late_events": [
        {"event_id": "e-a2", "match_id": "m-a", "delay_ms": 4000, "band": "slight",
         "expected": "keep", "stream_lag_ms": 1500},
        {"event_id": "e-b1", "match_id": "m-b", "delay_ms": 70000, "band": "very",
         "expected": "may_drop", "stream_lag_ms": 9000},
    ],
}


def _write(tmp_path: Path, data: Any) -> Path:
    path = tmp_path / "run.json"
    path.write_text(json.dumps(data))
    return path


def test_v1_manifest_is_accepted_without_offsets(tmp_path: Path) -> None:
    m = load_manifest(_write(tmp_path, V1))
    assert m.version == 1
    assert m.seed == 501
    assert m.kafka_offsets is None
    assert m.event_pairs == (("m-a", "e-a1"), ("m-a", "e-a2"), ("m-b", "e-b1"))
    assert m.event_ids == frozenset({"e-a1", "e-a2", "e-b1"})
    assert m.events_delivered == 3


def test_v2_manifest_has_int_partition_offsets(tmp_path: Path) -> None:
    m = load_manifest(_write(tmp_path, V2))
    assert m.version == 2
    assert m.kafka_offsets == {3: OffsetRange(20, 21, 2), 0: OffsetRange(13, 13, 1)}
    assert sum(o.count for o in m.kafka_offsets.values()) == m.events_delivered


def test_missing_version_defaults_to_v1() -> None:
    data = copy.deepcopy(V1)
    del data["manifest_version"]
    assert parse_manifest(data).version == 1


@pytest.mark.parametrize("mutate, message", [
    (lambda d: d.update(manifest_version=99), "not supported"),
    (lambda d: d["matches"]["m-a"].pop("event_ids"), "no event_ids"),
    (lambda d: d.pop("run_id"), "missing or invalid"),
    (lambda d: d.pop("totals"), "missing or invalid"),
])
def test_unusable_manifests_raise_clear_errors(mutate, message: str) -> None:
    data = copy.deepcopy(V1)
    mutate(data)
    with pytest.raises(ManifestError, match=message):
        parse_manifest(data)


@pytest.mark.parametrize("bad_offsets", [
    None, {"3": {"min": 1}}, {"x": {"min": 1, "max": 2, "count": 1}}, ["not", "a", "dict"],
])
def test_v2_with_bad_offsets_raises(bad_offsets: Any) -> None:
    data = copy.deepcopy(V2)
    data["kafka_offsets"] = bad_offsets
    with pytest.raises(ManifestError, match="kafka_offsets"):
        parse_manifest(data)


def test_v2_without_offsets_key_raises() -> None:
    data = copy.deepcopy(V2)
    del data["kafka_offsets"]
    with pytest.raises(ManifestError, match="kafka_offsets"):
        parse_manifest(data)


def test_invalid_json_file_raises(tmp_path: Path) -> None:
    path = tmp_path / "run.json"
    path.write_text("{truncated")
    with pytest.raises(ManifestError, match="not valid JSON"):
        load_manifest(path)


# --- v3 ------------------------------------------------------------------------

@pytest.mark.parametrize("old", [V1, V2], ids=["v1", "v2"])
def test_v1_and_v2_have_no_lateness_information_at_all(old: dict[str, Any]) -> None:
    """Not recorded is None, never an empty collection (that would read as 'on time')."""
    m = parse_manifest(copy.deepcopy(old))
    assert m.pacing is None
    assert m.late_counts is None
    assert m.late_events is None
    assert m.late_events_omitted_reason is None


def test_v2_with_extra_config_keys_from_slice_e2_is_still_plain_v2() -> None:
    data = copy.deepcopy(V2)
    data["config"] = {"seed": 1, "pace": "realtime", "time_scale": 10.0, "late_fraction": 0.3,
                      "late_too_late_share": 0.2, "assumed_watermark_s": 30.0}
    m = parse_manifest(data)
    assert m.version == 2
    assert m.pacing is None and m.late_events is None


def test_v3_manifest_is_parsed() -> None:
    m = parse_manifest(copy.deepcopy(V3))
    assert m.version == 3
    assert m.event_ids == frozenset({"e-a1", "e-a2", "e-b1"})       # unchanged ground truth
    assert m.kafka_offsets is not None and m.kafka_offsets[3] == OffsetRange(20, 21, 2)
    assert m.pacing is not None
    assert (m.pacing.mode, m.pacing.time_scale, m.pacing.assumed_watermark_s) == (
        "realtime", 10.0, 30.0)
    assert (m.pacing.late_fraction, m.pacing.late_too_late_share) == (0.3, 0.2)
    assert m.pacing.bands == {"slight": Band(3000, 15000, "keep"),
                              "very": Band(60000, 90000, "may_drop")}
    assert (m.pacing.future_events, m.pacing.max_ahead_ms, m.pacing.max_behind_ms) == (0, 0, 2)
    assert m.late_counts == {"slight": 1, "very": 1}
    assert m.late_events == {
        "e-a2": LateEvent("e-a2", "m-a", 4000, "slight", "keep", 1500),
        "e-b1": LateEvent("e-b1", "m-b", 70000, "very", "may_drop", 9000),
    }
    assert "e-a1" not in m.late_events       # v3: not listed = sent on time
    assert m.late_events_omitted_reason is None


def test_v3_with_no_late_events_gives_an_empty_mapping_not_none() -> None:
    data = copy.deepcopy(V3)
    data["late_events"] = []
    data["late_counts"] = {"slight": 0, "very": 0}
    m = parse_manifest(data)
    assert m.late_events == {}               # known: everything was on time
    assert m.late_counts == {"slight": 0, "very": 0}


def test_v3_with_omitted_list_keeps_counts_and_reason() -> None:
    data = copy.deepcopy(V3)
    data["late_events"] = None
    data["late_events_omitted_reason"] = "too_many_late_events"
    m = parse_manifest(data)
    assert m.late_events is None
    assert m.late_events_omitted_reason == "too_many_late_events"
    assert m.late_counts == {"slight": 1, "very": 1}
    assert m.pacing is not None


def test_v3_measured_values_may_be_null() -> None:
    data = copy.deepcopy(V3)
    data["pacing"]["measured"] = {"sent": None, "future_events": None, "max_ahead_ms": None,
                                  "max_behind_ms": None}
    m = parse_manifest(data)
    assert m.pacing is not None and m.pacing.max_ahead_ms is None


def _v3_mutations() -> list[tuple[str, Any]]:
    def drop(key: str) -> Any:
        return lambda d: d.pop(key)

    def unknown_event(d: dict[str, Any]) -> None:
        d["late_events"][0]["event_id"] = "not-delivered"

    def twice(d: dict[str, Any]) -> None:
        d["late_events"].append(dict(d["late_events"][0]))

    def bad_band(d: dict[str, Any]) -> None:
        d["late_events"][0]["band"] = "huge"

    def bad_mode(d: dict[str, Any]) -> None:
        d["pacing"]["mode"] = "burst"

    def wrong_counts(d: dict[str, Any]) -> None:
        d["late_counts"]["very"] = 5

    def null_without_reason(d: dict[str, Any]) -> None:
        d["late_events"] = None

    def list_with_reason(d: dict[str, Any]) -> None:
        d["late_events_omitted_reason"] = "no_manifest_ids"

    def no_bands(d: dict[str, Any]) -> None:
        del d["pacing"]["bands"]["very"]

    return [
        ("no pacing", drop("pacing")), ("no late_counts", drop("late_counts")),
        ("no late_events", drop("late_events")), ("unknown event", unknown_event),
        ("listed twice", twice), ("bad band", bad_band), ("bad mode", bad_mode),
        ("wrong counts", wrong_counts), ("null without reason", null_without_reason),
        ("list with reason", list_with_reason), ("missing band", no_bands),
    ]


@pytest.mark.parametrize(("name", "mutate"), _v3_mutations(),
                         ids=[n for n, _ in _v3_mutations()])
def test_broken_v3_manifests_raise_clear_errors(name: str, mutate: Any) -> None:
    data = copy.deepcopy(V3)
    mutate(data)
    with pytest.raises(ManifestError, match="invalid pacing/late_events"):
        parse_manifest(data)


@pytest.mark.parametrize("version", [0, 5, 99, "3", None])
def test_unknown_versions_are_still_rejected(version: Any) -> None:
    data = copy.deepcopy(V3)
    data["manifest_version"] = version
    with pytest.raises(ManifestError, match="not supported"):
        parse_manifest(data)


def test_real_v3_manifest_with_late_events_roundtrips(tmp_path: Path) -> None:
    """Writer and reader agree on v3, including the per-event late list."""
    from datetime import UTC, datetime

    from simulator.config import SimulatorConfig
    from simulator.generator import generate_events
    from simulator.manifest import build_manifest, write_manifest
    from simulator.schedule import build_schedule
    from simulator.sender import SendStats

    start = datetime(2026, 11, 14, tzinfo=UTC)
    cfg = SimulatorConfig(num_matches=3, pace="realtime", time_scale=10.0, late_fraction=0.4)
    events = generate_events(cfg, start)
    plan = build_schedule(events, cfg.seed, cfg.lateness())
    for include_ids in (True, False):
        manifest = build_manifest(
            run_id=f"rt3-{include_ids}", config=cfg.public_dict(), start_time=start,
            events=events, delivered_ids=[e.event_id for e in events], failed_ids=[],
            partitions_by_match={e.match_id: {1} for e in events},
            include_event_ids=include_ids, planned=plan,
            stats=SendStats(sent=len(plan), max_ahead_ms=0, max_behind_ms=1),
        )
        path = write_manifest(manifest, tmp_path)
        if not include_ids:      # unchanged rule: no ids = unusable for verification
            with pytest.raises(ManifestError, match="no event_ids"):
                load_manifest(path)
            continue
        m = load_manifest(path)
        late = [p for p in plan if p.band is not None]
        assert m.version == 4 and m.bad_data is None   # the writer is v4 now
        assert m.pacing is not None and m.pacing.mode == "realtime"
        assert m.pacing.max_behind_ms == 1
        assert m.late_events is not None
        assert list(m.late_events) == [p.event.event_id for p in late]     # send order
        assert all(m.late_events[p.event.event_id].delay_ms == p.delay_ms for p in late)
        assert set(m.late_events) <= m.event_ids


def test_real_manifest_written_by_simulator_roundtrips(tmp_path: Path) -> None:
    """The simulator's own writer and this reader must agree (current version)."""
    from datetime import UTC, datetime

    from simulator.config import SimulatorConfig
    from simulator.generator import generate_events
    from simulator.manifest import build_manifest, write_manifest
    from simulator.producer import PartitionOffsets

    cfg = SimulatorConfig(num_matches=2)
    events = generate_events(cfg, datetime(2026, 11, 14, tzinfo=UTC))
    ids = [e.event_id for e in events]
    offsets = {1: PartitionOffsets(0, len(ids) - 1, len(ids))}
    manifest = build_manifest(
        run_id="rt", config=cfg.public_dict(), start_time=datetime(2026, 11, 14, tzinfo=UTC),
        events=events, delivered_ids=ids, failed_ids=[],
        partitions_by_match={e.match_id: {1} for e in events}, offsets_by_partition=offsets,
    )
    m = load_manifest(write_manifest(manifest, tmp_path))
    assert m.version == 4 and m.bad_data is None   # current writer: v4
    assert m.late_events == {}
    assert m.event_ids == frozenset(ids)
    assert m.kafka_offsets == {1: OffsetRange(0, len(ids) - 1, len(ids))}
