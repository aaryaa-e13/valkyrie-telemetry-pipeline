"""Unit tests for common.manifest_reader: manifest v1 and v2 must both be accepted."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from common.manifest_reader import ManifestError, OffsetRange, load_manifest, parse_manifest

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


def test_real_manifest_written_by_simulator_roundtrips(tmp_path: Path) -> None:
    """The simulator's own writer and this reader must agree (v2)."""
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
    assert m.version == 2
    assert m.event_ids == frozenset(ids)
    assert m.kafka_offsets == {1: OffsetRange(0, len(ids) - 1, len(ids))}
