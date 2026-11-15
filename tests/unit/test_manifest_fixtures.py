"""Backward compatibility against REAL manifests committed under tests/fixtures.

`manifests/` is git-ignored, so these copies are what a fresh clone or CI has.
Each file was written by the simulator at a different stage of the project.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.manifest_reader import ManifestError, load_manifest
from simulator.manifest import event_ids_checksum

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "manifests"

V1 = FIXTURES / "v1_run_20261114T131326Z_seed42.json"
V2_PRE_E2 = FIXTURES / "v2_pre_e2_run_20261115T095317Z_seed1794736395320.json"
V2_E2 = FIXTURES / "v2_e2_run_20261115T101248Z_seed703.json"
V3 = FIXTURES / "v3_run_20261115T105409Z_seed723.json"

# file -> (version, delivered events)
EXPECTED = {V1: (1, 14), V2_PRE_E2: (2, 12), V2_E2: (2, 13), V3: (3, 13)}


def test_all_four_fixture_files_exist() -> None:
    assert sorted(p.name for p in FIXTURES.glob("*.json")) == sorted(p.name for p in EXPECTED)


@pytest.mark.parametrize("path", list(EXPECTED), ids=lambda p: p.name.split("_run_")[0])
def test_every_real_manifest_version_loads_with_its_ground_truth(path: Path) -> None:
    version, delivered = EXPECTED[path]
    m = load_manifest(path)
    raw = json.loads(path.read_text())
    assert m.version == version == raw["manifest_version"]
    assert m.events_delivered == delivered == len(m.event_ids)
    # The ids the reader returns are exactly the ones the writer checksummed.
    assert event_ids_checksum(m.event_ids) == raw["delivered_checksum"]["value"]


def test_v1_has_no_offsets_and_no_lateness_information() -> None:
    m = load_manifest(V1)
    assert m.kafka_offsets is None
    assert (m.pacing, m.late_counts, m.late_events) == (None, None, None)


@pytest.mark.parametrize("path", [V2_PRE_E2, V2_E2], ids=["pre_e2", "e2"])
def test_v2_has_offsets_but_lateness_is_not_recorded(path: Path) -> None:
    m = load_manifest(path)
    assert m.kafka_offsets
    assert sum(r.count for r in m.kafka_offsets.values()) == m.events_delivered
    # None, never empty: "not recorded" must not look like "everything was on time".
    assert (m.pacing, m.late_counts, m.late_events) == (None, None, None)


def test_the_e2_era_v2_file_really_has_the_extra_config_keys() -> None:
    """This run DID send late events; v2 just had nowhere to record them."""
    config = json.loads(V2_E2.read_text())["config"]
    assert config["pace"] == "realtime"
    assert config["late_fraction"] == 0.3
    assert {"time_scale", "late_too_late_share", "assumed_watermark_s"} <= set(config)
    assert "pace" not in json.loads(V2_PRE_E2.read_text())["config"]


def test_v3_records_pacing_and_late_events() -> None:
    m = load_manifest(V3)
    assert m.pacing is not None
    assert (m.pacing.mode, m.pacing.time_scale, m.pacing.assumed_watermark_s) == (
        "realtime", 10.0, 30.0)
    assert (m.pacing.future_events, m.pacing.max_ahead_ms) == (0, 0)
    assert m.late_counts == {"slight": 5, "very": 3}
    assert m.late_events is not None and len(m.late_events) == 8
    assert m.late_events_omitted_reason is None
    assert set(m.late_events) <= m.event_ids
    assert len(m.event_ids - set(m.late_events)) == 5      # not listed = sent on time
    for late in m.late_events.values():
        band = m.pacing.bands[late.band]
        assert band.min_delay_ms <= late.delay_ms <= band.max_delay_ms
        assert late.expected == band.expected
        assert 0 <= late.stream_lag_ms <= late.delay_ms


def test_a_future_version_of_a_real_file_is_rejected(tmp_path: Path) -> None:
    data = json.loads(V3.read_text())
    data["manifest_version"] = 4
    path = tmp_path / "v4.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ManifestError, match="not supported"):
        load_manifest(path)
