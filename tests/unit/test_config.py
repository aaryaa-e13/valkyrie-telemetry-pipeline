"""Unit tests for config loading and precedence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from simulator.config import load_config


def test_precedence_yaml_then_env_then_overrides(tmp_path: Path) -> None:
    yml = tmp_path / "c.yaml"
    yml.write_text("seed: 1\nnum_matches: 5\ntopic: from-yaml\n")
    env = {"SIM_SEED": "2", "TOPIC_NAME": "from-env"}
    cfg = load_config(yml, env=env, overrides={"seed": 3, "num_matches": None})
    assert cfg.seed == 3            # override beats env and yaml
    assert cfg.topic == "from-env"  # env beats yaml
    assert cfg.num_matches == 5     # None override is ignored


def test_types_are_coerced_from_env() -> None:
    env = {"SIM_RATE_PER_S": "2.5", "SIM_MANIFEST_INCLUDE_EVENT_IDS": "false",
           "SIM_START_TIME": "2026-11-14T12:00:00Z", "SIM_REGIONS": "EUW, NA"}
    cfg = load_config(env=env)
    assert cfg.rate_per_s == 2.5
    assert cfg.manifest_include_event_ids is False
    assert cfg.start_time == datetime(2026, 11, 14, 12, tzinfo=UTC)
    assert cfg.regions == ("EUW", "NA")


def test_unknown_yaml_key_is_rejected(tmp_path: Path) -> None:
    yml = tmp_path / "c.yaml"
    yml.write_text("seeed: 1\n")
    with pytest.raises(ValueError, match="unknown config keys"):
        load_config(yml, env={})


@pytest.mark.parametrize("overrides", [
    {"players_per_team": 11},
    {"kills_per_match_min": 5, "kills_per_match_max": 1},
    {"start_time": "2026-11-14T12:00:00"},  # no timezone
])
def test_invalid_values_are_rejected(overrides: dict) -> None:
    with pytest.raises(ValueError):
        load_config(env={}, overrides=overrides)


def test_shipped_yaml_files_load() -> None:
    assert load_config("simulator/config.yaml", env={}).num_matches == 2
    assert load_config("simulator/config.tiny.yaml", env={}).players_per_team == 1
