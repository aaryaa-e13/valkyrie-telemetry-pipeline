"""Unit tests for streaming.config.SilverJobConfig (env names, defaults, validation)."""

from __future__ import annotations

import pytest

from streaming.config import SilverJobConfig


def test_defaults() -> None:
    cfg = SilverJobConfig.from_env({})
    assert cfg.bronze_path == "/data/lake/bronze/game_events"
    assert cfg.table_path == "/data/lake/silver/game_events"
    assert cfg.control_path == "/data/lake/_control/silver_batches"
    assert cfg.checkpoint_path == "/data/checkpoints/silver"
    assert cfg.status_path == "/data/status/silver.json"
    assert cfg.late_path == "/data/lake/silver/late_events"
    assert (cfg.query_name, cfg.dedupe_horizon_days, cfg.allow_rebuild) == ("silver", 7, False)
    assert (cfg.watermark_delay_s, cfg.max_files_per_trigger) == (60, 1000)


def test_env_names() -> None:
    cfg = SilverJobConfig.from_env({
        "BRONZE_PATH": "/b", "SILVER_PATH": "/s", "SILVER_CONTROL_PATH": "/k",
        "SILVER_CHECKPOINT_PATH": "/c", "SILVER_STATUS_PATH": "", "DEDUPE_HORIZON_DAYS": "3",
        "SILVER_ALLOW_REBUILD": "1", "SILVER_QUERY_NAME": "s2", "SILVER_LATE_PATH": "/l",
        "WATERMARK_DELAY_S": "90", "SILVER_MAX_FILES_PER_TRIGGER": "2"})
    assert (cfg.bronze_path, cfg.table_path, cfg.control_path, cfg.checkpoint_path) == (
        "/b", "/s", "/k", "/c")
    assert cfg.status_path == ""  # explicitly empty = disabled
    assert (cfg.dedupe_horizon_days, cfg.allow_rebuild, cfg.query_name) == (3, True, "s2")
    assert (cfg.late_path, cfg.watermark_delay_s, cfg.max_files_per_trigger) == ("/l", 90, 2)


@pytest.mark.parametrize("env", [
    {"DEDUPE_HORIZON_DAYS": "-1"},
    {"SILVER_PATH": "/data/lake/bronze/game_events"},   # silver == bronze
    {"SILVER_CONTROL_PATH": "/data/checkpoints/silver"},
    {"SILVER_LATE_PATH": "/data/lake/silver/game_events"},   # late == silver
    {"WATERMARK_DELAY_S": "0"},
    {"SILVER_MAX_FILES_PER_TRIGGER": "0"},
])
def test_invalid_config_is_refused(env: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        SilverJobConfig.from_env(env)


def test_phase3_paths_default_to_siblings_of_late_and_control() -> None:
    cfg = SilverJobConfig.from_env({})
    assert cfg.dlq_path == "/data/lake/silver/dead_letter"
    assert cfg.resolutions_path == "/data/lake/_control/dlq_resolutions"
    assert cfg.replays_path == "/data/lake/_control/replays"
    assert cfg.rules_version == 1
    moved = SilverJobConfig(late_path="/t/x/late", control_path="/t/y/control")
    assert (moved.dlq_path, moved.resolutions_path, moved.replays_path) == (
        "/t/x/dead_letter", "/t/y/dlq_resolutions", "/t/y/replays")


def test_phase3_env_names() -> None:
    cfg = SilverJobConfig.from_env({"SILVER_DLQ_PATH": "/d", "DLQ_RESOLUTIONS_PATH": "/r",
                                    "REPLAYS_PATH": "/p", "QUALITY_RULES_VERSION": "2"})
    assert (cfg.dlq_path, cfg.resolutions_path, cfg.replays_path, cfg.rules_version) == (
        "/d", "/r", "/p", 2)


@pytest.mark.parametrize("env", [
    {"QUALITY_RULES_VERSION": "3"},
    {"QUALITY_RULES_VERSION": "0"},
    {"SILVER_DLQ_PATH": "/data/lake/silver/late_events"},          # DLQ == late
    {"DLQ_RESOLUTIONS_PATH": "/data/lake/_control/silver_batches"},  # == control
    {"REPLAYS_PATH": "/data/lake/_control/dlq_resolutions"},       # == resolutions
])
def test_phase3_invalid_values_are_rejected(env: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        SilverJobConfig.from_env(env)
