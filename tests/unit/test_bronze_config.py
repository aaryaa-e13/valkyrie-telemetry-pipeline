"""Unit tests for streaming.config.BronzeJobConfig (env names and defaults)."""

from __future__ import annotations

import pytest

from streaming.config import BronzeJobConfig


def test_defaults_point_at_bronze() -> None:
    cfg = BronzeJobConfig.from_env({})
    assert cfg.table_path == "/data/lake/bronze/game_events"
    assert cfg.checkpoint_path == "/data/checkpoints/bronze"
    assert cfg.status_path == "/data/status/bronze.json"
    assert cfg.query_name == "bronze"
    assert cfg.allow_rebuild is False


def test_env_names() -> None:
    cfg = BronzeJobConfig.from_env({"BRONZE_PATH": "/t", "BRONZE_CHECKPOINT_PATH": "/c"})
    assert (cfg.table_path, cfg.checkpoint_path) == ("/t", "/c")
    # The Phase 1 names are gone on purpose: setting them changes nothing.
    old = BronzeJobConfig.from_env({"RAW_OUTPUT_PATH": "/old", "RAW_CHECKPOINT_PATH": "/old"})
    assert old.table_path == "/data/lake/bronze/game_events"


@pytest.mark.parametrize(("value", "expected"), [
    ("1", True), ("true", True), ("YES", True), ("0", False), ("", False), ("no", False),
])
def test_allow_rebuild_flag(value: str, expected: bool) -> None:
    assert BronzeJobConfig.from_env({"BRONZE_ALLOW_REBUILD": value}).allow_rebuild is expected
