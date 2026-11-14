"""Simulator configuration.

Precedence (later wins): dataclass defaults < YAML file < environment < CLI flags.

Environment variables:
    KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME, LOG_LEVEL, and SIM_<FIELD_NAME_UPPER>
    for any other field, e.g. SIM_SEED=7, SIM_NUM_MATCHES=100.
"""

from __future__ import annotations

import dataclasses
import os
import types
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from simulator.catalog import CHAMPIONS

# Env vars with names shared with other components (not SIM_-prefixed).
_ENV_ALIASES = {
    "bootstrap_servers": "KAFKA_BOOTSTRAP_SERVERS",
    "topic": "TOPIC_NAME",
    "log_level": "LOG_LEVEL",
}


@dataclass(frozen=True)
class SimulatorConfig:
    """All simulator settings. See simulator/config.yaml for descriptions."""

    # Generation
    seed: int = 42
    num_matches: int = 2
    players_per_team: int = 5
    player_pool_size: int = 5000
    kills_per_match_min: int = 5
    kills_per_match_max: int = 15
    items_per_player_min: int = 2
    items_per_player_max: int = 6
    match_duration_s_min: int = 1200
    match_duration_s_max: int = 2400
    match_interval_s: int = 30
    patch_version: str = "14.3"
    regions: tuple[str, ...] = ("EUW", "NA", "KR", "BR")
    start_time: datetime | None = None  # None = "now" at run time

    # Kafka
    bootstrap_servers: str = "localhost:9092"
    topic: str = "game-events"
    client_id: str = "valkyrie-simulator"
    flush_timeout_s: float = 30.0
    rate_per_s: float = 0.0  # 0 = send as fast as possible

    # Output
    manifest_dir: str = "manifests"
    manifest_include_event_ids: bool = True
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Raise ValueError if settings are inconsistent."""
        errors: list[str] = []
        if self.num_matches < 0:
            errors.append("num_matches must be >= 0")
        if self.players_per_team < 1:
            errors.append("players_per_team must be >= 1")
        if 2 * self.players_per_team > len(CHAMPIONS):
            errors.append(f"players_per_team must be <= {len(CHAMPIONS) // 2} (unique champions)")
        if self.player_pool_size < 2 * self.players_per_team:
            errors.append("player_pool_size must be >= 2 * players_per_team")
        for lo, hi in (
            ("kills_per_match_min", "kills_per_match_max"),
            ("items_per_player_min", "items_per_player_max"),
            ("match_duration_s_min", "match_duration_s_max"),
        ):
            if not 0 <= getattr(self, lo) <= getattr(self, hi):
                errors.append(f"need 0 <= {lo} <= {hi}")
        if self.match_duration_s_min < 1:
            errors.append("match_duration_s_min must be >= 1")
        if self.rate_per_s < 0:
            errors.append("rate_per_s must be >= 0")
        if not self.regions:
            errors.append("regions must not be empty")
        if self.start_time is not None and self.start_time.tzinfo is None:
            errors.append("start_time must include a timezone, e.g. 2026-11-14T12:00:00Z")
        if errors:
            raise ValueError("invalid simulator config: " + "; ".join(errors))

    def public_dict(self) -> dict[str, Any]:
        """Settings as a JSON-friendly dict (for the run manifest)."""
        out: dict[str, Any] = {}
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if isinstance(value, datetime):
                value = value.isoformat()
            elif isinstance(value, tuple):
                value = list(value)
            out[f.name] = value
        return out


def _coerce(name: str, raw: Any, hint: Any) -> Any:
    """Convert a raw YAML/env/CLI value to the field's declared type."""
    if raw is None:
        return None
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):  # e.g. datetime | None
        non_none = [a for a in typing.get_args(hint) if a is not type(None)]
        return _coerce(name, raw, non_none[0])
    if origin is tuple:
        items = raw.split(",") if isinstance(raw, str) else list(raw)
        return tuple(str(i).strip() for i in items if str(i).strip())
    if hint is bool:
        if isinstance(raw, bool):
            return raw
        s = str(raw).strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"{name}: cannot parse boolean from {raw!r}")
    if hint is datetime:
        if isinstance(raw, datetime):
            return raw
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if hint in (int, float, str):
        try:
            return hint(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name}: expected {hint.__name__}, got {raw!r}") from exc
    raise TypeError(f"{name}: unsupported config type {hint!r}")


def load_config(
    yaml_path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> SimulatorConfig:
    """Build a SimulatorConfig from YAML, environment and explicit overrides.

    Args:
        yaml_path: Optional YAML file with field names as keys.
        env: Environment mapping (defaults to os.environ).
        overrides: Highest-priority values, e.g. from CLI flags. None values are ignored.

    Raises:
        ValueError: Unknown keys or invalid values.
    """
    env = os.environ if env is None else env
    hints = typing.get_type_hints(SimulatorConfig)
    names = {f.name for f in dataclasses.fields(SimulatorConfig)}
    raw: dict[str, Any] = {}

    if yaml_path is not None:
        with open(yaml_path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        unknown = set(data) - names
        if unknown:
            raise ValueError(f"unknown config keys in {yaml_path}: {sorted(unknown)}")
        raw.update(data)

    for name in names:
        env_name = _ENV_ALIASES.get(name, f"SIM_{name.upper()}")
        if env_name in env and env[env_name] != "":
            raw[name] = env[env_name]

    for name, value in (overrides or {}).items():
        if name not in names:
            raise ValueError(f"unknown config override: {name}")
        if value is not None:
            raw[name] = value

    values = {name: _coerce(name, value, hints[name]) for name, value in raw.items()}
    return SimulatorConfig(**values)
