"""Configuration for the bronze Kafka -> Delta streaming job (env vars only).

All settings have safe defaults for the Docker Compose setup; override with
environment variables, e.g. TRIGGER_INTERVAL="30 seconds".
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

_TRUE = {"1", "true", "yes"}


@dataclass(frozen=True)
class BronzeJobConfig:
    """Settings for streaming.bronze."""

    bootstrap_servers: str = "kafka:29092"
    topic: str = "game-events"
    starting_offsets: str = "earliest"     # only used when no checkpoint exists
    max_offsets_per_trigger: int = 50_000  # upper bound on rows per micro-batch
    fail_on_data_loss: bool = True         # stop if Kafka deleted unread data
    table_path: str = "/data/lake/bronze/game_events"
    checkpoint_path: str = "/data/checkpoints/bronze"
    status_path: str = "/data/status/bronze.json"  # "" disables the status file
    trigger_interval: str = "10 seconds"
    query_name: str = "bronze"
    # Start even if the table has data but the checkpoint is missing/empty. This
    # re-reads the topic from `starting_offsets` and appends every message again
    # (see the checkpoint-reset guard in streaming/bronze.py, DECISIONS D2.2).
    allow_rebuild: bool = False
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if self.max_offsets_per_trigger <= 0:
            raise ValueError("max_offsets_per_trigger must be > 0")
        if self.starting_offsets not in {"earliest", "latest"}:
            raise ValueError("starting_offsets must be 'earliest' or 'latest'")
        if not self.table_path or not self.checkpoint_path:
            raise ValueError("table_path and checkpoint_path are required")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> BronzeJobConfig:
        """Build config from environment variables (unset/empty = default)."""
        env = os.environ if env is None else env

        def get(name: str, default: str) -> str:
            value = env.get(name, "")
            return value if value != "" else default

        d = cls()
        return cls(
            bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.bootstrap_servers),
            topic=get("TOPIC_NAME", d.topic),
            starting_offsets=get("STARTING_OFFSETS", d.starting_offsets),
            max_offsets_per_trigger=int(
                get("MAX_OFFSETS_PER_TRIGGER", str(d.max_offsets_per_trigger))
            ),
            fail_on_data_loss=get("FAIL_ON_DATA_LOSS", str(d.fail_on_data_loss)).lower()
            in _TRUE,
            table_path=get("BRONZE_PATH", d.table_path),
            checkpoint_path=get("BRONZE_CHECKPOINT_PATH", d.checkpoint_path),
            # Not via get(): an explicitly empty STATUS_PATH means "disabled".
            status_path=env.get("STATUS_PATH", d.status_path),
            trigger_interval=get("TRIGGER_INTERVAL", d.trigger_interval),
            query_name=get("QUERY_NAME", d.query_name),
            allow_rebuild=get("BRONZE_ALLOW_REBUILD", "0").lower() in _TRUE,
            log_level=get("LOG_LEVEL", d.log_level),
        )
