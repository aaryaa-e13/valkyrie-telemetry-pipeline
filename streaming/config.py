"""Configuration for the raw Kafka -> Parquet streaming job (env vars only).

All settings have safe defaults for the Docker Compose setup; override with
environment variables, e.g. TRIGGER_INTERVAL="30 seconds".
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class RawJobConfig:
    """Settings for streaming.raw_to_parquet."""

    bootstrap_servers: str = "kafka:29092"
    topic: str = "game-events"
    starting_offsets: str = "earliest"     # only used when no checkpoint exists
    max_offsets_per_trigger: int = 50_000  # upper bound on rows per micro-batch
    fail_on_data_loss: bool = True         # stop if Kafka deleted unread data
    output_path: str = "/data/lake/raw/game_events"
    checkpoint_path: str = "/data/checkpoints/raw_to_parquet"
    status_path: str = "/data/status/raw_to_parquet.json"  # "" disables the status file
    trigger_interval: str = "10 seconds"
    query_name: str = "raw_to_parquet"
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if self.max_offsets_per_trigger <= 0:
            raise ValueError("max_offsets_per_trigger must be > 0")
        if self.starting_offsets not in {"earliest", "latest"}:
            raise ValueError("starting_offsets must be 'earliest' or 'latest'")
        if not self.output_path or not self.checkpoint_path:
            raise ValueError("output_path and checkpoint_path are required")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RawJobConfig:
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
            in {"1", "true", "yes"},
            output_path=get("RAW_OUTPUT_PATH", d.output_path),
            checkpoint_path=get("RAW_CHECKPOINT_PATH", d.checkpoint_path),
            # Not via get(): an explicitly empty STATUS_PATH means "disabled".
            status_path=env.get("STATUS_PATH", d.status_path),
            trigger_interval=get("TRIGGER_INTERVAL", d.trigger_interval),
            query_name=get("QUERY_NAME", d.query_name),
            log_level=get("LOG_LEVEL", d.log_level),
        )
