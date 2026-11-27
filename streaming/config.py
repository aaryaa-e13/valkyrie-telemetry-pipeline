"""Configuration for the streaming jobs (bronze and silver), env vars only.

All settings have safe defaults for the Docker Compose setup; override with
environment variables, e.g. TRIGGER_INTERVAL="30 seconds".
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

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


@dataclass(frozen=True)
class SilverJobConfig:
    """Settings for the silver job: bronze Delta -> deduplicated silver Delta (D2.3)."""

    bronze_path: str = "/data/lake/bronze/game_events"
    table_path: str = "/data/lake/silver/game_events"
    control_path: str = "/data/lake/_control/silver_batches"
    late_path: str = "/data/lake/silver/late_events"
    checkpoint_path: str = "/data/checkpoints/silver"
    status_path: str = "/data/status/silver.json"  # "" disables the status file
    trigger_interval: str = "10 seconds"
    query_name: str = "silver"
    # MERGE bounds = [min event_date - H, max event_date + H] of the batch. A
    # re-stamped duplicate more than H days from its first copy is inserted
    # twice, by design (parent plan D2); verify reports when that can happen.
    dedupe_horizon_days: int = 7
    # Event-time watermark W in seconds (DECISIONS D2.4): a keyed row is late iff
    # event_time <= max(clamped event_time of earlier batches of this query) - W.
    # Fixed per query id: start-up refuses a W that differs from the recorded one.
    watermark_delay_s: int = 60
    # Bronze files per silver micro-batch. 1000 is Delta's own default when no
    # limit is set; it is written down here so the cap is explicit (slice 4, C3)
    # and tests can use a small one. It also caps the initial snapshot.
    max_files_per_trigger: int = 1000
    # Phase 3 (D3.x). Empty = a sibling folder of the late / control table, so a config
    # that only sets the Phase 2 paths (tests, tools) can never touch the live lake:
    #   dead letter  <parent of late_path>/dead_letter
    #   resolutions  <parent of control_path>/dlq_resolutions
    #   replays      <parent of control_path>/replays   (written by replay, slice 3)
    dlq_path: str = ""
    resolutions_path: str = ""
    replays_path: str = ""
    # Quality rules version (streaming/gates.py). Fixed while a batch is in flight.
    rules_version: int = 1
    # Start although the silver checkpoint and silver/control tables disagree
    # (streaming/silver.py, DECISIONS D2.3). A reset silver checkpoint alone
    # needs no override: rebuilding silver from bronze is idempotent.
    allow_rebuild: bool = False

    def __post_init__(self) -> None:
        if self.dedupe_horizon_days < 0:
            raise ValueError("dedupe_horizon_days must be >= 0")
        if isinstance(self.watermark_delay_s, bool) or self.watermark_delay_s < 1:
            raise ValueError("watermark_delay_s must be an integer >= 1")
        if self.max_files_per_trigger < 1:
            raise ValueError("max_files_per_trigger must be >= 1")
        from streaming.gates import RULES_VERSIONS

        if not self.dlq_path:
            object.__setattr__(self, "dlq_path",
                               str(Path(self.late_path).parent / "dead_letter"))
        if not self.resolutions_path:
            object.__setattr__(self, "resolutions_path",
                               str(Path(self.control_path).parent / "dlq_resolutions"))
        if not self.replays_path:
            object.__setattr__(self, "replays_path",
                               str(Path(self.control_path).parent / "replays"))
        if isinstance(self.rules_version, bool) or self.rules_version not in RULES_VERSIONS:
            raise ValueError(f"rules_version must be one of {list(RULES_VERSIONS)}, "
                             f"got {self.rules_version!r}")
        paths = {self.bronze_path, self.table_path, self.control_path, self.checkpoint_path,
                 self.late_path, self.dlq_path, self.resolutions_path, self.replays_path}
        if "" in paths or len(paths) != 8:
            raise ValueError("bronze, silver, late, dead-letter, resolutions, replays, "
                             "control and checkpoint paths must be set and distinct")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SilverJobConfig:
        """Build config from environment variables (unset/empty = default)."""
        env = os.environ if env is None else env

        def get(name: str, default: str) -> str:
            value = env.get(name, "")
            return value if value != "" else default

        d = cls()
        return cls(
            bronze_path=get("BRONZE_PATH", d.bronze_path),
            table_path=get("SILVER_PATH", d.table_path),
            control_path=get("SILVER_CONTROL_PATH", d.control_path),
            late_path=get("SILVER_LATE_PATH", d.late_path),
            checkpoint_path=get("SILVER_CHECKPOINT_PATH", d.checkpoint_path),
            status_path=env.get("SILVER_STATUS_PATH", d.status_path),
            trigger_interval=get("TRIGGER_INTERVAL", d.trigger_interval),
            query_name=get("SILVER_QUERY_NAME", d.query_name),
            dedupe_horizon_days=int(get("DEDUPE_HORIZON_DAYS", str(d.dedupe_horizon_days))),
            watermark_delay_s=int(get("WATERMARK_DELAY_S", str(d.watermark_delay_s))),
            max_files_per_trigger=int(get("SILVER_MAX_FILES_PER_TRIGGER",
                                          str(d.max_files_per_trigger))),
            allow_rebuild=get("SILVER_ALLOW_REBUILD", "0").lower() in _TRUE,
            dlq_path=get("SILVER_DLQ_PATH", ""),
            resolutions_path=get("DLQ_RESOLUTIONS_PATH", ""),
            replays_path=get("REPLAYS_PATH", ""),
            rules_version=int(get("QUALITY_RULES_VERSION", str(d.rules_version))),
        )


SNAPSHOT_PARTITIONS_ENV = "DELTA_SNAPSHOT_PARTITIONS"


def delta_snapshot_partitions_from_env(env: Mapping[str, str] | None = None) -> int | None:
    """`spark.databricks.delta.snapshotPartitions` for the streaming job, or None.

    None (unset or empty) leaves Delta's default (50). Measured in the slice 3
    follow-up (DECISIONS D2.3): on this laptop 4 halves a small silver batch,
    because every Delta snapshot rebuild otherwise runs ~50 tasks on 4 cores.

    Raises:
        ValueError: the value is not an integer >= 1.
    """
    env = os.environ if env is None else env
    raw = env.get(SNAPSHOT_PARTITIONS_ENV, "").strip()
    if raw == "":
        return None
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{SNAPSHOT_PARTITIONS_ENV} must be an integer >= 1, got {raw!r}") \
            from None
    if value < 1:
        raise ValueError(f"{SNAPSHOT_PARTITIONS_ENV} must be an integer >= 1, got {value}")
    return value
