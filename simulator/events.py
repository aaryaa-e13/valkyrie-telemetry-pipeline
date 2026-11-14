"""Game telemetry event model (schema version 1).

Every event shares an *envelope* (identity, time, match position, game version).
Type-specific fields are nullable and only set for the event types that use them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

SCHEMA_VERSION = 1


class EventType(StrEnum):
    """The fixed set of event types the simulator emits."""

    MATCH_START = "match_start"
    CHAMPION_PICK = "champion_pick"
    KILL = "kill"
    ITEM_PURCHASE = "item_purchase"
    MATCH_END = "match_end"


def format_event_time(dt: datetime) -> str:
    """Format a timezone-aware datetime as ISO-8601 UTC with milliseconds and 'Z'."""
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class GameEvent:
    """One game telemetry event.

    Attributes:
        event_id: Deterministic UUID identifying this logical event (dedupe key).
        event_type: What happened.
        event_time: When it happened in the game (timezone-aware, UTC).
        match_id: Match identifier; also the Kafka message key.
        match_seq: Position of this event within its match, starting at 1, no gaps.
        patch_version: Game patch the match was played on.
        region: Game server region.
        player_id: Acting player (picks, kills, purchases).
        champion_id: Acting player's champion.
        team: "blue" or "red" (picks).
        victim_id: Player who was killed (kills).
        item_id: Purchased item (purchases).
        gold: Gold spent (purchases).
        winning_team: "blue" or "red" (match end).
        duration_s: Match length in seconds (match end).
        schema_version: Version of this event format.
    """

    event_id: str
    event_type: EventType
    event_time: datetime
    match_id: str
    match_seq: int
    patch_version: str
    region: str
    player_id: str | None = None
    champion_id: str | None = None
    team: str | None = None
    victim_id: str | None = None
    item_id: str | None = None
    gold: int | None = None
    winning_team: str | None = None
    duration_s: int | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware (UTC)")
        if self.match_seq < 1:
            raise ValueError(f"match_seq must be >= 1, got {self.match_seq}")

    @property
    def key(self) -> bytes:
        """Kafka message key: the match id as UTF-8 bytes."""
        return self.match_id.encode("utf-8")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict. Envelope first; nullable fields always present."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "schema_version": self.schema_version,
            "event_time": format_event_time(self.event_time),
            "match_id": self.match_id,
            "match_seq": self.match_seq,
            "patch_version": self.patch_version,
            "region": self.region,
            "player_id": self.player_id,
            "champion_id": self.champion_id,
            "team": self.team,
            "victim_id": self.victim_id,
            "item_id": self.item_id,
            "gold": self.gold,
            "winning_team": self.winning_team,
            "duration_s": self.duration_s,
        }

    def to_json_bytes(self) -> bytes:
        """Serialize to compact UTF-8 JSON (the Kafka message value)."""
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")
