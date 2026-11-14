"""Unit tests for event serialization."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from simulator.events import EventType, GameEvent, format_event_time

T = datetime(2026, 11, 14, 12, 0, 0, 123456, tzinfo=UTC)


def _event(**overrides: object) -> GameEvent:
    base: dict[str, object] = {
        "event_id": "e1", "event_type": EventType.KILL, "event_time": T,
        "match_id": "m1", "match_seq": 3, "patch_version": "14.3", "region": "EUW",
        "player_id": "p_1", "champion_id": "ch_x", "victim_id": "p_2",
    }
    base.update(overrides)
    return GameEvent(**base)  # type: ignore[arg-type]


def test_json_contains_envelope_and_all_nullable_fields() -> None:
    data = json.loads(_event().to_json_bytes())
    assert list(data)[:8] == [
        "event_id", "event_type", "schema_version", "event_time",
        "match_id", "match_seq", "patch_version", "region",
    ]
    assert data["event_type"] == "kill"
    assert data["schema_version"] == 1
    assert data["item_id"] is None  # present, explicitly null


def test_event_time_is_utc_millis_with_z() -> None:
    assert format_event_time(T) == "2026-11-14T12:00:00.123Z"
    plus_two = T.astimezone(timezone(timedelta(hours=2)))
    assert format_event_time(plus_two) == "2026-11-14T12:00:00.123Z"


def test_key_is_match_id_bytes() -> None:
    assert _event(match_id="abc").key == b"abc"


def test_naive_event_time_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(event_time=datetime(2026, 1, 1))


def test_match_seq_must_be_positive() -> None:
    with pytest.raises(ValueError, match="match_seq"):
        _event(match_seq=0)
