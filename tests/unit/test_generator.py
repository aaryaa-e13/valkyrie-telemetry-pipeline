"""Unit tests for the deterministic match generator."""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import UTC, datetime

import pytest

from simulator.config import SimulatorConfig
from simulator.events import EventType, GameEvent
from simulator.generator import EVENT_ID_NAMESPACE, generate_events, make_event_id

START = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)


def _cfg(**overrides: object) -> SimulatorConfig:
    base: dict[str, object] = {"seed": 42, "num_matches": 20, "players_per_team": 5}
    base.update(overrides)
    return SimulatorConfig(**base)  # type: ignore[arg-type]


def _by_match(events: list[GameEvent]) -> dict[str, list[GameEvent]]:
    """Group events by match, keeping emission order."""
    groups: dict[str, list[GameEvent]] = defaultdict(list)
    for e in events:
        groups[e.match_id].append(e)
    return groups


# --- Determinism -------------------------------------------------------------

def test_same_seed_produces_identical_events() -> None:
    first = generate_events(_cfg(), START)
    second = generate_events(_cfg(), START)
    assert [e.to_dict() for e in first] == [e.to_dict() for e in second]


def test_same_seed_produces_same_event_ids_even_with_different_start_time() -> None:
    later = datetime(2026, 12, 1, tzinfo=UTC)
    ids_a = [e.event_id for e in generate_events(_cfg(), START)]
    ids_b = [e.event_id for e in generate_events(_cfg(), later)]
    assert ids_a == ids_b


def test_different_seed_produces_different_events() -> None:
    ids_a = {e.event_id for e in generate_events(_cfg(seed=1), START)}
    ids_b = {e.event_id for e in generate_events(_cfg(seed=2), START)}
    assert ids_a.isdisjoint(ids_b)


def test_golden_event_id_is_stable() -> None:
    """Guards against accidental changes to generation order or id derivation.

    If this fails after an intentional change, every previously generated
    event_id has changed too: record it in docs/DECISIONS.md.
    """
    cfg = _cfg(num_matches=2, players_per_team=1, kills_per_match_min=1,
               kills_per_match_max=2, items_per_player_min=0, items_per_player_max=1,
               match_duration_s_min=60, match_duration_s_max=120, match_interval_s=5)
    first = generate_events(cfg, START)[0]
    assert first.event_type is EventType.MATCH_START
    assert first.match_id == "3eb13b90-4668-4257-bdd6-40fb06671ad1"
    assert first.event_id == "b94f52aa-6a40-5de2-a491-f9642c310b98"


def test_event_id_is_uuid5_of_match_id_and_seq() -> None:
    for e in generate_events(_cfg(num_matches=3), START):
        assert e.event_id == make_event_id(e.match_id, e.match_seq)
        assert e.event_id == str(uuid.uuid5(EVENT_ID_NAMESPACE, f"{e.match_id}:{e.match_seq}"))


def test_event_ids_are_globally_unique() -> None:
    events = generate_events(_cfg(num_matches=200), START)
    assert len({e.event_id for e in events}) == len(events)


# --- match_seq and ordering --------------------------------------------------

def test_match_seq_starts_at_one_with_no_gaps_or_duplicates() -> None:
    for match_id, events in _by_match(generate_events(_cfg(), START)).items():
        seqs = sorted(e.match_seq for e in events)
        assert seqs == list(range(1, len(events) + 1)), match_id


def test_events_of_one_match_are_emitted_in_order() -> None:
    """Within a match, emission order == match_seq order, and time never goes backwards."""
    for match_id, events in _by_match(generate_events(_cfg(), START)).items():
        seqs = [e.match_seq for e in events]
        assert seqs == sorted(seqs), match_id
        times = [e.event_time for e in events]
        assert times == sorted(times), match_id


def test_match_starts_with_start_and_ends_with_end() -> None:
    for events in _by_match(generate_events(_cfg(), START)).values():
        types = [e.event_type for e in events]
        assert types[0] is EventType.MATCH_START
        assert types[-1] is EventType.MATCH_END
        assert types.count(EventType.MATCH_START) == 1
        assert types.count(EventType.MATCH_END) == 1


def test_global_stream_is_sorted_by_event_time() -> None:
    events = generate_events(_cfg(), START)
    keys = [(e.event_time, e.match_id, e.match_seq) for e in events]
    assert keys == sorted(keys)


# --- Game rules ----------------------------------------------------------------

def test_picks_have_unique_champions_and_balanced_teams() -> None:
    cfg = _cfg(players_per_team=5)
    for events in _by_match(generate_events(cfg, START)).values():
        picks = [e for e in events if e.event_type is EventType.CHAMPION_PICK]
        assert len(picks) == 10
        assert len({p.champion_id for p in picks}) == 10
        assert len({p.player_id for p in picks}) == 10
        teams = [p.team for p in picks]
        assert teams.count("blue") == teams.count("red") == 5


def test_kills_are_between_opposing_teams() -> None:
    for events in _by_match(generate_events(_cfg(), START)).values():
        team_of = {e.player_id: e.team for e in events if e.event_type is EventType.CHAMPION_PICK}
        for k in (e for e in events if e.event_type is EventType.KILL):
            assert team_of[k.player_id] != team_of[k.victim_id]


def test_all_events_share_match_patch_and_region() -> None:
    for events in _by_match(generate_events(_cfg(), START)).values():
        assert len({e.region for e in events}) == 1
        assert {e.patch_version for e in events} == {"14.3"}


def test_event_times_are_utc_and_after_start() -> None:
    for e in generate_events(_cfg(), START):
        assert e.event_time.utcoffset() == UTC.utcoffset(None)
        assert e.event_time >= START


def test_zero_matches_yields_no_events() -> None:
    assert generate_events(_cfg(num_matches=0), START) == []


def test_naive_start_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        generate_events(_cfg(), datetime(2026, 11, 14, 12, 0, 0))
