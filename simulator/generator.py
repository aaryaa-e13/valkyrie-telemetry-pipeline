"""Deterministic match simulation.

Given a config (including `seed`) and a start time, `generate_events` always
returns exactly the same events, including the same event_ids. All randomness
comes from one `random.Random(seed)` instance, consumed in a fixed order.

Note: determinism is guaranteed for the same Python minor version (3.11 is
pinned). `random.Random` is stable in practice, but CPython does not formally
promise identical sequences across versions.

The champions and items are fictional.
"""

from __future__ import annotations

import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from simulator.catalog import CHAMPIONS, ITEMS, TEAMS
from simulator.config import SimulatorConfig
from simulator.events import EventType, GameEvent

# Fixed namespace for deriving event_ids. Never change it: doing so would change
# every event_id and break deduplication against previously ingested data.
EVENT_ID_NAMESPACE = uuid.UUID("6a1f6c2e-5b7d-4c1e-9a3f-2d8e4b7c9f10")

PICK_GAP_MS = (1_000, 4_000)          # time between champion picks
FIRST_MATCH_JITTER_MS = (0, 5_000)    # small random offset per match start


def make_event_id(match_id: str, match_seq: int) -> str:
    """Derive a deterministic event_id from the event's natural key.

    The same (match_id, match_seq) always yields the same id, so a resent or
    regenerated event is recognisable as a duplicate downstream.
    """
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, f"{match_id}:{match_seq}"))


@dataclass(frozen=True, slots=True)
class _Player:
    player_id: str
    champion_id: str
    team: str


def _uuid_from_rng(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _simulate_match(
    rng: random.Random, cfg: SimulatorConfig, match_start: datetime
) -> list[GameEvent]:
    """Simulate one match and return its events in match_seq order."""
    match_id = _uuid_from_rng(rng)
    region = rng.choice(cfg.regions)
    n_players = 2 * cfg.players_per_team

    player_numbers = rng.sample(range(cfg.player_pool_size), n_players)
    champions = rng.sample(CHAMPIONS, n_players)
    players = [
        _Player(
            player_id=f"p_{num:06d}",
            champion_id=champ,
            team=TEAMS[0] if i < cfg.players_per_team else TEAMS[1],
        )
        for i, (num, champ) in enumerate(zip(player_numbers, champions, strict=True))
    ]

    # Build (offset_ms, kind, payload) tuples, then number them with match_seq.
    timeline: list[tuple[int, EventType, dict[str, object]]] = [
        (0, EventType.MATCH_START, {})
    ]
    offset = 0
    for p in players:
        offset += rng.randint(*PICK_GAP_MS)
        timeline.append(
            (offset, EventType.CHAMPION_PICK,
             {"player_id": p.player_id, "champion_id": p.champion_id, "team": p.team})
        )
    picks_end = offset

    duration_s = rng.randint(cfg.match_duration_s_min, cfg.match_duration_s_max)
    end_offset = picks_end + duration_s * 1000

    body: list[tuple[int, EventType, dict[str, object]]] = []
    blue = [p for p in players if p.team == TEAMS[0]]
    red = [p for p in players if p.team == TEAMS[1]]
    for _ in range(rng.randint(cfg.kills_per_match_min, cfg.kills_per_match_max)):
        killer_team, victim_team = (blue, red) if rng.random() < 0.5 else (red, blue)
        killer = rng.choice(killer_team)
        victim = rng.choice(victim_team)
        body.append(
            (rng.randint(picks_end + 1, end_offset - 1), EventType.KILL,
             {"player_id": killer.player_id, "champion_id": killer.champion_id,
              "victim_id": victim.player_id})
        )
    for p in players:
        for _ in range(rng.randint(cfg.items_per_player_min, cfg.items_per_player_max)):
            item_id, gold = rng.choice(ITEMS)
            body.append(
                (rng.randint(picks_end + 1, end_offset - 1), EventType.ITEM_PURCHASE,
                 {"player_id": p.player_id, "champion_id": p.champion_id,
                  "item_id": item_id, "gold": gold})
            )
    # Stable sort by time: ties keep generation order, so the result is deterministic.
    body.sort(key=lambda item: item[0])
    timeline.extend(body)
    timeline.append(
        (end_offset, EventType.MATCH_END,
         {"winning_team": rng.choice(TEAMS), "duration_s": duration_s})
    )

    return [
        GameEvent(
            event_id=make_event_id(match_id, seq),
            event_type=kind,
            event_time=match_start + timedelta(milliseconds=offset_ms),
            match_id=match_id,
            match_seq=seq,
            patch_version=cfg.patch_version,
            region=region,
            **payload,  # type: ignore[arg-type]
        )
        for seq, (offset_ms, kind, payload) in enumerate(timeline, start=1)
    ]


def generate_matches(cfg: SimulatorConfig, start_time: datetime) -> Iterator[list[GameEvent]]:
    """Yield each match's events (in match_seq order), one match at a time."""
    rng = random.Random(cfg.seed)
    for i in range(cfg.num_matches):
        jitter_ms = rng.randint(*FIRST_MATCH_JITTER_MS)
        match_start = start_time + timedelta(
            seconds=i * cfg.match_interval_s, milliseconds=jitter_ms
        )
        yield _simulate_match(rng, cfg, match_start)


def generate_events(cfg: SimulatorConfig, start_time: datetime) -> list[GameEvent]:
    """Generate all events for a run, in the order they would be sent.

    Matches overlap in time, so their events are interleaved by event_time.
    Ties are broken by (match_id, match_seq), which keeps each match's
    events in match_seq order.

    Args:
        cfg: Simulator configuration (seed, match counts, ...).
        start_time: Timezone-aware time of the first match start.

    Returns:
        Events sorted by (event_time, match_id, match_seq).
    """
    if start_time.tzinfo is None:
        raise ValueError("start_time must be timezone-aware")
    events = [e for match in generate_matches(cfg, start_time) for e in match]
    events.sort(key=lambda e: (e.event_time, e.match_id, e.match_seq))
    return events
