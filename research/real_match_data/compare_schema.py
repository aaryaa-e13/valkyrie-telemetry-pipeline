"""Compare real match data (Riot Games Match-V5) with the simulator's events: counts only.

Reads the raw files written by ``fetch_matches.py`` (gitignored ``realdata/raw``) and a
simulator dry run (JSON lines), and writes ``findings.json`` and ``FINDINGS.md`` (generated
from the JSON). Standard library only and self-contained, so it runs with ``python -I``.

What may appear in the output: counts, field paths, event type names, and value
distributions of a fixed allowlist of schema fields (queue, patch, platform, ...). Never:
match ids, game ids, player ids (PUUIDs), summoner ids, names, tags, or event payloads.

Two checks run before anything is written:
- **Reconciliation:** totals must add up (events per type = events read = events in frames,
  files parsed + failed = files found, ...). Any mismatch -> exit 1.
- **Privacy:** every identifier read from the raw files (match ids, game ids, PUUIDs,
  summoner ids, names, name#tag) is searched for in the output text. A self-test first
  plants one real identifier of each kind into a copy and must catch it. Any hit, or a
  self-test miss -> exit 1. Identifier values are never printed.

Usage (from the repo root):
    .venv/bin/python -I research/real_match_data/compare_schema.py \\
        --raw realdata/raw --sim realdata/sim_events.jsonl

Exit codes: 0 written, 1 reconciliation or privacy check failed (nothing written),
2 setup problem (missing input).
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

EXIT_OK = 0
EXIT_CHECK_FAILED = 1
EXIT_SETUP = 2

OUT_DIR_DEFAULT = Path(__file__).resolve().parent
FINDINGS_SCHEMA = 1

SAMPLE_STATEMENT = (
    "Recent ranked solo games (queue 420) of top-ladder players on one server (the "
    "challenger ladder of one platform), so the same players appear in several matches. "
    "Small n. Not representative of all games."
)
DISCLAIMER = (
    "VALKYRIE isn't endorsed by Riot Games and doesn't reflect the views or opinions of Riot "
    "Games or anyone officially involved in producing or managing Riot Games properties. Riot "
    "Games, and all associated properties are trademarks or registered trademarks of Riot "
    "Games, Inc."
)

# Real timeline type -> simulator event type (None: no simulator equivalent).
SIM_TYPE_FOR: dict[str, str] = {
    "CHAMPION_KILL": "kill",
    "ITEM_PURCHASED": "item_purchase",
    "GAME_END": "match_end",
}
# Simulator types that come from the match summary, not from a timeline event.
SIM_TYPES_FROM_SUMMARY: dict[str, str] = {
    "match_start": "info.gameStartTimestamp (no timeline event; PAUSE_END at t=0 is the "
                   "closest)",
    "champion_pick": "info.participants[].championId + teamId (no time per pick; the draft "
                     "is not in the data)",
}

# Only these fields may have their *values* shown (schema enums, no identifiers, no names).
VALUE_ALLOWLIST_INFO = ("queueId", "gameMode", "gameType", "mapId", "platformId",
                        "endOfGameResult")
# Participant fields that carry identifiers or names (checked by privacy, never printed).
ID_FIELDS_PARTICIPANT = ("puuid", "summonerId")
NAME_FIELD = "riotIdGameName"
TAG_FIELD = "riotIdTagline"
OTHER_NAME_FIELDS = ("summonerName",)

# Actor / target fields per timeline event, used for natural-key collision counts.
ACTOR_FIELDS = ("participantId", "killerId", "creatorId")
TARGET_FIELDS = ("victimId", "itemId", "skillSlot", "level", "wardType", "beforeId",
                 "buildingType", "monsterType", "laneType")

PRIVACY_MIN_SUBSTRING = 5  # shorter identifiers are matched as whole words only
REMAKE_MAX_S = 300  # a match this short is treated as a remake


# --- small helpers -----------------------------------------------------------------------


def json_type(value: Any) -> str:
    """JSON type name of a parsed value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    return "dict"


def stats(values: Iterable[float]) -> dict[str, Any]:
    """min / median / max / n of a list of numbers (None when empty)."""
    vals = sorted(values)
    if not vals:
        return {"n": 0, "min": None, "median": None, "max": None}
    med = statistics.median(vals)
    return {"n": len(vals), "min": _num(vals[0]), "median": _num(med), "max": _num(vals[-1])}


def _num(x: float) -> float | int:
    return int(x) if float(x).is_integer() else round(float(x), 3)


def pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 1) if whole else 0.0


def dist(values: Iterable[Any]) -> dict[str, int]:
    """Value distribution with string keys, most common first."""
    return {str(k): n for k, n in Counter(values).most_common()}


def patch_of(version: Any) -> str:
    """'16.23.725.1234' -> '16.23'; anything else -> 'unparsed'."""
    if isinstance(version, str):
        parts = version.split(".")
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            return f"{parts[0]}.{parts[1]}"
    return "unparsed"


# --- loading -----------------------------------------------------------------------------


@dataclass
class Raw:
    """Parsed raw files. Keys of ``matches``/``timelines`` are file stems."""

    matches: dict[str, Any] = field(default_factory=dict)
    timelines: dict[str, Any] = field(default_factory=dict)
    match_files: int = 0
    timeline_files: int = 0
    match_parse_failed: int = 0
    timeline_parse_failed: int = 0
    ladder: Any = None
    ids_file: Any = None
    fetch_log: Any = None


def _read_dir(folder: Path) -> tuple[dict[str, Any], int, int]:
    parsed: dict[str, Any] = {}
    files = failed = 0
    for path in sorted(folder.glob("*.json")):
        files += 1
        try:
            parsed[path.stem] = json.loads(path.read_bytes())
        except ValueError:
            failed += 1
    return parsed, files, failed


def _read_optional(path: Path) -> Any:
    try:
        return json.loads(path.read_bytes()) if path.is_file() else None
    except ValueError:
        return None


def load_raw(raw: Path) -> Raw:
    """Load every raw file (no network)."""
    out = Raw()
    out.matches, out.match_files, out.match_parse_failed = _read_dir(raw / "matches")
    out.timelines, out.timeline_files, out.timeline_parse_failed = _read_dir(raw / "timelines")
    out.ladder = _read_optional(raw / "ladder.json")
    out.ids_file = _read_optional(raw / "ids.json")
    out.fetch_log = _read_optional(raw / "fetch_log.json")
    return out


@dataclass
class Sim:
    events: list[dict[str, Any]] = field(default_factory=list)
    lines: int = 0
    parse_failed: int = 0


def load_sim(path: Path) -> Sim:
    """Load the simulator dry run (one JSON event per line)."""
    out = Sim()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        out.lines += 1
        try:
            event = json.loads(line)
        except ValueError:
            out.parse_failed += 1
            continue
        if isinstance(event, dict):
            out.events.append(event)
        else:
            out.parse_failed += 1
    return out


# --- accessors ---------------------------------------------------------------------------


def info(doc: Any) -> dict[str, Any]:
    value = doc.get("info") if isinstance(doc, dict) else None
    return value if isinstance(value, dict) else {}


def metadata(doc: Any) -> dict[str, Any]:
    value = doc.get("metadata") if isinstance(doc, dict) else None
    return value if isinstance(value, dict) else {}


def participants(match: Any) -> list[dict[str, Any]]:
    return [p for p in info(match).get("participants", []) or [] if isinstance(p, dict)]


def frames(timeline: Any) -> list[dict[str, Any]]:
    return [f for f in info(timeline).get("frames", []) or [] if isinstance(f, dict)]


def events_of(timeline: Any) -> list[tuple[int, int, dict[str, Any]]]:
    """(frame index, event index, event) in the order the timeline lists them."""
    out = []
    for fi, frame in enumerate(frames(timeline)):
        for ei, event in enumerate(frame.get("events", []) or []):
            if isinstance(event, dict):
                out.append((fi, ei, event))
    return out


def pair_stems(raw: Raw) -> list[str]:
    return sorted(set(raw.matches) & set(raw.timelines))


# --- identifiers (privacy) ---------------------------------------------------------------


def collect_identifiers(raw: Raw) -> dict[str, set[str]]:
    """Every identifier or name in the raw files, by kind. Values stay in memory only."""
    ids: dict[str, set[str]] = defaultdict(set)

    def add(kind: str, value: Any) -> None:
        if isinstance(value, bool) or value is None:
            return
        text = str(value).strip()
        if text:
            ids[kind].add(text)

    for stem in set(raw.matches) | set(raw.timelines):
        add("match_id", stem)
    for doc in [*raw.matches.values(), *raw.timelines.values()]:
        meta, inf = metadata(doc), info(doc)
        add("match_id", meta.get("matchId"))
        add("game_id", inf.get("gameId"))
        add("game_name", inf.get("gameName"))
        add("tournament_code", inf.get("tournamentCode"))
        for puuid in meta.get("participants", []) or []:
            add("puuid", puuid)
        for p in inf.get("participants", []) or []:
            if not isinstance(p, dict):
                continue
            for key in ID_FIELDS_PARTICIPANT:
                add(key.lower() if key != "puuid" else "puuid", p.get(key))
            for key in (NAME_FIELD, *OTHER_NAME_FIELDS):
                add("player_name", p.get(key))
            name, tag = p.get(NAME_FIELD), p.get(TAG_FIELD)
            if isinstance(name, str) and isinstance(tag, str) and name and tag:
                add("name_tag", f"{name}#{tag}")
    for timeline in raw.timelines.values():
        for _, _, event in events_of(timeline):
            if event.get("type") == "GAME_END":
                add("game_id", event.get("gameId"))
    ladder = raw.ladder if isinstance(raw.ladder, dict) else {}
    add("league_id", ladder.get("leagueId"))
    for entry in ladder.get("entries", []) or []:
        if isinstance(entry, dict):
            add("puuid", entry.get("puuid"))
            add("summonerid", entry.get("summonerId"))
    ids_file = raw.ids_file if isinstance(raw.ids_file, dict) else {}
    for key in ("seed_ids", "snowball_ids"):
        for match_id in ids_file.get(key, []) or []:
            add("match_id", match_id)
    return {k: v for k, v in ids.items() if v}


def _word_re(value: str) -> re.Pattern[str]:
    return re.compile(r"(?<!\w)" + re.escape(value) + r"(?!\w)")


def privacy_hits(texts: Mapping[str, str], idents: Mapping[str, set[str]]) -> dict[str, int]:
    """Count identifiers found in the texts, by kind. Values are never returned."""
    hits: Counter[str] = Counter()
    for kind, values in idents.items():
        for value in values:
            if len(value) < 2:
                continue
            for text in texts.values():
                if len(value) >= PRIVACY_MIN_SUBSTRING:
                    found = value in text
                else:
                    found = _word_re(value).search(text) is not None
                if found:
                    hits[kind] += 1
                    break
    return dict(hits)


def privacy_self_test(texts: Mapping[str, str], idents: Mapping[str, set[str]]) -> list[str]:
    """Plant one real identifier of each kind into a copy; return the kinds NOT caught."""
    missed = []
    for kind, values in sorted(idents.items()):
        usable = sorted((v for v in values if len(v) >= 2), key=lambda v: (-len(v), v))
        if not usable:
            continue
        planted = {name: text + f"\nplanted: {usable[0]}\n" for name, text in texts.items()}
        if not privacy_hits(planted, {kind: {usable[0]}}):
            missed.append(kind)
    return missed


# --- real-data analysis ------------------------------------------------------------------


def analyze_sample(raw: Raw) -> dict[str, Any]:
    stems = pair_stems(raw)
    ms = [raw.matches[s] for s in stems]
    per_match_players: list[set[str]] = []
    for m in ms:
        per_match_players.append({p["puuid"] for p in participants(m)
                                  if isinstance(p.get("puuid"), str) and p["puuid"]})
    appearances: Counter[str] = Counter(p for players in per_match_players for p in players)
    sharing = sum(1 for players in per_match_players
                  if any(appearances[p] > 1 for p in players))
    pairs_sharing = 0
    for i in range(len(per_match_players)):
        for j in range(i + 1, len(per_match_players)):
            if per_match_players[i] & per_match_players[j]:
                pairs_sharing += 1
    log = raw.fetch_log if isinstance(raw.fetch_log, dict) else {}
    return {
        "statement": SAMPLE_STATEMENT,
        "matches": len(ms),
        "player_slots": sum(len(participants(m)) for m in ms),
        "distinct_players": len(appearances),
        "players_by_matches_played": {str(k): n for k, n in
                                      sorted(Counter(appearances.values()).items())},
        "matches_sharing_a_player_with_another": sharing,
        "match_pairs_sharing_a_player": pairs_sharing,
        "match_pairs_total": len(ms) * (len(ms) - 1) // 2,
        "ladder_players_used": log.get("ladder_players_used"),
        "ladder_entries": log.get("ladder_entries"),
        "queue": dist(info(m).get("queueId") for m in ms),
        "patch": dist(patch_of(info(m).get("gameVersion")) for m in ms),
        "platform": dist(info(m).get("platformId") for m in ms),
        "game_mode": dist(info(m).get("gameMode") for m in ms),
        "game_type": dist(info(m).get("gameType") for m in ms),
        "map": dist(info(m).get("mapId") for m in ms),
        "end_of_game_result": dist(info(m).get("endOfGameResult") for m in ms),
        "data_version": dist(metadata(m).get("dataVersion") for m in ms),
        "participants_per_match": dist(len(participants(m)) for m in ms),
        "duration_s": stats(info(m).get("gameDuration") for m in ms
                            if isinstance(info(m).get("gameDuration"), int)),
    }


def analyze_files(raw: Raw) -> dict[str, Any]:
    stems = pair_stems(raw)
    id_matches_stem = sum(1 for s in stems
                          if metadata(raw.matches[s]).get("matchId") == s
                          and metadata(raw.timelines[s]).get("matchId") == s)
    same_players = 0
    for s in stems:
        by_id_match = {p.get("participantId"): p.get("puuid") for p in participants(raw.matches[s])}
        by_id_tl = {p.get("participantId"): p.get("puuid")
                    for p in info(raw.timelines[s]).get("participants", []) or []
                    if isinstance(p, dict)}
        if by_id_match and by_id_match == by_id_tl:
            same_players += 1
    return {
        "match_files": raw.match_files, "timeline_files": raw.timeline_files,
        "match_parse_failed": raw.match_parse_failed,
        "timeline_parse_failed": raw.timeline_parse_failed,
        "pairs": len(stems),
        "matches_without_timeline": len(set(raw.matches) - set(raw.timelines)),
        "timelines_without_match": len(set(raw.timelines) - set(raw.matches)),
        "pairs_whose_ids_match_the_file_name": id_matches_stem,
        "pairs_with_same_participant_map_in_match_and_timeline": same_players,
    }


def analyze_events(raw: Raw) -> dict[str, Any]:
    by_type: Counter[str] = Counter()
    matches_with: dict[str, set[str]] = defaultdict(set)
    per_match_total: list[int] = []
    in_frames = 0
    field_present: dict[str, Counter[str]] = defaultdict(Counter)
    field_null: dict[str, Counter[str]] = defaultdict(Counter)
    field_types: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for s in pair_stems(raw):
        tl = raw.timelines[s]
        in_frames += sum(len(f.get("events", []) or []) for f in frames(tl))
        evs = events_of(tl)
        per_match_total.append(len(evs))
        for _, _, e in evs:
            t = str(e.get("type"))
            by_type[t] += 1
            matches_with[t].add(s)
            for key, value in e.items():
                field_present[t][key] += 1
                field_types[t][key].add(json_type(value))
                if value is None:
                    field_null[t][key] += 1
    types = []
    for t, n in by_type.most_common():
        fields = []
        for key in sorted(field_present[t]):
            present = field_present[t][key]
            fields.append({"field": key, "present": present, "missing": n - present,
                           "null": field_null[t][key],
                           "types": sorted(field_types[t][key])})
        types.append({"type": t, "count": n, "matches": len(matches_with[t]),
                      "simulator_type": SIM_TYPE_FOR.get(t), "fields": fields})
    return {
        "total": sum(by_type.values()),
        "total_in_frames": in_frames,
        "distinct_types": len(by_type),
        "per_match": stats(per_match_total),
        "types": types,
    }


def _key_presence(docs: list[dict[str, Any]]) -> dict[str, Any]:
    present: Counter[str] = Counter()
    nulls: Counter[str] = Counter()
    empty_str: Counter[str] = Counter()
    for d in docs:
        for k, v in d.items():
            present[k] += 1
            if v is None:
                nulls[k] += 1
            if v == "":
                empty_str[k] += 1
    n = len(docs)
    return {
        "documents": n,
        "distinct_keys": len(present),
        "keys_in_every_document": sum(1 for c in present.values() if c == n),
        "keys_missing_in_some": {k: n - c for k, c in sorted(present.items()) if c < n},
        "null_values": dict(sorted(nulls.items())),
        "empty_string_values": dict(sorted(empty_str.items())),
    }


def analyze_summary_fields(raw: Raw) -> dict[str, Any]:
    stems = pair_stems(raw)
    return {
        "match_info": _key_presence([info(raw.matches[s]) for s in stems]),
        "participants": _key_presence([p for s in stems for p in participants(raw.matches[s])]),
        "teams": _key_presence([t for s in stems for t in info(raw.matches[s]).get("teams", [])
                                if isinstance(t, dict)]),
    }


def _game_end(tl: Any) -> dict[str, Any] | None:
    for _, _, e in events_of(tl):
        if e.get("type") == "GAME_END":
            return e
    return None


def analyze_timestamps(raw: Raw) -> dict[str, Any]:
    out_of_order = ties = negative = after_end = later_than_frame = before_prev_frame = 0
    non_int = 0
    frame_gaps_off = frame_count = first_frame_zero = pause_end_at_zero = 0
    total = 0
    intervals: list[Any] = []
    unit_diff_s: list[float] = []
    end_event_vs_duration_s: list[float] = []
    real_ts_vs_end_ms: list[float] = []
    lobby_s: list[float] = []
    for s in pair_stems(raw):
        m, tl = raw.matches[s], raw.timelines[s]
        mi, ti = info(m), info(tl)
        interval = ti.get("frameInterval")
        intervals.append(interval)
        fr = frames(tl)
        frame_count += len(fr)
        if fr and fr[0].get("timestamp") == 0:
            first_frame_zero += 1
        for a, b in zip(fr, fr[1:-1], strict=False):
            ta, tb = a.get("timestamp"), b.get("timestamp")
            if (isinstance(ta, int) and isinstance(tb, int) and isinstance(interval, int)
                    and abs((tb - ta) - interval) > 1000):
                frame_gaps_off += 1
        end = _game_end(tl)
        end_ts = end.get("timestamp") if end else None
        prev = None
        for fi, _, e in events_of(tl):
            total += 1
            ts = e.get("timestamp")
            if not isinstance(ts, int) or isinstance(ts, bool):
                non_int += 1
                continue
            if ts < 0:
                negative += 1
            if prev is not None:
                if ts < prev:
                    out_of_order += 1
                elif ts == prev:
                    ties += 1
            prev = ts
            if isinstance(end_ts, int) and ts > end_ts:
                after_end += 1
            ft = fr[fi].get("timestamp")
            if isinstance(ft, int) and ts > ft:
                later_than_frame += 1
            if fi > 0:
                pt = fr[fi - 1].get("timestamp")
                if isinstance(pt, int) and ts < pt:
                    before_prev_frame += 1
            if e.get("type") == "PAUSE_END" and ts == 0:
                pause_end_at_zero += 1
        start, stop, dur = (mi.get("gameStartTimestamp"), mi.get("gameEndTimestamp"),
                            mi.get("gameDuration"))
        if all(isinstance(x, int) for x in (start, stop, dur)):
            unit_diff_s.append((stop - start) / 1000 - dur)
            if isinstance(end_ts, int):
                end_event_vs_duration_s.append(end_ts / 1000 - dur)
        if end and isinstance(end.get("realTimestamp"), int) and isinstance(stop, int):
            real_ts_vs_end_ms.append(end["realTimestamp"] - stop)
        created = mi.get("gameCreation")
        if isinstance(created, int) and isinstance(start, int):
            lobby_s.append((start - created) / 1000)
    return {
        "events": total,
        "timestamp_not_int": non_int,
        "out_of_order_in_listed_order": out_of_order,
        "ties_with_previous_event": ties,
        "negative": negative,
        "after_game_end_event": after_end,
        "later_than_own_frame_timestamp": later_than_frame,
        "earlier_than_previous_frame_timestamp": before_prev_frame,
        "pause_end_at_zero": pause_end_at_zero,
        "frames": frame_count,
        "frame_interval_ms": dist(intervals),
        "first_frame_at_zero": first_frame_zero,
        "frame_gaps_off_by_more_than_1s": frame_gaps_off,
        "duration_vs_end_minus_start_s": stats(unit_diff_s),
        "game_end_event_vs_duration_s": stats(end_event_vs_duration_s),
        "game_end_real_ts_vs_game_end_timestamp_ms": stats(real_ts_vs_end_ms),
        "start_minus_creation_s": stats(lobby_s),
    }


def _ids_in(e: dict[str, Any], key: str) -> Any:
    v = e.get(key)
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def analyze_odd_values(raw: Raw) -> dict[str, Any]:
    c: Counter[str] = Counter()
    for s in pair_stems(raw):
        m, tl = raw.matches[s], raw.timelines[s]
        mi = info(m)
        teams = [t for t in mi.get("teams", []) or [] if isinstance(t, dict)]
        winners = [t.get("teamId") for t in teams if t.get("win") is True]
        if len(winners) == 1:
            c["matches_with_exactly_one_winner"] += 1
        end = _game_end(tl)
        if end and winners and end.get("winningTeam") == winners[0]:
            c["game_end_winner_agrees_with_teams"] += 1
        for t in teams:
            c[f"team_id_{t.get('teamId')}"] += 1
        if isinstance(mi.get("gameDuration"), int) and mi["gameDuration"] < REMAKE_MAX_S:
            c["remakes_under_300_s"] += 1
        ps = participants(m)
        if ps and ps[0].get("gameEndedInSurrender") is True:
            c["ended_in_surrender"] += 1
        if ps and ps[0].get("gameEndedInEarlySurrender") is True:
            c["ended_in_early_surrender"] += 1
        for _, _, e in events_of(tl):
            t = e.get("type")
            pid = _ids_in(e, "participantId")
            if pid is not None and not 1 <= pid <= 10:
                c["participant_id_outside_1_10"] += 1
                c[f"participant_id_{pid}_on_{t}"] += 1
            if t == "CHAMPION_KILL":
                killer, victim = _ids_in(e, "killerId"), _ids_in(e, "victimId")
                c["kills"] += 1
                if killer == 0:
                    c["kills_by_non_champion_killer_0"] += 1
                if killer is not None and killer == victim:
                    c["kills_killer_equals_victim"] += 1
                if victim is None or not 1 <= victim <= 10:
                    c["kills_victim_outside_1_10"] += 1
                if "assistingParticipantIds" not in e:
                    c["kills_without_assist_field"] += 1
            elif t == "ITEM_PURCHASED":
                c["purchases"] += 1
                if _ids_in(e, "itemId") == 0:
                    c["purchases_item_id_0"] += 1
            elif t == "ITEM_UNDO":
                c["undos"] += 1
                if _ids_in(e, "afterId") == 0:
                    c["undos_of_a_purchase_after_id_0"] += 1
                if _ids_in(e, "beforeId") == 0:
                    c["undos_of_a_sale_before_id_0"] += 1
                gain = e.get("goldGain")
                if isinstance(gain, int):
                    c["undos_gold_gain_" + ("positive" if gain > 0 else
                                            "negative" if gain < 0 else "zero")] += 1
            elif t == "BUILDING_KILL" and _ids_in(e, "killerId") == 0:
                c["buildings_killer_0"] += 1
            elif t == "WARD_PLACED":
                c[f"wards_type_{e.get('wardType')}"] += 1
                if _ids_in(e, "creatorId") == 0:
                    c["wards_creator_0"] += 1
            elif t == "WARD_KILL" and _ids_in(e, "killerId") == 0:
                c["ward_kills_killer_0"] += 1
            elif t == "ELITE_MONSTER_KILL" and _ids_in(e, "killerId") == 0:
                c["monster_kills_killer_0"] += 1
    return dict(sorted(c.items()))


def _natural_key(e: dict[str, Any], with_target: bool) -> tuple[Any, ...]:
    actor = next((e.get(k) for k in ACTOR_FIELDS if k in e), None)
    key: tuple[Any, ...] = (e.get("type"), e.get("timestamp"), actor)
    if with_target:
        key += (next((e.get(k) for k in TARGET_FIELDS if k in e), None),)
    return key


def analyze_duplicates(raw: Raw) -> dict[str, Any]:
    identical = matches_with_identical = 0
    k1 = k2 = 0
    by_type: Counter[str] = Counter()
    matches_by_type: Counter[str] = Counter()
    group_sizes: Counter[int] = Counter()
    ward_types: Counter[str] = Counter()
    for s in pair_stems(raw):
        evs = [e for _, _, e in events_of(raw.timelines[s])]
        canon = Counter(json.dumps(e, sort_keys=True) for e in evs)
        extra = sum(n - 1 for n in canon.values() if n > 1)
        identical += extra
        matches_with_identical += 1 if extra else 0
        types_here: set[str] = set()
        for text, n in canon.items():
            if n > 1:
                e = json.loads(text)
                by_type[str(e.get("type"))] += n - 1
                types_here.add(str(e.get("type")))
                group_sizes[n] += 1
                if e.get("type") == "WARD_PLACED":
                    ward_types[str(e.get("wardType"))] += n - 1
        matches_by_type.update(types_here)
        keys1 = Counter(_natural_key(e, False) for e in evs)
        keys2 = Counter(_natural_key(e, True) for e in evs)
        k1 += sum(n - 1 for n in keys1.values() if n > 1)
        k2 += sum(n - 1 for n in keys2.values() if n > 1)
    return {
        "identical_events_beyond_first": identical,
        "matches_with_identical_events": matches_with_identical,
        "identical_beyond_first_by_type": dict(by_type.most_common()),
        "matches_with_identical_by_type": dict(matches_by_type.most_common()),
        "identical_group_sizes": {str(k): n for k, n in sorted(group_sizes.items())},
        "identical_ward_placed_by_ward_type": dict(ward_types.most_common()),
        "collisions_type_timestamp_actor": k1,
        "collisions_type_timestamp_actor_target": k2,
        "note": "a collision count is the number of events whose key was already used in "
                "the same match; identical events are included in both",
    }


# --- simulator ---------------------------------------------------------------------------


ISO_MS_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def analyze_sim(sim: Sim) -> dict[str, Any]:
    by_type: Counter[str] = Counter(str(e.get("event_type")) for e in sim.events)
    per_match: dict[str, Counter[str]] = defaultdict(Counter)
    durations: list[int] = []
    present: dict[str, Counter[str]] = defaultdict(Counter)
    nulls: dict[str, Counter[str]] = defaultdict(Counter)
    for e in sim.events:
        t = str(e.get("event_type"))
        per_match[str(e.get("match_id"))][t] += 1
        for k, v in e.items():
            present[t][k] += 1
            if v is None:
                nulls[t][k] += 1
        if t == "match_end" and isinstance(e.get("duration_s"), int):
            durations.append(e["duration_s"])
    types = []
    for t, n in by_type.most_common():
        types.append({"type": t, "count": n,
                      "fields_non_null": sorted(k for k in present[t]
                                                if present[t][k] - nulls[t][k] == n),
                      "fields_always_null": sorted(k for k in present[t] if nulls[t][k] == n),
                      "fields_missing": sum(n - c for c in present[t].values())})
    return {
        "lines": sim.lines, "parse_failed": sim.parse_failed, "events": len(sim.events),
        "matches": len(per_match), "types": types,
        "event_time_iso_ms_z": sum(1 for e in sim.events
                                   if isinstance(e.get("event_time"), str)
                                   and ISO_MS_Z.match(e["event_time"])),
        "patch": dist(e.get("patch_version") for e in sim.events),
        "region": dist(e.get("region") for e in sim.events),
        "duration_s": stats(durations),
        "_per_match": per_match,
    }


def compare_volumes(raw: Raw, sim_part: dict[str, Any]) -> list[dict[str, Any]]:
    stems = pair_stems(raw)
    real_counts = [Counter(str(e.get("type")) for _, _, e in events_of(raw.timelines[s]))
                   for s in stems]
    per_match: dict[str, Counter[str]] = sim_part["_per_match"]
    rows = []
    for label, real_type, sim_type in (("kills per match", "CHAMPION_KILL", "kill"),
                                       ("item purchases per match", "ITEM_PURCHASED",
                                        "item_purchase")):
        rows.append({"metric": label,
                     "real": stats(c[real_type] for c in real_counts),
                     "simulator": stats(c[sim_type] for c in per_match.values())})
    rows.append({"metric": "events per match (all types)",
                 "real": stats(sum(c.values()) for c in real_counts),
                 "simulator": stats(sum(c.values()) for c in per_match.values())})
    rows.append({"metric": "match duration (s)",
                 "real": stats(info(raw.matches[s]).get("gameDuration") for s in stems
                               if isinstance(info(raw.matches[s]).get("gameDuration"), int)),
                 "simulator": sim_part["duration_s"]})
    rows.append({"metric": "players per match",
                 "real": stats(len(participants(raw.matches[s])) for s in stems),
                 "simulator": stats(c["champion_pick"] for c in per_match.values())})
    return rows


# --- mapping -----------------------------------------------------------------------------


def _coverage(events: dict[str, Any], real_type: str, fld: str) -> dict[str, int] | None:
    for t in events["types"]:
        if t["type"] == real_type:
            row = next((f for f in t["fields"] if f["field"] == fld), None)
            ok = row["present"] - row["null"] if row else 0
            return {"events": t["count"], "with_value": ok}
    return None


def build_mapping(events: dict[str, Any]) -> list[dict[str, Any]]:
    """Simulator field -> real source, how it maps, and coverage where it is an event field."""
    rows = [
        ("event_id", "all", "none", "derive", None,
         "no event id in the data; must be derived (match id + position, or a content hash)"),
        ("event_type", "all", "event.type", "rename", None,
         "upper-case names; only 3 of the real types have a simulator type"),
        ("schema_version", "all", "metadata.dataVersion", "different meaning", None,
         "API format version, not our event schema version"),
        ("event_time", "all", "info.gameStartTimestamp + event.timestamp", "derive", None,
         "epoch ms + ms since game start, vs our ISO-8601 UTC string"),
        ("match_id", "all", "metadata.matchId", "direct", None,
         "string with a platform prefix, vs our UUID"),
        ("match_seq", "all", "none (frame index, event index)", "derive", None,
         "list position; timestamps tie, so position is needed"),
        ("patch_version", "all", "info.gameVersion", "derive", None,
         "4-part version; cut to major.minor"),
        ("region", "all", "info.platformId", "rename", None, "different code set"),
        ("player_id", "kill", "CHAMPION_KILL.killerId", "join", ("CHAMPION_KILL", "killerId"),
         "participant 1-10 -> PUUID via the participant list; 0 = not a champion"),
        ("victim_id", "kill", "CHAMPION_KILL.victimId", "join", ("CHAMPION_KILL", "victimId"),
         "participant 1-10 -> PUUID"),
        ("player_id", "item_purchase", "ITEM_PURCHASED.participantId", "join",
         ("ITEM_PURCHASED", "participantId"), "participant 1-10 -> PUUID"),
        ("item_id", "item_purchase", "ITEM_PURCHASED.itemId", "direct",
         ("ITEM_PURCHASED", "itemId"), "integer, vs our string id"),
        ("gold", "item_purchase", "none on the event", "absent", None,
         "needs a static item price table; ITEM_UNDO carries goldGain"),
        ("champion_id", "kill, item_purchase, champion_pick", "info.participants[].championId",
         "join", None, "integer + name, per participant, not per event"),
        ("team", "champion_pick", "info.participants[].teamId", "rename", None,
         "100 / 200, vs blue / red"),
        ("winning_team", "match_end", "GAME_END.winningTeam", "rename",
         ("GAME_END", "winningTeam"), "100 / 200; also info.teams[].win"),
        ("duration_s", "match_end", "info.gameDuration", "direct", None,
         "seconds (see timestamps)"),
    ]
    out = []
    for sim_field, sim_types, source, how, cov, note in rows:
        out.append({"simulator_field": sim_field, "simulator_types": sim_types,
                    "real_source": source, "how": how,
                    "coverage": _coverage(events, *cov) if cov else None, "note": note})
    return out


def reverse(events: dict[str, Any]) -> dict[str, Any]:
    real_only = [{"type": t["type"], "count": t["count"]} for t in events["types"]
                 if t["simulator_type"] is None]
    return {
        "simulator_types_without_a_timeline_event": SIM_TYPES_FROM_SUMMARY,
        "simulator_fields_without_a_real_source": ["event_id", "match_seq", "gold",
                                                   "schema_version (different meaning)"],
        "real_types_without_a_simulator_type": real_only,
        "real_events_without_a_simulator_type": sum(r["count"] for r in real_only),
    }


# --- Phase 3 inputs ----------------------------------------------------------------------


def phase3_inputs(f: dict[str, Any]) -> list[dict[str, str]]:
    ev, ts, odd, dup = f["events"], f["timestamps"], f["odd_values"], f["duplicates"]
    missing = sum(fl["missing"] for t in ev["types"] for fl in t["fields"])
    nulls = sum(fl["null"] for t in ev["types"] for fl in t["fields"])
    part = f["summary_fields"]["participants"]
    real_only = f["reverse"]["real_events_without_a_simulator_type"]
    kills, k0 = odd.get("kills", 0), odd.get("kills_by_non_champion_killer_0", 0)
    return [
        {"input": "Missing is not null",
         "evidence": f"timeline events: {missing} absent fields, {nulls} explicit nulls; the "
                     "simulator always sends every field, with null when unused",
         "gate": "required-field checks must treat an absent key and null the same way"},
        {"input": "Empty strings in identifier-like fields",
         "evidence": "participant fields with empty strings: "
                     + (", ".join(f"{k} ({n})" for k, n in part["empty_string_values"].items())
                        or "none"),
         "gate": "treat '' as missing for ids and names"},
        {"input": "Unknown event types",
         "evidence": f"{ev['distinct_types']} real event types, {len(SIM_TYPE_FOR)} map to a "
                     f"simulator type; {real_only} of {ev['total']} events "
                     f"({pct(real_only, ev['total'])}%) have none",
         "gate": "an allowlist of event types; unknown types go to the dead-letter queue, "
                 "counted, not dropped"},
        {"input": "Ties in event time",
         "evidence": f"{ts['ties_with_previous_event']} of {ts['events']} events "
                     f"({pct(ts['ties_with_previous_event'], ts['events'])}%) share the "
                     "timestamp of the event before them",
         "gate": "ordering and match_seq cannot come from time alone; keep the list position"},
        {"input": "Order and range of event times",
         "evidence": f"out of order in listed order: {ts['out_of_order_in_listed_order']}; "
                     f"negative: {ts['negative']}; after GAME_END: "
                     f"{ts['after_game_end_event']}; later than own frame: "
                     f"{ts['later_than_own_frame_timestamp']}",
         "gate": "event_time within [game start, game end]; counts above say how strict "
                 "this can be"},
        {"input": "Relative timestamps",
         "evidence": "event timestamps are ms since game start; absolute time needs "
                     "info.gameStartTimestamp from the match file",
         "gate": "a timeline without its match summary cannot get an event_time: hold or "
                 "dead-letter it"},
        {"input": "Participant 0 on player events",
         "evidence": "participantId outside 1-10: " + (", ".join(
             f"{k.removeprefix('participant_id_')} ({n})" for k, n in odd.items()
             if k.startswith("participant_id_") and k != "participant_id_outside_1_10")
             or "none"),
         "gate": "a purchase or skill event with participant 0 cannot be joined to a "
                 "player: flag it (quarantine or keep with a null player), do not drop it"},
        {"input": "Non-champion actors",
         "evidence": f"{k0} of {kills} kills ({pct(k0, kills)}%) have killerId 0",
         "gate": "killer 0 is valid (not a player); a player join must allow it"},
        {"input": "Corrections in the stream",
         "evidence": f"ITEM_UNDO: {odd.get('undos', 0)} vs {odd.get('purchases', 0)} purchases",
         "gate": "a purchase is not final; downstream counts need undo handling"},
        {"input": "Natural keys collide",
         "evidence": f"identical events beyond the first: {dup['identical_events_beyond_first']}"
                     f" (by type: {_dist(dup['identical_beyond_first_by_type'])})"
                     f"; (type, time, actor) collisions: "
                     f"{dup['collisions_type_timestamp_actor']}; with target: "
                     f"{dup['collisions_type_timestamp_actor_target']}",
         "gate": "event_id = match id + frame index + event index (stable position), not a "
                 "content hash"},
        {"input": "More than one patch",
         "evidence": "patches in the sample: " + ", ".join(
             f"{k} ({n})" for k, n in f["sample"]["patch"].items()),
         "gate": "patch comes from each match (gameVersion cut to major.minor); the "
                 "SCD2 patch dimension must expect several in one day"},
    ]


# --- reconciliation ----------------------------------------------------------------------


def reconcile(raw: Raw, sim: Sim, f: dict[str, Any]) -> list[str]:
    """Return the failed checks (empty list = everything adds up)."""
    failed = []
    ev = f["events"]
    if sum(t["count"] for t in ev["types"]) != ev["total"]:
        failed.append("events per type != events read")
    if ev["total"] != ev["total_in_frames"]:
        failed.append("events read != events listed in frames")
    if f["timestamps"]["events"] != ev["total"]:
        failed.append("timestamp check saw a different number of events")
    for t in ev["types"]:
        for fl in t["fields"]:
            if fl["present"] + fl["missing"] != t["count"]:
                failed.append(f"present + missing != count for {t['type']}.{fl['field']}")
    if raw.match_files != len(raw.matches) + raw.match_parse_failed:
        failed.append("match files != parsed + failed")
    if raw.timeline_files != len(raw.timelines) + raw.timeline_parse_failed:
        failed.append("timeline files != parsed + failed")
    files = f["files"]
    if files["pairs"] + files["matches_without_timeline"] != len(raw.matches):
        failed.append("pairs + matches without timeline != matches parsed")
    sample = f["sample"]
    if sum(int(k) * n for k, n in sample["players_by_matches_played"].items()) != \
            sample["player_slots"]:
        failed.append("player appearances != player slots")
    s = f["simulator"]
    if s["events"] + s["parse_failed"] != s["lines"]:
        failed.append("simulator events + failed != lines")
    if sum(t["count"] for t in s["types"]) != s["events"]:
        failed.append("simulator events per type != events")
    return failed


# --- output ------------------------------------------------------------------------------


def build_findings(raw: Raw, sim: Sim) -> dict[str, Any]:
    events = analyze_events(raw)
    sim_part = analyze_sim(sim)
    f: dict[str, Any] = {
        "findings_schema": FINDINGS_SCHEMA,
        "generated_on": datetime.now(UTC).date().isoformat(),
        "disclaimer": DISCLAIMER,
        "sample": analyze_sample(raw),
        "files": analyze_files(raw),
        "events": events,
        "summary_fields": analyze_summary_fields(raw),
        "timestamps": analyze_timestamps(raw),
        "odd_values": analyze_odd_values(raw),
        "duplicates": analyze_duplicates(raw),
        "mapping": build_mapping(events),
        "reverse": reverse(events),
        "volumes": compare_volumes(raw, sim_part),
    }
    sim_part.pop("_per_match")
    f["simulator"] = sim_part
    f["phase3_inputs"] = phase3_inputs(f)
    return f


def _s(st: dict[str, Any]) -> str:
    if not st or not st.get("n"):
        return "n/a"
    return f"{st['min']} / {st['median']} / {st['max']} (n={st['n']})"


def _dist(d: Mapping[str, int]) -> str:
    return ", ".join(f"{k}: {n}" for k, n in d.items()) or "none"


def render_markdown(f: dict[str, Any]) -> str:
    """FINDINGS.md, generated from the findings dict only."""
    sm, fi, ev, ts = f["sample"], f["files"], f["events"], f["timestamps"]
    odd, dup, sf, sim = f["odd_values"], f["duplicates"], f["summary_fields"], f["simulator"]
    lines = [
        "# Real match data vs the simulator: findings",
        "",
        f"> {f['disclaimer']}",
        "",
        "Generated by `compare_schema.py` from `findings.json` (counts and field paths only; no "
        "match ids, player ids, names or event payloads). Raw data stays in the gitignored "
        f"`realdata/`. Generated on {f['generated_on']}.",
        "",
        "## Sample",
        "",
        f"**{sm['statement']}**",
        "",
        "| Item | Value |",
        "|---|---|",
        f"| Matches (match + timeline pairs) | {sm['matches']} |",
        f"| Ladder players used as seeds | {sm['ladder_players_used']} of "
        f"{sm['ladder_entries']} ladder entries |",
        f"| Player slots | {sm['player_slots']} |",
        f"| Distinct players | {sm['distinct_players']} |",
        f"| Players by number of matches played | {_dist(sm['players_by_matches_played'])} |",
        f"| Matches sharing a player with another match | "
        f"{sm['matches_sharing_a_player_with_another']} of {sm['matches']} |",
        f"| Match pairs sharing a player | {sm['match_pairs_sharing_a_player']} of "
        f"{sm['match_pairs_total']} |",
        f"| Queue | {_dist(sm['queue'])} |",
        f"| Patch (major.minor) | {_dist(sm['patch'])} |",
        f"| Platform | {_dist(sm['platform'])} |",
        f"| Mode / type / map | {_dist(sm['game_mode'])} / {_dist(sm['game_type'])} / "
        f"{_dist(sm['map'])} |",
        f"| End of game result | {_dist(sm['end_of_game_result'])} |",
        f"| Data version | {_dist(sm['data_version'])} |",
        f"| Participants per match | {_dist(sm['participants_per_match'])} |",
        f"| Duration s (min / median / max) | {_s(sm['duration_s'])} |",
        "",
        "## Files",
        "",
        f"- Match files {fi['match_files']}, timeline files {fi['timeline_files']}, parse "
        f"failures {fi['match_parse_failed']} + {fi['timeline_parse_failed']}, pairs "
        f"{fi['pairs']}.",
        f"- Pairs whose ids match their file name: "
        f"{fi['pairs_whose_ids_match_the_file_name']}.",
        f"- Pairs whose participant map (participantId -> player) is the same in match and "
        f"timeline: {fi['pairs_with_same_participant_map_in_match_and_timeline']}.",
        "",
        "## Event types",
        "",
        f"{ev['total']} timeline events, {ev['distinct_types']} types, events per match "
        f"{_s(ev['per_match'])}.",
        "",
        "| Real type | Events | Matches | Simulator type |",
        "|---|---|---|---|",
    ]
    for t in ev["types"]:
        lines.append(f"| `{t['type']}` | {t['count']} | {t['matches']} | "
                     f"{t['simulator_type'] or '(none)'} |")
    rv = f["reverse"]
    lines += [
        "",
        "**The reverse direction:**",
        "",
    ]
    for t, src in rv["simulator_types_without_a_timeline_event"].items():
        lines.append(f"- Simulator `{t}` has no timeline event: {src}.")
    lines += [
        "- Simulator fields with no real source: "
        + ", ".join(f"`{x}`" for x in rv["simulator_fields_without_a_real_source"]) + ".",
        f"- Real events with no simulator type: {rv['real_events_without_a_simulator_type']} "
        f"of {ev['total']} ({pct(rv['real_events_without_a_simulator_type'], ev['total'])}%).",
        "",
        "## Field mapping (simulator -> real)",
        "",
        "| Simulator field | Types | Real source | How | Coverage | Note |",
        "|---|---|---|---|---|---|",
    ]
    for r in f["mapping"]:
        cov = r["coverage"]
        cov_s = f"{cov['with_value']} / {cov['events']}" if cov else ""
        lines.append(f"| `{r['simulator_field']}` | {r['simulator_types']} | "
                     f"`{r['real_source']}` | {r['how']} | {cov_s} | {r['note']} |")
    lines += [
        "",
        "## Missing, null and empty values",
        "",
        "Timeline event fields that are absent in some events of their type (no field is "
        "ever null unless listed):",
        "",
        "| Type | Field | Missing | Null | JSON types |",
        "|---|---|---|---|---|",
    ]
    for t in ev["types"]:
        for fl in t["fields"]:
            if fl["missing"] or fl["null"]:
                lines.append(f"| `{t['type']}` | `{fl['field']}` | {fl['missing']} of "
                             f"{t['count']} | {fl['null']} | {', '.join(fl['types'])} |")
    for label, part in (("Match info", sf["match_info"]), ("Participants", sf["participants"]),
                        ("Teams", sf["teams"])):
        lines += [
            "",
            f"**{label}:** {part['documents']} documents, {part['distinct_keys']} keys, "
            f"{part['keys_in_every_document']} in every document.",
            f"- Keys missing in some: {_dist(part['keys_missing_in_some'])}.",
            f"- Null values: {_dist(part['null_values'])}.",
            f"- Empty strings: {_dist(part['empty_string_values'])}.",
        ]
    lines += [
        "",
        "## Timestamps",
        "",
        "- Encoding: match summary times are epoch milliseconds (`gameCreation`, "
        "`gameStartTimestamp`, `gameEndTimestamp`); `gameDuration` is checked below; timeline "
        "`timestamp` values are milliseconds since game start; `GAME_END` and `PAUSE_END` "
        "also carry `realTimestamp`. The simulator sends an ISO-8601 UTC string with "
        f"milliseconds ({sim['event_time_iso_ms_z']} of {sim['events']} events match "
        "`YYYY-MM-DDTHH:MM:SS.mmmZ`).",
        f"- `gameDuration` vs (end - start) / 1000, in s: {_s(ts['duration_vs_end_minus_start_s'])}"
        " (near 0 means `gameDuration` is in seconds).",
        f"- `GAME_END.timestamp` / 1000 vs `gameDuration`, s: "
        f"{_s(ts['game_end_event_vs_duration_s'])}.",
        f"- `GAME_END.realTimestamp` vs `gameEndTimestamp`, ms: "
        f"{_s(ts['game_end_real_ts_vs_game_end_timestamp_ms'])}.",
        f"- Start minus creation (lobby/loading), s: {_s(ts['start_minus_creation_s'])}.",
        f"- Frames: {ts['frames']}, interval ms {_dist(ts['frame_interval_ms'])}, first frame "
        f"at 0 in {ts['first_frame_at_zero']} matches, gaps off by > 1 s: "
        f"{ts['frame_gaps_off_by_more_than_1s']}.",
        "",
        "| Check over all timeline events | Count |",
        "|---|---|",
        f"| Events | {ts['events']} |",
        f"| Timestamp not an integer | {ts['timestamp_not_int']} |",
        f"| Earlier than the event listed before it (out of order) | "
        f"{ts['out_of_order_in_listed_order']} |",
        f"| Same timestamp as the event listed before it (tie) | "
        f"{ts['ties_with_previous_event']} |",
        f"| Negative | {ts['negative']} |",
        f"| After the `GAME_END` event | {ts['after_game_end_event']} |",
        f"| Later than its own frame's timestamp | {ts['later_than_own_frame_timestamp']} |",
        f"| Earlier than the previous frame's timestamp | "
        f"{ts['earlier_than_previous_frame_timestamp']} |",
        f"| `PAUSE_END` at timestamp 0 | {ts['pause_end_at_zero']} |",
        "",
        "## Odd values",
        "",
        "| Check | Count |",
        "|---|---|",
    ]
    lines += [f"| `{k}` | {n} |" for k, n in odd.items()]
    lines += [
        "",
        "## Duplicates and keys",
        "",
        f"- Identical events within a match (beyond the first copy): "
        f"{dup['identical_events_beyond_first']}, in {dup['matches_with_identical_events']} "
        "matches.",
        f"- Identical events by type (beyond the first copy): "
        f"{_dist(dup['identical_beyond_first_by_type'])}; matches affected by type: "
        f"{_dist(dup['matches_with_identical_by_type'])}.",
        f"- Sizes of groups of identical events (size: groups): "
        f"{_dist(dup['identical_group_sizes'])}; identical `WARD_PLACED` by `wardType`: "
        f"{_dist(dup['identical_ward_placed_by_ward_type'])}. Whether these are distinct "
        "real events or an artifact of the data is UNVERIFIED; either way a content-hash "
        "dedupe would remove them.",
        f"- Key (type, timestamp, actor) collisions: {dup['collisions_type_timestamp_actor']}; "
        f"(type, timestamp, actor, target): {dup['collisions_type_timestamp_actor_target']}. "
        f"({dup['note']}.)",
        "",
        "## Simulator vs real volumes",
        "",
        "| Metric | Real (min / median / max) | Simulator (min / median / max) |",
        "|---|---|---|",
    ]
    lines += [f"| {r['metric']} | {_s(r['real'])} | {_s(r['simulator'])} |" for r in f["volumes"]]
    lines += [
        "",
        f"Simulator run: {sim['events']} events in {sim['matches']} matches (seed 42, default "
        f"config, dry run); patch {_dist(sim['patch'])}; region {_dist(sim['region'])}.",
        "",
        "## Inputs for the Phase 3 quality gates",
        "",
        "Inputs only; the Phase 3 plan decides the rules.",
        "",
        "| Input | Evidence (counts) | Candidate gate |",
        "|---|---|---|",
    ]
    lines += [f"| {p['input']} | {p['evidence']} | {p['gate']} |" for p in f["phase3_inputs"]]
    chk = f.get("checks", {})
    lines += [
        "",
        "## Checks",
        "",
        f"- Reconciliation: {chk.get('reconciliation', 'not run')}.",
        f"- Privacy: {chk.get('privacy', 'not run')}.",
        "",
    ]
    return "\n".join(lines)


def dumps(f: dict[str, Any]) -> str:
    return json.dumps(f, indent=1, sort_keys=False, ensure_ascii=False) + "\n"


# --- CLI ---------------------------------------------------------------------------------


def run(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Compare real match data with simulator events.")
    p.add_argument("--raw", default="realdata/raw", help="raw folder from fetch_matches.py")
    p.add_argument("--sim", default="realdata/sim_events.jsonl", help="simulator dry run")
    p.add_argument("--out-dir", default=str(OUT_DIR_DEFAULT), help="where to write findings")
    args = p.parse_args(argv)
    raw_dir, sim_path, out_dir = Path(args.raw), Path(args.sim), Path(args.out_dir)
    if not (raw_dir / "matches").is_dir() or not sim_path.is_file():
        print("setup: --raw must contain matches/ and --sim must exist", file=sys.stderr)
        return EXIT_SETUP
    raw, sim = load_raw(raw_dir), load_sim(sim_path)
    findings = build_findings(raw, sim)

    failed = reconcile(raw, sim, findings)
    if failed:
        for item in failed:
            print(f"RECONCILIATION FAILED: {item}", file=sys.stderr)
        return EXIT_CHECK_FAILED
    idents = collect_identifiers(raw)
    findings["checks"] = {
        "reconciliation": "PASS (all totals add up)",
        "privacy": "PASS: 0 of " + str(sum(len(v) for v in idents.values()))
                   + " identifiers found (" + ", ".join(
                       f"{k} {len(v)}" for k, v in sorted(idents.items()))
                   + "); tags are checked together with the name (name#tag); "
                   "names under 5 characters are matched as whole words; "
                   "self-test planted one of each kind and caught it",
    }
    texts = {"findings.json": dumps(findings), "FINDINGS.md": render_markdown(findings)}
    missed = privacy_self_test(texts, idents)
    if missed:
        print(f"PRIVACY SELF-TEST FAILED: planted ids not caught for kinds {missed}",
              file=sys.stderr)
        return EXIT_CHECK_FAILED
    hits = privacy_hits(texts, idents)
    if hits:
        print(f"PRIVACY CHECK FAILED: identifiers found in the output, by kind: {hits} "
              "(values not shown); nothing written", file=sys.stderr)
        return EXIT_CHECK_FAILED
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, text in texts.items():
        (out_dir / name).write_text(text, encoding="utf-8")
    print("SUMMARY " + json.dumps({
        "result": "WRITTEN", "matches": findings["sample"]["matches"],
        "events": findings["events"]["total"], "types": findings["events"]["distinct_types"],
        "simulator_events": findings["simulator"]["events"],
        "identifiers_checked": sum(len(v) for v in idents.values()), "privacy_hits": 0,
        "files": sorted(texts)}))
    return EXIT_OK


def main() -> int:
    return run(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
