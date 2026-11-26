"""Unit tests for research/real_match_data/compare_schema.py.

Fixtures are small, hand-written and fictional. Field names that carry a studio name are
taken from the module's constants on purpose: this file must not contain any studio or game
name.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.real_match_data import compare_schema as cs

START = 1_700_000_000_000  # epoch ms, fictional


def participant(pid: int, puuid: str, name: str, team: int) -> dict:
    return {"participantId": pid, "puuid": puuid, "summonerId": f"summ-{puuid}",
            cs.NAME_FIELD: name, cs.TAG_FIELD: "TAG9", "summonerName": "",
            "teamId": team, "championId": 900 + pid, "championName": "Zorblaxion",
            "gameEndedInSurrender": False, "gameEndedInEarlySurrender": False}


def make_match(mid: str, puuids: list[str], duration: int = 1500) -> tuple[dict, dict]:
    ps = [participant(i + 1, p, f"Fictional Player {p}", 100 if i < 5 else 200)
          for i, p in enumerate(puuids)]
    match = {"metadata": {"dataVersion": "2", "matchId": mid, "participants": puuids},
             "info": {"gameId": int(mid.split("_")[1]), "gameName": f"lobby-{mid}",
                      "gameCreation": START - 60_000, "gameStartTimestamp": START,
                      "gameEndTimestamp": START + duration * 1000, "gameDuration": duration,
                      "gameVersion": "77.4.123.4567", "queueId": 420, "gameMode": "CLASSIC",
                      "gameType": "MATCHED_GAME", "mapId": 11, "platformId": "XX1",
                      "endOfGameResult": "GameComplete", "tournamentCode": "",
                      "participants": ps,
                      "teams": [{"teamId": 100, "win": True, "bans": [], "objectives": {}},
                                {"teamId": 200, "win": False, "bans": [], "objectives": {}}]}}
    end_ms = duration * 1000
    frames = [
        {"timestamp": 0, "participantFrames": {}, "events": [
            {"type": "PAUSE_END", "timestamp": 0, "realTimestamp": START}]},
        {"timestamp": 60_000, "participantFrames": {}, "events": [
            {"type": "ITEM_PURCHASED", "timestamp": 1000, "participantId": 1, "itemId": 55},
            {"type": "ITEM_PURCHASED", "timestamp": 1000, "participantId": 1, "itemId": 55},
            {"type": "ITEM_PURCHASED", "timestamp": 2000, "participantId": 0, "itemId": 56},
            {"type": "ITEM_UNDO", "timestamp": 3000, "participantId": 2, "beforeId": 0,
             "afterId": 57, "goldGain": -300},
            {"type": "CHAMPION_KILL", "timestamp": 30_000, "killerId": 3, "victimId": 7,
             "assistingParticipantIds": [4], "bounty": 300},
            {"type": "CHAMPION_KILL", "timestamp": 40_000, "killerId": 0, "victimId": 8,
             "bounty": None},
            {"type": "WARD_PLACED", "timestamp": 50_000, "creatorId": 0,
             "wardType": "UNDEFINED"},
        ]},
        {"timestamp": end_ms, "participantFrames": {}, "events": [
            {"type": "GAME_END", "timestamp": end_ms, "realTimestamp": START + end_ms + 250,
             "gameId": int(mid.split("_")[1]), "winningTeam": 100}]},
    ]
    timeline = {"metadata": {"dataVersion": "2", "matchId": mid, "participants": puuids},
                "info": {"frameInterval": 60_000, "frames": frames, "gameId": 0,
                         "participants": [{"participantId": i + 1, "puuid": p}
                                          for i, p in enumerate(puuids)]}}
    return match, timeline


def sim_event(n: int, mid: str, etype: str, **extra) -> dict:
    base = {"event_id": f"e-{mid}-{n}", "event_type": etype, "schema_version": 1,
            "event_time": "2026-01-01T00:00:00.000Z", "match_id": mid, "match_seq": n,
            "patch_version": "14.3", "region": "NA", "player_id": None, "champion_id": None,
            "team": None, "victim_id": None, "item_id": None, "gold": None,
            "winning_team": None, "duration_s": None}
    base.update(extra)
    return base


def write_fixture(root: Path) -> tuple[Path, Path]:
    raw = root / "raw"
    (raw / "matches").mkdir(parents=True)
    (raw / "timelines").mkdir()
    shared = "puuid-shared-0001"
    m1 = [shared] + [f"puuid-a-{i:04d}" for i in range(9)]
    m2 = [shared] + [f"puuid-b-{i:04d}" for i in range(9)]
    for mid, players in (("XX1_1001", m1), ("XX1_1002", m2)):
        match, timeline = make_match(mid, players)
        (raw / "matches" / f"{mid}.json").write_text(json.dumps(match))
        (raw / "timelines" / f"{mid}.json").write_text(json.dumps(timeline))
    (raw / "ladder.json").write_text(json.dumps(
        {"tier": "CHALLENGER", "queue": "Q", "entries": [{"puuid": shared, "wins": 1}]}))
    (raw / "ids.json").write_text(json.dumps({"seed_ids": ["XX1_1001", "XX1_1002"],
                                              "snowball_ids": []}))
    (raw / "fetch_log.json").write_text(json.dumps({"ladder_players_used": 1,
                                                    "ladder_entries": 1}))
    sim = root / "sim.jsonl"
    events = []
    for mid in ("s1", "s2"):
        events += [sim_event(1, mid, "match_start"),
                   sim_event(2, mid, "champion_pick", player_id="p_1", team="blue"),
                   sim_event(3, mid, "kill", player_id="p_1", victim_id="p_2"),
                   sim_event(4, mid, "item_purchase", item_id="it_x", gold=300),
                   sim_event(5, mid, "match_end", winning_team="blue", duration_s=1400)]
    sim.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return raw, sim


@pytest.fixture()
def fixture_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    raw, sim = write_fixture(tmp_path)
    return raw, sim, tmp_path / "out"


def run_cs(raw: Path, sim: Path, out: Path) -> int:
    return cs.run(["--raw", str(raw), "--sim", str(sim), "--out-dir", str(out)])


def findings_of(raw: Path, sim: Path) -> dict:
    return cs.build_findings(cs.load_raw(raw), cs.load_sim(sim))


# --- end to end --------------------------------------------------------------------------


def test_run_writes_both_files_and_md_has_sample_and_disclaimer(fixture_paths) -> None:
    raw, sim, out = fixture_paths
    assert run_cs(raw, sim, out) == cs.EXIT_OK
    md = (out / "FINDINGS.md").read_text()
    data = json.loads((out / "findings.json").read_text())
    assert cs.SAMPLE_STATEMENT in md and cs.DISCLAIMER in md
    assert data["sample"]["statement"] == cs.SAMPLE_STATEMENT
    assert data["checks"]["reconciliation"].startswith("PASS")
    assert data["checks"]["privacy"].startswith("PASS")


def test_output_holds_no_identifiers_names_or_disallowed_values(fixture_paths) -> None:
    raw, sim, out = fixture_paths
    run_cs(raw, sim, out)
    text = (out / "FINDINGS.md").read_text() + (out / "findings.json").read_text()
    for needle in ("puuid-", "summ-", "Fictional Player", "XX1_100", "lobby-", "TAG9",
                   "Zorblaxion", "1700000000000"):
        assert needle not in text, needle


def test_missing_input_is_a_setup_error(tmp_path: Path) -> None:
    assert cs.run(["--raw", str(tmp_path / "nope"), "--sim", str(tmp_path / "x"),
                   "--out-dir", str(tmp_path / "out")]) == cs.EXIT_SETUP
    assert not (tmp_path / "out").exists()


# --- counts ------------------------------------------------------------------------------


def test_sample_counts_distinct_players_and_sharing(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    s = findings_of(raw, sim)["sample"]
    assert s["matches"] == 2 and s["player_slots"] == 20
    assert s["distinct_players"] == 19
    assert s["players_by_matches_played"] == {"1": 18, "2": 1}
    assert s["matches_sharing_a_player_with_another"] == 2
    assert s["match_pairs_sharing_a_player"] == 1 and s["match_pairs_total"] == 1
    assert s["patch"] == {"77.4": 2} and s["queue"] == {"420": 2}


def test_event_counts_fields_missing_and_null(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    ev = findings_of(raw, sim)["events"]
    assert ev["total"] == ev["total_in_frames"] == 2 * 9
    kill = next(t for t in ev["types"] if t["type"] == "CHAMPION_KILL")
    assert kill["count"] == 4 and kill["simulator_type"] == "kill"
    fields = {f["field"]: f for f in kill["fields"]}
    assert fields["assistingParticipantIds"]["missing"] == 2
    assert fields["bounty"]["null"] == 2 and fields["bounty"]["missing"] == 0
    assert next(t for t in ev["types"] if t["type"] == "WARD_PLACED")["simulator_type"] is None


def test_timestamps_ties_order_and_units(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    ts = findings_of(raw, sim)["timestamps"]
    assert ts["ties_with_previous_event"] == 2  # the duplicate purchase, once per match
    assert ts["out_of_order_in_listed_order"] == 0 and ts["negative"] == 0
    assert ts["pause_end_at_zero"] == 2 and ts["after_game_end_event"] == 0
    assert ts["duration_vs_end_minus_start_s"]["max"] == 0
    assert ts["game_end_real_ts_vs_game_end_timestamp_ms"]["median"] == 250


def test_out_of_order_events_are_counted(tmp_path: Path) -> None:
    raw, sim = write_fixture(tmp_path)
    path = raw / "timelines" / "XX1_1001.json"
    tl = json.loads(path.read_text())
    tl["info"]["frames"][1]["events"].append({"type": "LEVEL_UP", "timestamp": 10,
                                               "participantId": 1, "level": 2})
    path.write_text(json.dumps(tl))
    assert findings_of(raw, sim)["timestamps"]["out_of_order_in_listed_order"] == 1


def test_odd_values(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    odd = findings_of(raw, sim)["odd_values"]
    assert odd["kills_by_non_champion_killer_0"] == 2
    assert odd["participant_id_0_on_ITEM_PURCHASED"] == 2
    assert odd["undos_of_a_sale_before_id_0"] == 2 and odd["undos_gold_gain_negative"] == 2
    assert odd["wards_creator_0"] == 2 and odd["matches_with_exactly_one_winner"] == 2
    assert odd["game_end_winner_agrees_with_teams"] == 2


def test_identical_events_are_counted_by_type(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    dup = findings_of(raw, sim)["duplicates"]
    assert dup["identical_events_beyond_first"] == 2
    assert dup["identical_beyond_first_by_type"] == {"ITEM_PURCHASED": 2}
    assert dup["identical_group_sizes"] == {"2": 2}


def test_simulator_side_and_volumes(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    f = findings_of(raw, sim)
    assert f["simulator"]["events"] == 10 and f["simulator"]["event_time_iso_ms_z"] == 10
    kills = next(r for r in f["volumes"] if r["metric"] == "kills per match")
    assert kills["real"]["median"] == 2 and kills["simulator"]["median"] == 1
    assert "_per_match" not in f["simulator"]


def test_phase3_inputs_cite_counts(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    inputs = {p["input"]: p for p in findings_of(raw, sim)["phase3_inputs"]}
    assert "2 of 4 kills" in inputs["Non-champion actors"]["evidence"]
    assert "0_on_ITEM_PURCHASED (2)" in inputs["Participant 0 on player events"]["evidence"]


def test_parse_failures_are_counted_and_still_reconcile(fixture_paths) -> None:
    raw, sim, out = fixture_paths
    (raw / "matches" / "XX1_9999.json").write_text("{not json")
    loaded = cs.load_raw(raw)
    assert loaded.match_parse_failed == 1 and loaded.match_files == 3
    assert run_cs(raw, sim, out) == cs.EXIT_OK


def test_patch_of() -> None:
    assert cs.patch_of("16.23.725.1") == "16.23"
    for bad in (None, "", "x.y", 16, "16"):
        assert cs.patch_of(bad) == "unparsed"


# --- reconciliation ----------------------------------------------------------------------


def test_reconciliation_failure_exits_1_and_writes_nothing(fixture_paths, monkeypatch,
                                                           capsys) -> None:
    raw, sim, out = fixture_paths
    real = cs.analyze_events

    def broken(r):
        ev = real(r)
        ev["types"][0]["count"] += 1  # per-type counts no longer add up
        return ev

    monkeypatch.setattr(cs, "analyze_events", broken)
    assert run_cs(raw, sim, out) == cs.EXIT_CHECK_FAILED
    assert not out.exists()
    assert "RECONCILIATION FAILED" in capsys.readouterr().err


def test_reconcile_passes_on_the_fixture(fixture_paths) -> None:
    raw, sim, _ = fixture_paths
    r, s = cs.load_raw(raw), cs.load_sim(sim)
    assert cs.reconcile(r, s, cs.build_findings(r, s)) == []


# --- privacy -----------------------------------------------------------------------------


def test_a_planted_id_is_caught_exit_1_nothing_written_value_not_printed(
        fixture_paths, monkeypatch, capsys) -> None:
    raw, sim, out = fixture_paths
    real = cs.render_markdown
    monkeypatch.setattr(cs, "render_markdown", lambda f: real(f) + "\npuuid-a-0003\n")
    assert run_cs(raw, sim, out) == cs.EXIT_CHECK_FAILED
    assert not out.exists()
    err = capsys.readouterr().err
    assert "PRIVACY CHECK FAILED" in err and "puuid" in err and "puuid-a-0003" not in err


@pytest.mark.parametrize("planted", ["XX1_1002", "1002", "summ-puuid-b-0001",
                                     "Fictional Player puuid-a-0001",
                                     "Fictional Player puuid-a-0001#TAG9", "lobby-XX1_1001"])
def test_every_identifier_kind_is_caught(fixture_paths, planted: str) -> None:
    raw, _, _ = fixture_paths
    idents = cs.collect_identifiers(cs.load_raw(raw))
    assert cs.privacy_hits({"t": f"text {planted} text"}, idents)


def test_collected_kinds(fixture_paths) -> None:
    raw, _, _ = fixture_paths
    idents = cs.collect_identifiers(cs.load_raw(raw))
    assert {"match_id", "game_id", "game_name", "puuid", "summonerid", "player_name",
            "name_tag"} <= set(idents)
    assert "puuid-shared-0001" in idents["puuid"]  # also from the ladder


def test_short_names_match_whole_words_only() -> None:
    idents = {"player_name": {"Abe"}}
    assert cs.privacy_hits({"t": "a player Abe here"}, idents) == {"player_name": 1}
    assert cs.privacy_hits({"t": "Abegail and Babel"}, idents) == {}
    assert cs.privacy_hits({"t": "Abegail"}, {"player_name": {"Abegail"}})


def test_self_test_catches_each_kind() -> None:
    idents = {"puuid": {"puuid-long-value"}, "player_name": {"Bo"}}
    assert cs.privacy_self_test({"t": "nothing here"}, idents) == []


def test_open_questions_are_in_both_outputs(fixture_paths) -> None:
    raw, sim, out = fixture_paths
    run_cs(raw, sim, out)
    data = json.loads((out / "findings.json").read_text())
    md = (out / "FINDINGS.md").read_text()
    assert [q["question"] for q in data["open_questions"]] == [
        q["question"] for q in cs.OPEN_QUESTIONS]
    assert "## Open questions" in md
    assert "stable across two fetches" in md and "timeline step 7" in md
