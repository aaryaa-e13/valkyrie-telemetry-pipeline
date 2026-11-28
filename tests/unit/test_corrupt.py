"""simulator/corrupt.py: deterministic choice, the corrupted bytes, predictions, coverage."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime

import pytest

from simulator import corrupt
from simulator.config import SimulatorConfig, load_config
from simulator.events import EventType, GameEvent
from simulator.generator import generate_events

START = datetime(2026, 11, 28, 12, 0, 0, tzinfo=UTC)
BAD_CFG = load_config("simulator/config.bad.yaml")


def events(seed: int = 7, cfg: SimulatorConfig = BAD_CFG) -> list[GameEvent]:
    return generate_events(dataclasses.replace(cfg, seed=seed), START)


def test_the_same_seed_gives_the_same_choices_and_bytes() -> None:
    a = corrupt.corrupt_run(events(), 7, 0.2, corrupt.KINDS)
    b = corrupt.corrupt_run(events(), 7, 0.2, corrupt.KINDS)
    assert a == b and len(a) > 0
    assert corrupt.choose(events(), 8, 0.2, corrupt.KINDS) != corrupt.choose(
        events(), 7, 0.2, corrupt.KINDS)


def test_raising_bad_fraction_only_adds_bad_events() -> None:
    evs = events()
    low = corrupt.choose(evs, 7, 0.1, corrupt.KINDS)
    high = corrupt.choose(evs, 7, 0.4, corrupt.KINDS)
    assert set(low) < set(high)
    assert all(high[e] == c for e, c in low.items())     # same kind, field and variant


def test_off_means_no_bad_event() -> None:
    assert corrupt.choose(events(), 7, 0.0, corrupt.KINDS) == {}
    assert corrupt.choose(events(), 7, 0.5, ()) == {}


def test_a_kind_is_used_only_on_types_it_applies_to() -> None:
    evs = events()
    types = {e.event_id: e.event_type.value for e in evs}
    for seed in range(20):
        for eid, c in corrupt.choose(evs, seed, 0.5, corrupt.KINDS).items():
            assert types[eid] in corrupt.APPLIES_TO[c.kind], (c, types[eid])
            options = corrupt.field_options(c.kind, types[eid])
            if c.kind == "wrong_type":
                assert c.variant in options
            elif options:
                assert c.field in options


def test_new_region_takes_exactly_one_whole_match_and_nothing_else() -> None:
    evs = events()
    ch = corrupt.choose(evs, 7, 0.3, corrupt.KINDS)
    region = corrupt.region_match((e.match_id for e in evs), 7)
    in_match = [e.event_id for e in evs if e.match_id == region]
    assert in_match and all(ch[e] == corrupt.Choice("new_region", field="region")
                            for e in in_match)
    assert all(c.kind != "new_region" for e, c in ch.items() if e not in set(in_match))
    without = corrupt.choose(evs, 7, 0.3, [k for k in corrupt.KINDS if k != "new_region"])
    assert not any(c.kind == "new_region" for c in without.values())


def _ev(kind: str = "item_purchase") -> GameEvent:
    per = {
        "item_purchase": dict(player_id="p_000001", champion_id="ch_a", item_id="it_x",
                              gold=100),
        "kill": dict(player_id="p_000001", champion_id="ch_a", victim_id="p_000002"),
        "champion_pick": dict(player_id="p_000001", champion_id="ch_a", team="blue"),
        "match_end": dict(winning_team="red", duration_s=90),
        "match_start": {},
    }[kind]
    return GameEvent(event_id="e-1", event_type=EventType(kind),
                     event_time=datetime(2026, 11, 27, 10, 12, 34, 567000, tzinfo=UTC),
                     match_id="m-1", match_seq=3, patch_version="14.3", region="EUW", **per)


MATCH_START = datetime(2026, 11, 27, 10, 0, 0, tzinfo=UTC)


def _value(choice: corrupt.Choice, kind: str = "item_purchase") -> tuple[bytes, str | None]:
    return corrupt.corrupt_value(_ev(kind), choice, MATCH_START)


@pytest.mark.parametrize("choice,kind,check", [
    (corrupt.Choice("drop_event_id"), "item_purchase", lambda d: "event_id" not in d),
    (corrupt.Choice("empty_event_id"), "item_purchase", lambda d: d["event_id"] == ""),
    (corrupt.Choice("bad_time_format"), "item_purchase",
     lambda d: d["event_time"] == "2026-11-27 10:12:34"),
    (corrupt.Choice("epoch_time"), "item_purchase",
     lambda d: d["event_time"] == "1970-01-01T00:12:34.567Z"),
    (corrupt.Choice("unknown_type"), "item_purchase", lambda d: d["event_type"] == "ward_placed"),
    (corrupt.Choice("schema_v99"), "item_purchase", lambda d: d["schema_version"] == 99),
    (corrupt.Choice("drop_required", field="match_seq"), "item_purchase",
     lambda d: "match_seq" not in d),
    (corrupt.Choice("null_required", field="gold"), "item_purchase", lambda d: d["gold"] is None),
    (corrupt.Choice("empty_required", field="region"), "item_purchase",
     lambda d: d["region"] == ""),
    (corrupt.Choice("wrong_type", variant="envelope", field="match_seq"), "kill",
     lambda d: d["match_seq"] == "abc"),
    (corrupt.Choice("wrong_type", variant="field", field="gold"), "item_purchase",
     lambda d: d["gold"] == "100"),
    (corrupt.Choice("bad_patch"), "item_purchase", lambda d: d["patch_version"] == "14.2.1"),
    (corrupt.Choice("bad_team", field="team"), "champion_pick", lambda d: d["team"] == "green"),
    (corrupt.Choice("bad_team", field="winning_team"), "match_end",
     lambda d: d["winning_team"] == "green"),
    (corrupt.Choice("new_region", field="region"), "kill", lambda d: d["region"] == "SEA"),
    (corrupt.Choice("negative_gold"), "item_purchase", lambda d: d["gold"] == -5),
    (corrupt.Choice("future_time"), "item_purchase",
     lambda d: d["event_time"] == "2026-11-28T10:12:34.567Z"),
    (corrupt.Choice("self_kill"), "kill", lambda d: d["victim_id"] == d["player_id"]),
    (corrupt.Choice("player_zero"), "kill", lambda d: d["player_id"] == "0"),
    (corrupt.Choice("extra_field", field="gold"), "kill", lambda d: d["gold"] == 400),
])
def test_the_corrupted_bytes_look_as_planned(choice: corrupt.Choice, kind: str,
                                             check: object) -> None:
    value, sent = _value(choice, kind)
    d = json.loads(value)
    assert check(d), d                                        # type: ignore[operator]
    good = _ev(kind).to_dict()
    changed = {k for k in set(good) | set(d) if good.get(k, "<absent>") != d.get(k, "<absent>")}
    assert len(changed) == 1, changed                         # one fault touches one field
    assert sent == ("" if choice.kind == "empty_event_id" else
                    None if choice.kind == "drop_event_id" else "e-1")


def test_malformed_json_is_half_a_message_and_unreadable() -> None:
    value, sent = _value(corrupt.Choice("malformed_json"))
    full = _ev().to_json_bytes()
    assert value == full[: len(full) // 2] and sent is None
    with pytest.raises(json.JSONDecodeError):
        json.loads(value)


@pytest.mark.parametrize("choice,etype,expected", [
    (corrupt.Choice("malformed_json"), "kill", ("dlq", "not_json", "offset")),
    (corrupt.Choice("empty_event_id"), "kill", ("dlq", "missing_event_id", "offset")),
    (corrupt.Choice("bad_time_format"), "kill", ("dlq", "bad_event_time", "offset")),
    (corrupt.Choice("epoch_time"), "kill", ("dlq", "event_time_implausible", "id")),
    (corrupt.Choice("drop_required", field="event_type"), "kill",
     ("dlq", "missing_required_field", "offset")),
    (corrupt.Choice("null_required", field="region"), "kill",
     ("dlq", "missing_required_field", "id")),
    (corrupt.Choice("null_required", field="victim_id"), "kill",
     ("dlq", "missing_required_field", "id")),
    (corrupt.Choice("wrong_type", variant="envelope", field="match_seq"), "item_purchase",
     ("dlq", "missing_required_field", "offset")),
    (corrupt.Choice("wrong_type", variant="field", field="gold"), "item_purchase",
     ("dlq", "missing_required_field", "id")),
    (corrupt.Choice("new_region", field="region"), "kill", ("dlq", "bad_enum", "id")),
    (corrupt.Choice("future_time"), "kill", ("flag", "future_event_time", None)),
    (corrupt.Choice("extra_field", field="team"), "kill", ("flag", "unexpected_field", None)),
])
def test_predictions(choice: corrupt.Choice, etype: str, expected: tuple) -> None:
    x = corrupt.expected_for(choice, etype)
    assert (x.cls, x.code, x.key_kind) == expected


def test_field_options_exclusions() -> None:
    assert "event_type" not in corrupt.field_options("empty_required", "kill")
    assert "schema_version" not in corrupt.field_options("empty_required", "kill")
    assert corrupt.field_options("wrong_type", "kill") == ("envelope",)
    assert corrupt.field_options("wrong_type", "item_purchase") == ("envelope", "field")
    assert corrupt.field_options("extra_field", "match_end") == (
        "player_id", "champion_id", "team", "victim_id", "item_id", "gold")


def test_coverage_search_finds_a_seed_or_names_what_is_missing() -> None:
    def make(seed: int) -> list[GameEvent]:
        return events(seed)

    found = corrupt.find_covering_seed(make, 42, 0.2, corrupt.KINDS, max_seeds=1000)
    assert found.seed is not None and found.missing == []
    evs = make(found.seed)
    assert corrupt.missing_units(evs, corrupt.choose(evs, found.seed, 0.2, corrupt.KINDS),
                                 corrupt.KINDS) == []
    none = corrupt.find_covering_seed(make, 42, 0.01, corrupt.KINDS, max_seeds=3)
    assert none.seed is None and none.tried == 3 and none.missing
    assert set(none.missing) <= corrupt.required_units(corrupt.KINDS)


def test_required_units_cover_every_gate_with_every_kind_enabled() -> None:
    units = corrupt.required_units(corrupt.KINDS)
    assert {u for u in units if u.startswith("gate:")} == {
        f"gate:{c}" for c in (*corrupt.HARD_CODES, *corrupt.SOFT_CODES)}
    assert {"wrong_type:envelope", "wrong_type:field", "h7:id", "h7:offset"} <= units
    assert len(corrupt.KINDS) == 19


def test_config_validation() -> None:
    with pytest.raises(ValueError, match="unknown bad-data kinds"):
        SimulatorConfig(bad_fraction=0.1, bad_kinds=("nope",))
    with pytest.raises(ValueError, match="unknown bad-data kinds"):
        SimulatorConfig(bad_kinds=("nope",))                  # checked even when off
    with pytest.raises(ValueError, match=r"bad_fraction must be in \[0, 1\]"):
        SimulatorConfig(bad_fraction=1.5)
    with pytest.raises(ValueError, match="new_region needs a region"):
        SimulatorConfig(bad_fraction=0.1, regions=("EUW", "SEA"))
    assert SimulatorConfig().enabled_bad_kinds() == ()
    assert SimulatorConfig(bad_fraction=0.1).enabled_bad_kinds() == corrupt.KINDS
    assert BAD_CFG.enabled_bad_kinds() == corrupt.KINDS and BAD_CFG.bad_fraction == 0.2
    assert load_config(env={"SIM_BAD_FRACTION": "0.1", "SIM_BAD_KINDS": "self_kill,bad_patch"}
                       ).enabled_bad_kinds() == ("bad_patch", "self_kill")
