"""Unit tests for the rule DATA in streaming/gates.py (no Spark).

The Spark expressions are tested in tests/spark/test_gates.py. Here: the fixed rule order,
the rules versions, and that the per-type required fields agree with what the simulator
really emits, so good data can never be rejected by a wrong field list.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest

from simulator.config import SimulatorConfig
from simulator.events import EventType
from simulator.generator import generate_events
from streaming import gates

START = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)


def test_rule_order_is_fixed() -> None:
    assert gates.HARD_CODES == (
        "not_json", "missing_event_id", "bad_event_time", "event_time_implausible",
        "unknown_event_type", "unsupported_schema_version", "missing_required_field",
        "bad_patch_format", "bad_enum", "out_of_range")
    assert gates.SOFT_CODES == (
        "future_event_time", "self_kill", "player_id_sentinel", "unexpected_field")


def test_rules_versions() -> None:
    assert gates.RULES_VERSIONS == (1, 2)
    assert set(gates.REGIONS) == set(gates.RULES_VERSIONS)
    assert set(gates.REGIONS[2]) - set(gates.REGIONS[1]) == {"SEA"}
    assert set(gates.REGIONS[1]) <= set(gates.REGIONS[2])
    for v in gates.RULES_VERSIONS:
        assert gates.check_version(v) == v
    for bad in (0, 3, True, "1"):
        with pytest.raises(ValueError, match="unknown rules version"):
            gates.check_version(bad)  # type: ignore[arg-type]


def test_types_and_fields_match_the_event_format() -> None:
    assert set(gates.EVENT_TYPES) == {t.value for t in EventType}
    assert set(gates.REQUIRED_BY_TYPE) == set(gates.EVENT_TYPES)
    for fields in gates.REQUIRED_BY_TYPE.values():
        assert set(fields) <= set(gates.TYPE_FIELDS)
    events = generate_events(SimulatorConfig(seed=7, num_matches=1), START)
    keys = set(events[0].to_dict())
    assert keys == {"event_id", "event_time", *gates.ENVELOPE, *gates.TYPE_FIELDS}
    assert keys >= gates.NUMERIC_FIELDS


@pytest.mark.parametrize("seed", [1, 7, 42])
def test_simulator_events_set_exactly_the_required_fields(seed: int) -> None:
    """Every generated event sets its type's required fields and nothing else (so the
    simulator triggers neither H7 nor S4), with values the other rules accept."""
    events = generate_events(SimulatorConfig(seed=seed, num_matches=3), START)
    seen = set()
    for e in events:
        d = e.to_dict()
        t = d["event_type"]
        seen.add(t)
        set_fields = {f for f in gates.TYPE_FIELDS if d[f] is not None}
        assert set_fields == set(gates.REQUIRED_BY_TYPE[t]), (t, set_fields)
        assert all(d[f] not in (None, "") for f in gates.ENVELOPE)
        assert d["schema_version"] in gates.SCHEMA_VERSIONS
        assert re.fullmatch(gates.PATCH_PATTERN, d["patch_version"])
        assert d["region"] in gates.REGIONS[1]
        if d["player_id"] is not None:
            assert re.fullmatch(gates.PLAYER_PATTERN, d["player_id"])
        for f in ("team", "winning_team"):
            assert d[f] is None or d[f] in gates.TEAMS
        assert d["match_seq"] >= 1
        assert d["gold"] is None or d["gold"] > 0
        assert d["duration_s"] is None or d["duration_s"] > 0
        assert t != "kill" or d["victim_id"] != d["player_id"]
        assert e.event_time >= gates.EVENT_TIME_FLOOR
    assert seen == set(gates.EVENT_TYPES)
