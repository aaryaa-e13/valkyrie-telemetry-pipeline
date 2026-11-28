"""streaming/oracle_gates.py: verify's own gates (stdlib) on the shared corpus, the
three-way test part 1 (corrupt.py's prediction == these gates), parser pins, independence.
"""

from __future__ import annotations

import ast
import dataclasses
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from simulator import corrupt
from simulator.events import EventType, GameEvent
from streaming import gates
from streaming import oracle_gates as og
from tests.gates_corpus import CORPUS, raw

NOW = datetime.now(UTC)
TOPIC = "game-events"


def _verdict(message: object, offset: int = 1, version: int = 1,
             ingest: datetime = NOW) -> og.Verdict:
    text = raw(message).decode()  # type: ignore[arg-type]
    return og.classify(text, topic=TOPIC, partition=0, offset=offset, ingest_time=ingest,
                       rules_version=version)


@pytest.mark.parametrize("version", [1, 2])
def test_the_corpus(version: int) -> None:
    for off, msg, keyed, reason, reasons, flags in CORPUS:
        if version == 2 and off == 26:            # v2 accepts region SEA (replay demo)
            reason, reasons = None, []
        v = _verdict(msg, off, version)
        assert (v.keyed, v.reason_code, list(v.reasons), list(v.flags)) == (
            keyed, reason, reasons, flags), (off, v)
        if reason is None:
            assert v.dlq_key is None
        elif keyed:
            assert v.dlq_key == f"id:{v.event_id}" and v.key_kind == "event_id"
        else:
            assert v.dlq_key == f"off:{TOPIC}/0/{off}" and v.key_kind == "offset"


def test_independence_imports_nothing_but_the_standard_library() -> None:
    tree = ast.parse(Path("streaming/oracle_gates.py").read_text())
    mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import)
            for a in n.names}
    mods |= {n.module.split(".")[0] for n in ast.walk(tree)
             if isinstance(n, ast.ImportFrom) and n.module}
    assert mods <= set(sys.stdlib_module_names) | {"__future__"}, mods


def test_the_rule_data_matches_the_job_and_the_simulator() -> None:
    """A drift detector only: the three-way test is what proves the behaviour."""
    for name in ("EVENT_TYPES", "ENVELOPE", "REQUIRED_BY_TYPE", "TYPE_FIELDS", "TEAMS",
                 "REGIONS", "HARD_CODES", "SOFT_CODES", "SCHEMA_VERSIONS"):
        assert getattr(og, name) == getattr(gates, name), name
    assert og.EVENT_TIME_FLOOR == gates.EVENT_TIME_FLOOR
    assert timedelta(seconds=gates.FUTURE_TOLERANCE_S) == og.FUTURE_TOLERANCE
    assert (og.PATCH_RE.pattern, og.PLAYER_RE.pattern) == (gates.PATCH_PATTERN,
                                                           gates.PLAYER_PATTERN)
    for name in ("ENVELOPE", "REQUIRED_BY_TYPE", "TYPE_FIELDS", "HARD_CODES", "SOFT_CODES"):
        assert getattr(corrupt, name) == getattr(og, name), name


# --- three-way test, part 1: corrupt.py's prediction == verify's gates ------------------

def _events() -> list[GameEvent]:
    t = datetime(2026, 11, 27, 10, 0, 30, 250000, tzinfo=UTC)
    per = {
        "match_start": {},
        "champion_pick": dict(player_id="p_000001", champion_id="ch_a", team="blue"),
        "kill": dict(player_id="p_000001", champion_id="ch_a", victim_id="p_000002"),
        "item_purchase": dict(player_id="p_000001", champion_id="ch_a", item_id="it_x",
                              gold=850),
        "match_end": dict(winning_team="red", duration_s=95),
    }
    return [GameEvent(event_id=f"e-{k}", event_type=EventType(k), event_time=t,
                      match_id="m-1", match_seq=i + 2, patch_version="14.3", region="EUW",
                      **kw) for i, (k, kw) in enumerate(per.items())]


def all_cases() -> list[tuple[GameEvent, corrupt.Choice]]:
    """Every kind x every type it applies to x every field / variant it can pick."""
    out = []
    for e in _events():
        t = e.event_type.value
        for kind in corrupt.KINDS:
            if t not in corrupt.APPLIES_TO[kind]:
                continue
            options = corrupt.field_options(kind, t) or (None,)
            for opt in options:
                if kind == "wrong_type":
                    c = corrupt.Choice(kind, variant=opt,
                                       field="match_seq" if opt == "envelope" else "gold")
                elif kind == "new_region":
                    c = corrupt.Choice(kind, field="region")
                else:
                    c = corrupt.Choice(kind, field=opt)
                out.append((e, c))
    return out


MATCH_START = datetime(2026, 11, 27, 10, 0, 0, tzinfo=UTC)
INGEST = datetime(2026, 11, 27, 10, 0, 31, tzinfo=UTC)      # a backfill ingest time


def test_three_way_part1_every_kind_and_variant() -> None:
    cases = all_cases()
    assert {c.kind for _, c in cases} == set(corrupt.KINDS)
    assert {c.variant for _, c in cases if c.kind == "wrong_type"} == {"envelope", "field"}
    for e, c in cases:
        value, sent = corrupt.corrupt_value(e, c, MATCH_START)
        x = corrupt.expected_for(c, e.event_type.value)
        v = og.classify(value.decode(), topic=TOPIC, partition=3, offset=77,
                        ingest_time=INGEST, rules_version=corrupt.RULES_VERSION_EXPECTED)
        label = (e.event_type.value, c)
        if x.cls == "dlq":
            assert v.reason_code == x.code, (label, v)
            assert v.keyed == (x.key_kind == "id"), (label, v)
            assert v.dlq_key == (f"id:{e.event_id}" if x.key_kind == "id"
                                 else f"off:{TOPIC}/3/77"), label
        else:
            assert (v.reason_code, v.flags) == (None, (x.code,)), (label, v)
            assert v.keyed and v.event_id == e.event_id
        if sent is not None and sent != "":
            assert v.event_id == sent, label


def test_good_events_pass_with_no_flag() -> None:
    for e in _events():
        v = og.classify(e.to_json_bytes().decode(), topic=TOPIC, partition=0, offset=1,
                        ingest_time=INGEST, rules_version=1)
        assert (v.keyed, v.reason_code, v.flags) == (True, None, ()), (e.event_type, v)


def test_new_region_is_accepted_under_rules_v2() -> None:
    e = _events()[2]
    value, _ = corrupt.corrupt_value(e, corrupt.Choice("new_region", field="region"),
                                     MATCH_START)
    v = og.classify(value.decode(), topic=TOPIC, partition=0, offset=1, ingest_time=INGEST,
                    rules_version=2)
    assert v.reason_code is None


# --- parser pins (the Spark behaviour this module copies, D3.2 P1) -----------------------

@pytest.mark.parametrize("text,field,want", [
    ('{"match_seq": "7"}', "match_seq", None),                 # no string -> int coercion
    ('{"match_seq": 7.0}', "match_seq", None),
    ('{"match_seq": true}', "match_seq", None),
    ('{"match_seq": 2147483648}', "match_seq", None),          # int32 overflow
    ('{"match_seq": -2147483648}', "match_seq", -2147483648),
    ('{"gold": 2147483648}', "gold", 2147483648),             # gold is int64
    ('{"region": 7}', "region", "7"),       # an int -> its digits (Spark: corpus row 56)
    ('{"region": null}', "region", None),
])
def test_field_conversion(text: str, field: str, want: object) -> None:
    assert og.parse(text).fields[field] == want


@pytest.mark.parametrize("text,want", [
    ("2026-11-27T10:00:00.123Z", datetime(2026, 11, 27, 10, 0, 0, 123000, tzinfo=UTC)),
    ("2026-11-27T10:00:00Z", None),                            # no milliseconds
    ("2026-11-27 10:00:00.123Z", None),
    ("2026-02-30T10:00:00.000Z", None),                        # not a date
    ("2026-11-27T10:00:00.1234Z", None),
    (None, None),
])
def test_event_time_format(text: str | None, want: datetime | None) -> None:
    assert og.parse_time(text) == want


@pytest.mark.parametrize("text", ["", "{bad", "[1, 2]", "7", '"x"', "null"])
def test_not_an_object_is_h1_with_every_field_null(text: str) -> None:
    p = og.parse(text)
    assert not p.is_object and all(v is None for v in p.fields.values())
    assert og.hard_reasons(p, 1)[0] == "not_json"


def test_blank_means_spaces_only_as_spark_trim() -> None:
    base = dataclasses.asdict(_events()[0]) | {"event_type": "match_start",
                                               "event_time": "2026-11-27T10:00:00.000Z"}
    import json
    assert not og.key_ok(og.parse(json.dumps(base | {"event_id": "   "})))
    assert og.key_ok(og.parse(json.dumps(base | {"event_id": "\t"})))   # trim keeps a tab


def test_unknown_rules_version_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown rules version"):
        og.hard_reasons(og.parse("{}"), 3)
