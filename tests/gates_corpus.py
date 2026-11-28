"""The shared gate corpus (plan section 2; DECISIONS D3.7): plain data, no Spark.

One list of (offset, message, keyed, reason_code, reasons, flags). Two independent
implementations must give exactly these rows:
  * the job's gates (Spark): tests/spark/test_dlq.py;
  * verify's own gates (stdlib, R11/R14): tests/unit/test_oracle_gates.py.
Moved here from test_dlq.py in Phase 3 slice 2 (rows 45+ added for the slice 2 kinds).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from simulator import corrupt
from simulator.events import EventType, GameEvent

T0 = datetime(2026, 11, 19, 12, 0, 0, tzinfo=UTC)


# --- event builders -------------------------------------------------------------------

def ev(event_id: str | None = "e", when: datetime = T0, kind: str = "item_purchase",
       **change: Any) -> dict[str, Any]:
    """A valid simulator event as a dict; `change` overrides fields (DROP removes one)."""
    base = dict(event_id="x" if event_id is None else event_id, event_time=when,
                match_id="m-1", match_seq=1, patch_version="14.3", region="EUW")
    per = {
        "item_purchase": dict(event_type=EventType.ITEM_PURCHASE, player_id="p_000001",
                              champion_id="ch_quillon", item_id="it_stormglass", gold=100),
        "kill": dict(event_type=EventType.KILL, player_id="p_000001", champion_id="ch_quillon",
                     victim_id="p_000002"),
        "champion_pick": dict(event_type=EventType.CHAMPION_PICK, player_id="p_000001",
                              champion_id="ch_quillon", team="blue"),
        "match_end": dict(event_type=EventType.MATCH_END, winning_team="red", duration_s=1500),
        "match_start": dict(event_type=EventType.MATCH_START),
    }[kind]
    d = GameEvent(**base, **per).to_dict()
    if event_id is None:
        d.pop("event_id")
    for k, v in change.items():
        if v is DROP:
            d.pop(k, None)
        else:
            d[k] = v
    return d


DROP = object()


def raw(d: dict[str, Any] | str) -> bytes:
    return (d if isinstance(d, str) else json.dumps(d)).encode("utf-8")


def half(d: dict[str, Any]) -> str:
    """The first half of a message's JSON text (bad kind malformed_json)."""
    text = json.dumps(d)
    return text[: len(text) // 2]


# --- the corpus --------------------------------------------------------------------------

FUTURE = datetime.now(UTC) + timedelta(days=2)
# (offset, message, keyed, reason_code, reasons, flags)
CORPUS: list[tuple[int, Any, bool, str | None, list[str], list[str]]] = [
    (1, ev("good"), True, None, [], []),
    (2, ev("g-start", kind="match_start"), True, None, [], []),
    (3, ev("g-end", kind="match_end"), True, None, [], []),
    (4, ev("g-pick", kind="champion_pick"), True, None, [], []),
    (5, ev("g-kill", kind="kill"), True, None, [], []),
    (10, "{not json", False, "not_json", ["not_json", "missing_event_id", "bad_event_time",
                                         "missing_required_field"], []),
    (11, "[1, 2]", False, "not_json", ["not_json", "missing_event_id", "bad_event_time",
                                      "missing_required_field"], []),
    (12, ev(None), False, "missing_event_id", ["missing_event_id"], []),
    (13, ev(""), False, "missing_event_id", ["missing_event_id"], []),
    (14, ev("   "), False, "missing_event_id", ["missing_event_id"], []),
    (15, ev("h3", event_time="yesterday"), False, "bad_event_time", ["bad_event_time"], []),
    (16, ev("h4", event_time="1970-01-01T00:00:01.000Z"), True, "event_time_implausible",
     ["event_time_implausible"], []),
    (17, ev("h5", event_type="teleport"), True, "unknown_event_type",
     ["unknown_event_type"], []),
    (18, ev("h6", schema_version=99), True, "unsupported_schema_version",
     ["unsupported_schema_version"], []),
    (19, ev("h7a", match_id=DROP), False, "missing_required_field",
     ["missing_required_field"], []),           # envelope field absent: parse_ok false
    (20, ev("h7b", player_id=None), True, "missing_required_field",
     ["missing_required_field"], []),
    (21, ev("h7c", champion_id=""), True, "missing_required_field",
     ["missing_required_field"], []),
    (22, ev("h7d", region=DROP), True, "missing_required_field",
     ["missing_required_field"], []),           # not a parse_ok field: still keyed
    (23, ev("h7e", schema_version="abc"), False, "missing_required_field",
     ["missing_required_field"], []),           # wrong type nulls only that field (P1)
    (24, ev("h7f", event_type=None), False, "missing_required_field",
     ["missing_required_field"], []),           # NULL type is H7, never H5
    (25, ev("h8", patch_version="14"), True, "bad_patch_format", ["bad_patch_format"], []),
    (26, ev("h9a", region="SEA"), True, "bad_enum", ["bad_enum"], []),
    (27, ev("h9b", kind="champion_pick", team="green"), True, "bad_enum", ["bad_enum"], []),
    (28, ev("h9c", kind="match_end", winning_team="none"), True, "bad_enum", ["bad_enum"], []),
    (29, ev("h10a", gold=-5), True, "out_of_range", ["out_of_range"], []),
    (30, ev("h10b", gold=0), True, "out_of_range", ["out_of_range"], []),
    (31, ev("h10c", match_seq=0), True, "out_of_range", ["out_of_range"], []),
    (32, ev("multi", event_time="1970-01-01T00:00:01.000Z", region="XX"), True,
     "event_time_implausible", ["event_time_implausible", "bad_enum"], []),
    (40, ev("s1", when=FUTURE), True, None, [], ["future_event_time"]),
    (41, ev("s2", kind="kill", victim_id="p_000001"), True, None, [], ["self_kill"]),
    (42, ev("s3", player_id="0"), True, None, [], ["player_id_sentinel"]),
    (43, ev("s4", kind="kill", gold=50), True, None, [], ["unexpected_field"]),
    (44, ev("s34", player_id="0", item_id="it_x", team="blue"), True, None, [],
     ["player_id_sentinel", "unexpected_field"]),
    # --- slice 2 kinds (simulator/corrupt.py) and two parser pins -----------------------
    (45, ev("h7g", gold="100"), True, "missing_required_field",
     ["missing_required_field"], []),           # wrong_type field: no "100" -> 100
    (46, ev("h7h", match_seq="abc"), False, "missing_required_field",
     ["missing_required_field"], []),           # wrong_type envelope: parse_ok false
    (47, half(ev("h1b")), False, "not_json", ["not_json", "missing_event_id",
                                             "bad_event_time", "missing_required_field"],
     []),                                       # malformed_json: half a message
    (48, ev("h3b", event_time="2026-11-27 10:00:00"), False, "bad_event_time",
     ["bad_event_time"], []),                   # bad_time_format
    (49, ev("h4b", event_time="1970-01-01T00:12:34.567Z"), True, "event_time_implausible",
     ["event_time_implausible"], []),           # epoch_time (F5)
    (50, ev("h7i", match_id=""), True, "missing_required_field",
     ["missing_required_field"], []),           # empty_required: '' is non-NULL, keyed
    (51, ev("h8b", patch_version="14.2.1"), True, "bad_patch_format",
     ["bad_patch_format"], []),                 # bad_patch (F8)
    (52, ev("h7j", kind="match_end", duration_s=DROP), True, "missing_required_field",
     ["missing_required_field"], []),           # a per-type number absent: keyed
    (53, ev("h10d", kind="match_end", duration_s=0), True, "out_of_range",
     ["out_of_range"], []),
    (54, ev("h7k", schema_version=DROP), False, "missing_required_field",
     ["missing_required_field"], []),           # an absent parse field: keyless
    (55, "{}", False, "missing_event_id", ["missing_event_id", "bad_event_time",
                                           "missing_required_field"], []),  # an object
    (56, ev("h5b", event_type=7), True, "unknown_event_type", ["unknown_event_type"],
     []),                                       # a number in a string field is its text
]


# --- every corrupt.py kind on every type it applies to (three-way test) -------------------

def corrupt_events(t: datetime) -> list[GameEvent]:
    """One valid event of each type at time t (match m-1), for the corrupt.py cases."""
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


def corrupt_cases(events: list[GameEvent]) -> list[tuple[GameEvent, corrupt.Choice]]:
    """Every kind x every type it applies to x every field / variant it can pick."""
    out = []
    for e in events:
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
