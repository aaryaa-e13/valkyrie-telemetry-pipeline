"""Bad-data mode: corrupt chosen events on purpose and predict what silver must do with them.

Phase 3 slice 2 (DECISIONS D3.7, plan docs/plans/phase3-slice2-plan.md section 2). Pure:
no Kafka, no clock, no randomness outside sha256. Same seed + same events = same choices,
the same corrupted bytes and the same predictions.

How events are chosen (deterministic, its own salt so it never moves the generator):
    h = sha256(f"{seed}:bad:{event_id}")
    u1 = h[0:8] / 2**64         bad iff u1 < bad_fraction
    u2 = h[8:16]                which kind, among the enabled kinds that apply to the type
    u3 = h[16:24]               which field or variant, where a kind has a choice
At most one kind per event. Raising bad_fraction only adds bad events (u2 and u3 do not
depend on it). `new_region` is different: it corrupts EVERY event of exactly one match,
the one with the smallest sha256(f"{seed}:region:{match_id}"), and those events get no
other kind.

Each corrupted event carries its expected result: either a dead-letter row
(`expected_class` "dlq", the reason_code = the first failing hard rule H1-H10, and the
key kind) or a kept row with one soft flag (`expected_class` "flag"). The rule data
below is written again here on purpose: the simulator image has no `streaming/`, and a
copy that drifts from the job's rules is caught by the three-way test (this module's
prediction == the job's gates == verify's own gates, on the same bytes).

The key kind follows the job's key check, not the rule number (D3.2 correction 3):
a message without a usable event_id or event_time, or with an absent / NULL /
wrong-typed parse field (event_type, schema_version, match_id, match_seq), is keyless:
it goes to the DLQ keyed by Kafka offset. Every other rejected row is keyed by id.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from simulator.events import GameEvent, format_event_time

RULES_VERSION_EXPECTED = 1      # the gates' rules version the predictions hold for
NEW_REGION = "SEA"              # unknown under rules v1 (H9), allowed in v2 (replay demo)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# --- the job's rule data, written again (see module docstring) --------------------------
ENVELOPE = ("event_type", "schema_version", "match_id", "match_seq", "patch_version",
            "region")
PARSE_FIELDS = frozenset({"event_type", "schema_version", "match_id", "match_seq"})
NUMERIC = frozenset({"schema_version", "match_seq", "gold", "duration_s"})
REQUIRED_BY_TYPE: dict[str, tuple[str, ...]] = {
    "match_start": (),
    "champion_pick": ("player_id", "champion_id", "team"),
    "kill": ("player_id", "champion_id", "victim_id"),
    "item_purchase": ("player_id", "champion_id", "item_id", "gold"),
    "match_end": ("winning_team", "duration_s"),
}
TYPE_FIELDS = ("player_id", "champion_id", "team", "victim_id", "item_id", "gold",
               "winning_team", "duration_s")
# A value for a field the type does not use: it triggers S4 and nothing else.
EXTRA_VALUES: dict[str, Any] = {
    "player_id": "p_000001", "champion_id": "ch_quillon", "team": "blue",
    "victim_id": "p_000002", "item_id": "it_iron_sigil", "gold": 400,
    "winning_team": "red", "duration_s": 1500,
}

DLQ, FLAG = "dlq", "flag"
KEY_ID, KEY_OFFSET = "id", "offset"

KINDS: tuple[str, ...] = (
    "malformed_json", "drop_event_id", "empty_event_id", "bad_time_format", "epoch_time",
    "unknown_type", "schema_v99", "drop_required", "null_required", "empty_required",
    "wrong_type", "bad_patch", "bad_team", "new_region", "negative_gold",
    "future_time", "self_kill", "player_zero", "extra_field",
)
_ALL = frozenset(REQUIRED_BY_TYPE)
APPLIES_TO: dict[str, frozenset[str]] = {
    **{k: _ALL for k in KINDS},
    "bad_team": frozenset({"champion_pick", "match_end"}),
    "negative_gold": frozenset({"item_purchase"}),
    "self_kill": frozenset({"kill"}),
    "player_zero": frozenset({"champion_pick", "kill", "item_purchase"}),
}
WRONG_TYPE_VARIANTS = ("envelope", "field")      # match_seq: "abc" / gold: "100"

# kind -> (class, reason or flag). The key kind of drop/null_required and wrong_type
# depends on the field (expected_for).
_RESULT: dict[str, tuple[str, str]] = {
    "malformed_json": (DLQ, "not_json"),
    "drop_event_id": (DLQ, "missing_event_id"),
    "empty_event_id": (DLQ, "missing_event_id"),
    "bad_time_format": (DLQ, "bad_event_time"),
    "epoch_time": (DLQ, "event_time_implausible"),
    "unknown_type": (DLQ, "unknown_event_type"),
    "schema_v99": (DLQ, "unsupported_schema_version"),
    "drop_required": (DLQ, "missing_required_field"),
    "null_required": (DLQ, "missing_required_field"),
    "empty_required": (DLQ, "missing_required_field"),
    "wrong_type": (DLQ, "missing_required_field"),
    "bad_patch": (DLQ, "bad_patch_format"),
    "bad_team": (DLQ, "bad_enum"),
    "new_region": (DLQ, "bad_enum"),
    "negative_gold": (DLQ, "out_of_range"),
    "future_time": (FLAG, "future_event_time"),
    "self_kill": (FLAG, "self_kill"),
    "player_zero": (FLAG, "player_id_sentinel"),
    "extra_field": (FLAG, "unexpected_field"),
}
_KEYLESS_KINDS = frozenset({"malformed_json", "drop_event_id", "empty_event_id",
                            "bad_time_format"})
HARD_CODES = ("not_json", "missing_event_id", "bad_event_time", "event_time_implausible",
              "unknown_event_type", "unsupported_schema_version", "missing_required_field",
              "bad_patch_format", "bad_enum", "out_of_range")
SOFT_CODES = ("future_event_time", "self_kill", "player_id_sentinel", "unexpected_field")


@dataclass(frozen=True)
class Choice:
    """Which fault an event gets: the kind, plus a variant or field where there is a choice."""

    kind: str
    variant: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class Expected:
    """What the silver job must do with the corrupted message."""

    cls: str                 # "dlq" or "flag"
    code: str                # reason_code (dlq) or the one flag (flag)
    key_kind: str | None     # "id" or "offset" for dlq; None for a kept row


@dataclass(frozen=True)
class BadEvent:
    """One corrupted event: the bytes actually sent and the expected result."""

    event_id: str            # the logical id (the manifest's id)
    match_id: str
    event_type: str
    choice: Choice
    sent_event_id: str | None   # what the message carried: the id, "" or None (unreadable)
    expected: Expected
    value: bytes

    @property
    def kind(self) -> str:
        return self.choice.kind


def _hash_ints(seed: int, tag: str, key: str) -> tuple[float, int, int]:
    h = hashlib.sha256(f"{seed}:{tag}:{key}".encode()).digest()
    return (int.from_bytes(h[0:8], "big") / 2 ** 64, int.from_bytes(h[8:16], "big"),
            int.from_bytes(h[16:24], "big"))


def field_options(kind: str, event_type: str) -> tuple[str, ...]:
    """The fields (or variants) a kind can pick from for one event type; () = no choice."""
    per = REQUIRED_BY_TYPE[event_type]
    if kind in ("drop_required", "null_required"):
        return (*ENVELOPE, *per)
    if kind == "empty_required":
        # String fields only; never event_type (a non-null "" type is H5) nor event_id
        # (that is empty_event_id).
        return tuple(f for f in (*ENVELOPE, *per) if f not in NUMERIC and f != "event_type")
    if kind == "wrong_type":
        return WRONG_TYPE_VARIANTS if event_type == "item_purchase" else ("envelope",)
    if kind == "extra_field":
        return tuple(f for f in TYPE_FIELDS if f not in per)
    if kind == "bad_team":
        return ("team",) if event_type == "champion_pick" else ("winning_team",)
    return ()


def expected_for(choice: Choice, event_type: str) -> Expected:
    """The predicted result of one fault on one event type."""
    cls, code = _RESULT[choice.kind]
    if cls == FLAG:
        return Expected(FLAG, code, None)
    if choice.kind in _KEYLESS_KINDS:
        return Expected(DLQ, code, KEY_OFFSET)
    if choice.kind in ("drop_required", "null_required"):
        return Expected(DLQ, code, KEY_OFFSET if choice.field in PARSE_FIELDS else KEY_ID)
    if choice.kind == "wrong_type":
        return Expected(DLQ, code, KEY_OFFSET if choice.variant == "envelope" else KEY_ID)
    return Expected(DLQ, code, KEY_ID)


def check_kinds(kinds: Iterable[str]) -> tuple[str, ...]:
    """Validate kind names; returns them in KINDS order. Raises ValueError."""
    ks = set(kinds)
    unknown = sorted(ks - set(KINDS))
    if unknown:
        raise ValueError(f"unknown bad-data kinds {unknown}; known: {list(KINDS)}")
    return tuple(k for k in KINDS if k in ks)


def region_match(match_ids: Iterable[str], seed: int) -> str | None:
    """The one match `new_region` corrupts: the smallest sha256(f"{seed}:region:{m}")."""
    ids = sorted(set(match_ids))
    if not ids:
        return None
    return min(ids, key=lambda m: hashlib.sha256(f"{seed}:region:{m}".encode()).hexdigest())


def choose(events: Sequence[GameEvent], seed: int, bad_fraction: float,
           kinds: Iterable[str]) -> dict[str, Choice]:
    """event_id -> Choice for every event that gets a fault (pure: ids and types only)."""
    if not 0.0 <= bad_fraction <= 1.0:
        raise ValueError(f"bad_fraction must be in [0, 1], got {bad_fraction}")
    enabled = check_kinds(kinds)
    out: dict[str, Choice] = {}
    if bad_fraction == 0.0 or not enabled:
        return out
    region = region_match((e.match_id for e in events), seed) \
        if "new_region" in enabled else None
    per_event = [k for k in enabled if k != "new_region"]
    for e in events:
        if e.match_id == region:
            out[e.event_id] = Choice("new_region", field="region")
            continue
        u1, u2, u3 = _hash_ints(seed, "bad", e.event_id)
        if u1 >= bad_fraction:
            continue
        t = e.event_type.value
        applicable = [k for k in per_event if t in APPLIES_TO[k]]
        if not applicable:
            continue
        kind = applicable[u2 % len(applicable)]
        options = field_options(kind, t)
        pick = options[u3 % len(options)] if options else None
        if kind == "wrong_type":
            out[e.event_id] = Choice(kind, variant=pick,
                                     field="match_seq" if pick == "envelope" else "gold")
        else:
            out[e.event_id] = Choice(kind, field=pick)
    return out


def _dumps(d: Mapping[str, Any]) -> bytes:
    return json.dumps(d, separators=(",", ":")).encode("utf-8")


def corrupt_value(event: GameEvent, choice: Choice, match_start: datetime
                  ) -> tuple[bytes, str | None]:
    """(the corrupted Kafka value, the event_id the message carries; None = unreadable)."""
    d = event.to_dict()
    k, f = choice.kind, choice.field
    if k == "malformed_json":
        full = _dumps(d)
        return full[: len(full) // 2], None        # an object without its closing brace
    if k == "drop_event_id":
        d.pop("event_id")
        return _dumps(d), None
    if k == "empty_event_id":
        d["event_id"] = ""
        return _dumps(d), ""
    if k == "bad_time_format":
        d["event_time"] = event.event_time.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    elif k == "epoch_time":
        # FINDINGS F5: game time (ms since the match start) read as epoch time.
        d["event_time"] = format_event_time(EPOCH + (event.event_time - match_start))
    elif k == "unknown_type":
        d["event_type"] = "ward_placed"
    elif k == "schema_v99":
        d["schema_version"] = 99
    elif k == "drop_required":
        assert f is not None
        d.pop(f)
    elif k == "null_required":
        assert f is not None
        d[f] = None
    elif k == "empty_required":
        assert f is not None
        d[f] = ""
    elif k == "wrong_type":
        if choice.variant == "envelope":
            d["match_seq"] = "abc"
        else:
            d["gold"] = str(d["gold"])                # "100": no string -> int coercion
    elif k == "bad_patch":
        d["patch_version"] = "14.2.1"                  # FINDINGS F8: a 4-part-style patch
    elif k == "bad_team":
        assert f is not None
        d[f] = "green"
    elif k == "new_region":
        d["region"] = NEW_REGION
    elif k == "negative_gold":
        d["gold"] = -5
    elif k == "future_time":
        d["event_time"] = format_event_time(event.event_time + timedelta(hours=24))
    elif k == "self_kill":
        d["victim_id"] = d["player_id"]
    elif k == "player_zero":
        d["player_id"] = "0"                           # FINDINGS F4: a sentinel id
    elif k == "extra_field":
        assert f is not None
        d[f] = EXTRA_VALUES[f]
    else:  # pragma: no cover - check_kinds keeps unknown kinds out
        raise ValueError(f"unknown kind {k!r}")
    return _dumps(d), event.event_id


def corrupt_run(events: Sequence[GameEvent], seed: int, bad_fraction: float,
                kinds: Iterable[str]) -> dict[str, BadEvent]:
    """event_id -> BadEvent for every chosen event (events must hold whole matches:
    `epoch_time` needs each match's start, the time of its first event)."""
    choices = choose(events, seed, bad_fraction, kinds)
    starts: dict[str, datetime] = {}
    for e in events:
        if e.match_id not in starts or e.event_time < starts[e.match_id]:
            starts[e.match_id] = e.event_time
    out: dict[str, BadEvent] = {}
    for e in events:
        c = choices.get(e.event_id)
        if c is None:
            continue
        value, sent = corrupt_value(e, c, starts[e.match_id])
        out[e.event_id] = BadEvent(e.event_id, e.match_id, e.event_type.value, c, sent,
                                   expected_for(c, e.event_type.value), value)
    return out


# --- coverage: does one run exercise every gate? (plan section 5) -----------------------


def coverage(events: Sequence[GameEvent], choices: Mapping[str, Choice]) -> Counter[str]:
    """Units covered by these choices, with counts:
    kind:<k>, gate:<reason or flag>, wrong_type:<variant>, h7:<id|offset>."""
    types = {e.event_id: e.event_type.value for e in events}
    out: Counter[str] = Counter()
    for eid, c in choices.items():
        x = expected_for(c, types[eid])
        out[f"kind:{c.kind}"] += 1
        out[f"gate:{x.code}"] += 1
        if c.kind == "wrong_type":
            out[f"wrong_type:{c.variant}"] += 1
        if x.code == "missing_required_field":
            out[f"h7:{x.key_kind}"] += 1
    return out


def required_units(kinds: Iterable[str]) -> frozenset[str]:
    """What a covering run needs, given the enabled kinds."""
    enabled = check_kinds(kinds)
    units = {f"kind:{k}" for k in enabled} | {f"gate:{_RESULT[k][1]}" for k in enabled}
    if "wrong_type" in enabled:
        units |= {f"wrong_type:{v}" for v in WRONG_TYPE_VARIANTS}
    if {"drop_required", "null_required", "wrong_type"} & set(enabled):
        units |= {"h7:id", "h7:offset"}
    return frozenset(units)


def missing_units(events: Sequence[GameEvent], choices: Mapping[str, Choice],
                  kinds: Iterable[str]) -> list[str]:
    """Required units with no event ([] = this run covers everything)."""
    have = coverage(events, choices)
    return sorted(u for u in required_units(kinds) if have[u] == 0)


@dataclass(frozen=True)
class SeedSearch:
    """Result of find_covering_seed."""

    seed: int | None             # None: no covering seed within max_seeds
    tried: int
    missing: list[str]           # for the last seed tried when seed is None, else []
    coverage: dict[str, int]


def find_covering_seed(events_for_seed: Callable[[int], Sequence[GameEvent]],
                       base_seed: int, bad_fraction: float, kinds: Iterable[str],
                       max_seeds: int = 1000) -> SeedSearch:
    """Search base_seed, base_seed + 1, ... for a run that covers every required unit.

    `events_for_seed(seed)` returns the run's events (the generator; no Kafka). Choices
    depend only on ids and types, so times do not matter here.
    """
    enabled = check_kinds(kinds)
    last_missing: list[str] = []
    last_cov: dict[str, int] = {}
    for i in range(max_seeds):
        seed = base_seed + i
        events = events_for_seed(seed)
        choices = choose(events, seed, bad_fraction, enabled)
        last_missing = missing_units(events, choices, enabled)
        last_cov = dict(sorted(coverage(events, choices).items()))
        if not last_missing:
            return SeedSearch(seed, i + 1, [], last_cov)
    return SeedSearch(None, max_seeds, last_missing, last_cov)
