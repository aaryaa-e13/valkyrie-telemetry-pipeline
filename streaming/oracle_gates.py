"""Verify's own quality gates, for oracle rules R11 and R14 (Phase 3 slice 2, D3.7 Q1 A).

Standard library only. It imports nothing from the job (`streaming/gates.py`,
`streaming/silver.py`, `streaming/schemas.py`, `streaming/bronze.py`): it re-parses
each bronze row's `raw_value` with `json` instead of Spark's `from_json`, and states the
rules again. A bug in the job's parser or rules then shows up as an R11 FAIL instead of
being shared by the job and its check. tests/unit/test_oracle_gates.py asserts the
import list.

What it copies from the job, on purpose (each pinned by the shared corpus,
tests/gates_corpus.py, which both this module's unit test and the job's Spark test run):

* parsing (Spark 4.1.4 `from_json`, PERMISSIVE, D3.2 P1):
    - not valid JSON, or not a JSON object -> every field NULL (and H1);
    - a field of the wrong JSON type becomes NULL, only that field: an int field takes a
      JSON integer token in range (int32; int64 for `gold`), never a string ("7"), a
      float, or a boolean; a string field takes a JSON string, and any other non-null
      value becomes its JSON text (Spark copies the token's structure; an integer
      becomes its digits, pinned by corpus row 56);
    - absent and null are the same (NULL);
    - `event_time` is a string parsed with `yyyy-MM-dd'T'HH:mm:ss.SSSX`; anything else is
      NULL (H3).
* the key check (job's key_ok, C3): parse_ok (the six parse fields non-NULL) and a
  non-blank event_id and a non-NULL event_time. "Blank" means empty after removing
  SPACES only: Spark's `trim` strips ' ' (U+0020), not tabs or newlines.
* rules H1-H10 / S1-S4 of rules versions 1 and 2, reason_code = the first failing hard
  rule in the fixed order, `reasons` = every failing hard rule, `flags` = every soft rule.

UNVERIFIED (outside the corpus, so not pinned): time offsets other than 'Z'
(`+01`, `+0100`); the text of a float, boolean, array or object inside a string field
(Jackson may re-format it, e.g. 1.50 -> 1.5); duplicate JSON
keys, NaN/Infinity, trailing bytes after the object, line terminators other than '\\n'
before the end of a regex-checked string. The simulator sends none of these.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

RULES_VERSIONS = (1, 2)
# (name, type) in the job's bronze schema order. "int" = int32, "long" = int64.
FIELDS: tuple[tuple[str, str], ...] = (
    ("event_id", "string"), ("event_type", "string"), ("schema_version", "int"),
    ("event_time", "string"), ("match_id", "string"), ("match_seq", "int"),
    ("patch_version", "string"), ("region", "string"), ("player_id", "string"),
    ("champion_id", "string"), ("team", "string"), ("victim_id", "string"),
    ("item_id", "string"), ("gold", "long"), ("winning_team", "string"),
    ("duration_s", "int"),
)
PARSE_FIELDS = ("event_id", "event_type", "schema_version", "event_time", "match_id",
                "match_seq")
NUMERIC = frozenset(n for n, t in FIELDS if t != "string")
EVENT_TYPES = ("match_start", "champion_pick", "kill", "item_purchase", "match_end")
ENVELOPE = ("event_type", "schema_version", "match_id", "match_seq", "patch_version",
            "region")
REQUIRED_BY_TYPE: dict[str, tuple[str, ...]] = {
    "match_start": (),
    "champion_pick": ("player_id", "champion_id", "team"),
    "kill": ("player_id", "champion_id", "victim_id"),
    "item_purchase": ("player_id", "champion_id", "item_id", "gold"),
    "match_end": ("winning_team", "duration_s"),
}
TYPE_FIELDS = ("player_id", "champion_id", "team", "victim_id", "item_id", "gold",
               "winning_team", "duration_s")
SCHEMA_VERSIONS = (1,)
TEAMS = ("blue", "red")
REGIONS: dict[int, tuple[str, ...]] = {
    1: ("EUW", "NA", "KR", "BR"),
    2: ("EUW", "NA", "KR", "BR", "SEA"),
}
EVENT_TIME_FLOOR = datetime(2020, 1, 1, tzinfo=UTC)
FUTURE_TOLERANCE = timedelta(seconds=300)
PATCH_RE = re.compile(r"^[0-9]+\.[0-9]+$")
PLAYER_RE = re.compile(r"^p_[0-9]{6}$")
HARD_CODES = ("not_json", "missing_event_id", "bad_event_time", "event_time_implausible",
              "unknown_event_type", "unsupported_schema_version", "missing_required_field",
              "bad_patch_format", "bad_enum", "out_of_range")
SOFT_CODES = ("future_event_time", "self_kill", "player_id_sentinel", "unexpected_field")
_INT_RANGE = {"int": (-(2 ** 31), 2 ** 31 - 1), "long": (-(2 ** 63), 2 ** 63 - 1)}
# yyyy-MM-dd'T'HH:mm:ss.SSSX: exactly 3 fraction digits, then Z (or +HH / +HHmm,
# UNVERIFIED against Spark).
_TIME_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})"
                      r"(Z|[+-]\d{2}(?:\d{2})?)")


class _Num(str):
    """A JSON number token kept as its source text (so ints are exact and a number in a
    string field becomes its own text, as Spark copies it)."""

    is_int: bool = True


class _Float(_Num):
    is_int = False


def _to_text(v: Any) -> str:
    """A non-string JSON value as compact JSON text (Spark copies the structure)."""
    if isinstance(v, _Num):
        return str(v)
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, float):                     # NaN / Infinity constants (UNVERIFIED)
        return json.dumps(v)
    if isinstance(v, list):
        return "[" + ",".join("null" if x is None else _to_text(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" +
                              ("null" if x is None else _to_text(x))
                              for k, x in v.items()) + "}"
    raise TypeError(f"unexpected JSON value {v!r}")


def _convert(kind: str, v: Any) -> Any:
    if v is None:
        return None
    if kind == "string":
        return v if type(v) is str else _to_text(v)
    if isinstance(v, _Num) and v.is_int:
        n = int(v)
        lo, hi = _INT_RANGE[kind]
        return n if lo <= n <= hi else None
    return None                                   # "7", 7.0, true, [], {} -> NULL


def parse_time(text: str | None) -> datetime | None:
    """`try_to_timestamp(text, "yyyy-MM-dd'T'HH:mm:ss.SSSX")`, as UTC; None on no match."""
    if text is None:
        return None
    m = _TIME_RE.fullmatch(text)
    if m is None:
        return None
    y, mo, d, h, mi, s, ms, off = m.groups()
    try:
        t = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s), int(ms) * 1000,
                     tzinfo=UTC)
    except ValueError:
        return None
    if off != "Z":
        sign = 1 if off[0] == "+" else -1
        t -= sign * timedelta(hours=int(off[1:3]), minutes=int(off[3:5] or 0))
    return t


@dataclass(frozen=True)
class Parsed:
    """One bronze row as the job's bronze parse would give it."""

    is_object: bool                       # json_object_keys(raw_value) IS NOT NULL
    fields: dict[str, Any]                # every schema field (None = NULL)
    event_time: datetime | None

    @property
    def parse_ok(self) -> bool:
        return all((self.event_time if f == "event_time" else self.fields[f]) is not None
                   for f in PARSE_FIELDS)


def parse(raw_value: str | None) -> Parsed:
    """Parse one Kafka value the way bronze does (see the module docstring)."""
    empty = {name: None for name, _ in FIELDS}
    if raw_value is None:
        return Parsed(False, empty, None)
    try:
        obj = json.loads(raw_value, parse_int=_Num, parse_float=_Float)
    except ValueError:
        return Parsed(False, empty, None)
    if not isinstance(obj, dict):
        return Parsed(False, empty, None)
    fields = {name: _convert(kind, obj.get(name)) for name, kind in FIELDS}
    return Parsed(True, fields, parse_time(fields["event_time"]))


def _blank(v: Any) -> bool:
    """Absent, NULL or (strings) empty after stripping spaces: one meaning (F1, F7)."""
    return v is None or (isinstance(v, str) and v.strip(" ") == "")


def key_ok(p: Parsed) -> bool:
    """The job's key check (key_ok, amended by C3)."""
    return p.parse_ok and not _blank(p.fields["event_id"]) and p.event_time is not None


def check_version(rules_version: int) -> int:
    if isinstance(rules_version, bool) or rules_version not in RULES_VERSIONS:
        raise ValueError(f"unknown rules version {rules_version!r}; known: "
                         f"{list(RULES_VERSIONS)}")
    return int(rules_version)


def hard_reasons(p: Parsed, rules_version: int) -> tuple[str, ...]:
    """Every failing hard rule, in the fixed order (empty: the row passes)."""
    v = check_version(rules_version)
    f, et, typ = p.fields, p.event_time, p.fields["event_type"]

    def bad_enum(name: str, allowed: tuple[str, ...]) -> bool:
        return not _blank(f[name]) and f[name] not in allowed

    def lt(name: str, bound: int) -> bool:
        return f[name] is not None and f[name] <= bound

    conditions = (
        ("not_json", not p.is_object),
        ("missing_event_id", _blank(f["event_id"])),
        ("bad_event_time", et is None),
        ("event_time_implausible", et is not None and et < EVENT_TIME_FLOOR),
        ("unknown_event_type", typ is not None and typ not in EVENT_TYPES),
        ("unsupported_schema_version",
         f["schema_version"] is not None and f["schema_version"] not in SCHEMA_VERSIONS),
        ("missing_required_field",
         any(_blank(f[n]) for n in ENVELOPE)
         or any(typ == t and _blank(f[n]) for t, ns in REQUIRED_BY_TYPE.items() for n in ns)),
        ("bad_patch_format",
         not _blank(f["patch_version"]) and PATCH_RE.search(f["patch_version"]) is None),
        ("bad_enum", bad_enum("team", TEAMS) or bad_enum("winning_team", TEAMS)
         or bad_enum("region", REGIONS[v])),
        ("out_of_range", lt("match_seq", 0) or lt("gold", 0) or lt("duration_s", 0)),
    )
    return tuple(code for code, failed in conditions if failed)


def soft_flags(p: Parsed, ingest_time: datetime | None) -> tuple[str, ...]:
    """Every soft rule that fires, in order (the same in every rules version)."""
    f, et, typ = p.fields, p.event_time, p.fields["event_type"]
    conditions = (
        ("future_event_time", et is not None and ingest_time is not None
         and et > ingest_time + FUTURE_TOLERANCE),
        ("self_kill", typ == "kill" and f["victim_id"] is not None
         and f["player_id"] is not None and f["victim_id"] == f["player_id"]),
        ("player_id_sentinel",
         not _blank(f["player_id"]) and PLAYER_RE.search(f["player_id"]) is None),
        ("unexpected_field", any(typ == t and f[n] is not None
                                 for t, ns in REQUIRED_BY_TYPE.items()
                                 for n in TYPE_FIELDS if n not in ns)),
    )
    return tuple(code for code, fired in conditions if fired)


@dataclass(frozen=True)
class Verdict:
    """What the gates decide for one bronze row."""

    keyed: bool
    event_id: str | None
    dlq_key: str | None          # None: the row passes every hard rule
    reason_code: str | None
    reasons: tuple[str, ...]
    flags: tuple[str, ...]

    @property
    def rejected(self) -> bool:
        return self.reason_code is not None

    @property
    def key_kind(self) -> str:
        """The DLQ's key_kind column value."""
        return "event_id" if self.keyed else "offset"


def classify(raw_value: str | None, *, topic: str, partition: int, offset: int,
             ingest_time: datetime | None, rules_version: int) -> Verdict:
    """The gates' decision for one bronze row (its raw value + Kafka position)."""
    p = parse(raw_value)
    keyed = key_ok(p)
    reasons = hard_reasons(p, rules_version)
    eid = p.fields["event_id"]
    key = None
    if reasons:
        key = f"id:{eid}" if keyed else f"off:{topic}/{partition}/{offset}"
    return Verdict(keyed, eid, key, reasons[0] if reasons else None, reasons,
                   soft_flags(p, ingest_time))
