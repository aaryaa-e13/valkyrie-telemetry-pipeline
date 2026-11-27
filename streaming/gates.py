"""Phase 3 quality gates (DECISIONS D3.2, plan docs/plans/phase3-plan.md section 1).

Every bronze row that silver reads is checked against the rules of one rules version:

* hard rules H1-H10 -> the row is rejected to the dead-letter table (DLQ). The
  `reason_code` is the FIRST failing rule in the fixed order below; `reasons` lists
  every failing rule. Each rule fires only on its own condition (e.g. H5 on a non-null
  unknown type; a NULL type is H7), so one fault gives exactly one expected reason.
* soft rules S1-S4 -> the row is kept and gets a flag in `quality_flags`.

Which rows are KEYED does not depend on the rule: `key_ok` (streaming/silver.py,
amended by C3) decides it. A row with an absent or wrong-typed envelope field has
parse_ok false, so it is keyless (keyed by Kafka offset in the DLQ) although its
reason is H7 (slice 0, P1).

The rule data here is plain Python (no Spark import at module level), so the config
and the unit tests can import it. The Spark expressions are built by `hard_reasons`
and `soft_flags`. Verify re-implements the rules on its own (slice 2, R11).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column

RULES_VERSIONS: tuple[int, ...] = (1, 2)

EVENT_TYPES: tuple[str, ...] = ("match_start", "champion_pick", "kill", "item_purchase",
                                "match_end")
# Envelope fields every type needs (event_id and event_time are H2/H3).
ENVELOPE: tuple[str, ...] = ("event_type", "schema_version", "match_id", "match_seq",
                             "patch_version", "region")
# Per-type fields, from simulator/generator.py (VERIFIED in the plan).
REQUIRED_BY_TYPE: dict[str, tuple[str, ...]] = {
    "match_start": (),
    "champion_pick": ("player_id", "champion_id", "team"),
    "kill": ("player_id", "champion_id", "victim_id"),
    "item_purchase": ("player_id", "champion_id", "item_id", "gold"),
    "match_end": ("winning_team", "duration_s"),
}
TYPE_FIELDS: tuple[str, ...] = ("player_id", "champion_id", "team", "victim_id", "item_id",
                                "gold", "winning_team", "duration_s")
NUMERIC_FIELDS: frozenset[str] = frozenset({"schema_version", "match_seq", "gold",
                                            "duration_s"})
SCHEMA_VERSIONS: tuple[int, ...] = (1,)
TEAMS: tuple[str, ...] = ("blue", "red")
REGIONS: dict[int, tuple[str, ...]] = {
    1: ("EUW", "NA", "KR", "BR"),
    2: ("EUW", "NA", "KR", "BR", "SEA"),   # v2: a new region (replay demo, slice 3)
}
EVENT_TIME_FLOOR = datetime(2020, 1, 1, tzinfo=UTC)
FUTURE_TOLERANCE_S = 300          # S1, D1.13: event_time > ingest_time + 5 min
PATCH_PATTERN = r"^[0-9]+\.[0-9]+$"
PLAYER_PATTERN = r"^p_[0-9]{6}$"

# Fixed evaluation order. Never reorder: reason_code is the first failing rule.
HARD_CODES: tuple[str, ...] = (
    "not_json",                      # H1
    "missing_event_id",              # H2
    "bad_event_time",                # H3
    "event_time_implausible",        # H4
    "unknown_event_type",            # H5
    "unsupported_schema_version",    # H6
    "missing_required_field",        # H7
    "bad_patch_format",              # H8
    "bad_enum",                      # H9
    "out_of_range",                  # H10
)
SOFT_CODES: tuple[str, ...] = (
    "future_event_time",             # S1
    "self_kill",                     # S2
    "player_id_sentinel",            # S3
    "unexpected_field",              # S4
)


def check_version(rules_version: int) -> int:
    if isinstance(rules_version, bool) or rules_version not in RULES_VERSIONS:
        raise ValueError(f"unknown rules version {rules_version!r}; "
                         f"known: {list(RULES_VERSIONS)}")
    return int(rules_version)


# --- Spark expressions --------------------------------------------------------------


def _missing(name: str) -> Column:
    """Absent, NULL or (for strings) blank after trim: one meaning (F1, F7)."""
    from pyspark.sql import functions as F

    c = F.col(name)
    if name in NUMERIC_FIELDS:
        return c.isNull()
    return c.isNull() | (F.trim(c) == F.lit(""))


def _safe(cond: Column) -> Column:
    """NULL-safe boolean: a NULL condition is false (never a NULL rule result)."""
    from pyspark.sql import functions as F

    return F.coalesce(cond, F.lit(False))


def hard_conditions(rules_version: int) -> list[tuple[str, Column]]:
    """(code, condition) for H1-H10 in the fixed order. Needs bronze columns."""
    from pyspark.sql import functions as F

    v = check_version(rules_version)
    et, typ = F.col("event_time"), F.col("event_type")
    per_type = F.lit(False)
    for t, fields in REQUIRED_BY_TYPE.items():
        for f in fields:
            per_type = per_type | ((typ == F.lit(t)) & _missing(f))
    envelope = F.lit(False)
    for f in ENVELOPE:
        envelope = envelope | _missing(f)
    not_blank = lambda n: ~_missing(n)  # noqa: E731
    return [(c, _safe(cond)) for c, cond in (
        ("not_json", F.expr("json_object_keys(raw_value)").isNull()),
        ("missing_event_id", _missing("event_id")),
        ("bad_event_time", et.isNull()),
        ("event_time_implausible",
         et < F.lit(EVENT_TIME_FLOOR.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")),
        ("unknown_event_type", typ.isNotNull() & ~typ.isin(*EVENT_TYPES)),
        ("unsupported_schema_version",
         F.col("schema_version").isNotNull() & ~F.col("schema_version").isin(
             *SCHEMA_VERSIONS)),
        ("missing_required_field", envelope | per_type),
        ("bad_patch_format",
         not_blank("patch_version") & ~F.col("patch_version").rlike(PATCH_PATTERN)),
        ("bad_enum",
         (not_blank("team") & ~F.col("team").isin(*TEAMS))
         | (not_blank("winning_team") & ~F.col("winning_team").isin(*TEAMS))
         | (not_blank("region") & ~F.col("region").isin(*REGIONS[v]))),
        ("out_of_range",
         (F.col("match_seq") < 1) | (F.col("gold") <= 0) | (F.col("duration_s") <= 0)),
    )]


def soft_conditions() -> list[tuple[str, Column]]:
    """(code, condition) for S1-S4 (the same in every rules version so far)."""
    from pyspark.sql import functions as F

    typ = F.col("event_type")
    unexpected = F.lit(False)
    for t, fields in REQUIRED_BY_TYPE.items():
        for f in TYPE_FIELDS:
            if f not in fields:
                unexpected = unexpected | ((typ == F.lit(t)) & F.col(f).isNotNull())
    return [(c, _safe(cond)) for c, cond in (
        ("future_event_time", F.col("event_time") > F.col("ingest_time")
         + F.expr(f"INTERVAL {FUTURE_TOLERANCE_S} SECONDS")),
        ("self_kill", (typ == F.lit("kill")) & (F.col("victim_id") == F.col("player_id"))),
        ("player_id_sentinel",
         ~_missing("player_id") & ~F.col("player_id").rlike(PLAYER_PATTERN)),
        ("unexpected_field", unexpected),
    )]


def reason_code(rules_version: int) -> Column:
    """The first failing hard rule, or NULL if the row passes every hard rule."""
    from pyspark.sql import functions as F

    return F.coalesce(*[F.when(cond, F.lit(code)) for code, cond in
                        hard_conditions(rules_version)])


def reasons(rules_version: int) -> Column:
    """Every failing hard rule, in order ([] if none)."""
    from pyspark.sql import functions as F

    return F.array_compact(F.array(*[F.when(cond, F.lit(code)) for code, cond in
                                     hard_conditions(rules_version)]))


def flags() -> Column:
    """Every soft rule that fires, in order ([] if none)."""
    from pyspark.sql import functions as F

    return F.array_compact(F.array(*[F.when(cond, F.lit(code)) for code, cond in
                                     soft_conditions()]))
