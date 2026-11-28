"""Read simulator run manifests of any supported version (standard library only).

Used by verification (Spark container) and the e2e orchestrator (laptop), so it
must not import pyspark or confluent_kafka.

Supported versions:
    1  event ids per match, counts, checksum. No Kafka offsets.
    2  as v1, plus `kafka_offsets` {partition: {min, max, count}}.
    3  as v2, plus `pacing`, `late_counts` and `late_events` (deliberate lateness).
    4  as v3, plus `bad_data` (deliberate bad data, Phase 3 slice 2): None when bad-data
       mode was off. Otherwise its counts and, unless left out, one BadEventEntry per
       delivered corrupted event with its expected result and Kafka (partition, offset).

What "no lateness information" means depends on the version:
    v1/v2  lateness was NOT RECORDED: `pacing`, `late_counts` and `late_events`
           are all None (never an empty collection).
    v3     `pacing` and `late_counts` are always set. `late_events` maps
           event_id -> LateEvent; an event that is not in it was sent ON TIME.
           If the writer left the list out, `late_events` is None and
           `late_events_omitted_reason` says why.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUPPORTED_VERSIONS = frozenset({1, 2, 3, 4})
BAD_CLASSES = frozenset({"dlq", "flag"})
KEY_KINDS = frozenset({"id", "offset"})
PACE_MODES = frozenset({"backfill", "realtime"})
BANDS = ("slight", "very")
EXPECTATIONS = frozenset({"keep", "may_drop"})


class ManifestError(ValueError):
    """The manifest cannot be used (unknown version, missing ids, bad structure)."""


@dataclass(frozen=True)
class OffsetRange:
    """Offsets of a run's acknowledged messages in one partition (inclusive)."""

    min: int
    max: int
    count: int


@dataclass(frozen=True)
class Band:
    """One delay band of deliberately late events (v3)."""

    min_delay_ms: int
    max_delay_ms: int
    expected: str  # "keep" or "may_drop"


@dataclass(frozen=True)
class Pacing:
    """How the run was sent (v3)."""

    mode: str  # "backfill" or "realtime"
    time_scale: float
    assumed_watermark_s: float
    late_fraction: float
    late_too_late_share: float
    bands: dict[str, Band]  # keys "slight" and "very"
    # Measured by the simulator's send loop; None if not measured.
    future_events: int | None
    max_ahead_ms: int | None
    max_behind_ms: int | None


@dataclass(frozen=True)
class LateEvent:
    """One delivered event that was deliberately sent late (v3)."""

    event_id: str
    match_id: str
    delay_ms: int
    band: str  # "slight" or "very"
    expected: str  # "keep" or "may_drop"
    stream_lag_ms: int


@dataclass(frozen=True)
class BadEventEntry:
    """One delivered event that was corrupted on purpose (v4)."""

    event_id: str                 # logical id (in the manifest's event_ids)
    sent_event_id: str | None     # what the message carried: the id, "" or None
    match_id: str
    event_type: str
    kind: str
    variant: str | None
    field: str | None
    expected_class: str           # "dlq" or "flag"
    expected_code: str            # reason_code (dlq) or the flag (flag)
    expected_key_kind: str | None  # "id" or "offset" (dlq); None (flag)
    kafka_partition: int
    kafka_offset: int

    @property
    def id_readable(self) -> bool:
        """Can bronze's event_id column find this message? False for a half message
        and for a missing or blank id (bronze then finds it by offset)."""
        return (self.kind != "malformed_json" and self.sent_event_id is not None
                and self.sent_event_id.strip(" ") != "")

    def dlq_key(self, topic: str) -> str | None:
        """The DLQ row key the job must write (None for a kept, flagged row)."""
        if self.expected_class != "dlq":
            return None
        if self.expected_key_kind == "id":
            return f"id:{self.event_id}"
        return f"off:{topic}/{self.kafka_partition}/{self.kafka_offset}"


@dataclass(frozen=True)
class BadData:
    """The v4 `bad_data` section."""

    bad_fraction: float
    enabled_kinds: tuple[str, ...]
    rules_version_expected: int
    new_region_match: str | None
    bad_counts: dict[str, int]
    expected_rejections: dict[str, int]
    expected_flags: dict[str, int]
    expected_by_key_kind: dict[str, int]
    # event_id -> entry; None when the list was left out (see the reason).
    bad_events: dict[str, BadEventEntry] | None
    bad_events_omitted_reason: str | None

    @property
    def expected_dlq_count(self) -> int:
        return sum(self.expected_rejections.values())

    def dlq_events(self) -> list[BadEventEntry]:
        return [b for b in (self.bad_events or {}).values() if b.expected_class == "dlq"]

    def flag_events(self) -> list[BadEventEntry]:
        return [b for b in (self.bad_events or {}).values() if b.expected_class == "flag"]


@dataclass(frozen=True)
class Manifest:
    """The parts of a run manifest that verification needs."""

    path: Path
    version: int
    run_id: str
    seed: int | None
    topic: str | None
    events_delivered: int
    # (match_id, event_id) for every acknowledged event, in match_seq order per match.
    event_pairs: tuple[tuple[str, str], ...]
    # partition -> offsets; None for v1 manifests (offsets were not recorded).
    kafka_offsets: dict[int, OffsetRange] | None
    # The next four are None for v1/v2 manifests: lateness was not recorded.
    pacing: Pacing | None = None
    # band -> number of delivered late events (v3: always present).
    late_counts: dict[str, int] | None = None
    # event_id -> LateEvent, in send order. v3: an event not in here was on time.
    # None for v1/v2, and for v3 when the list was omitted (see the reason below).
    late_events: dict[str, LateEvent] | None = None
    late_events_omitted_reason: str | None = None
    # v4: None when bad-data mode was off (and for v1-v3: they describe good data only).
    bad_data: BadData | None = None

    @property
    def event_ids(self) -> frozenset[str]:
        return frozenset(event_id for _, event_id in self.event_pairs)

    @property
    def expected_dlq_ids(self) -> frozenset[str]:
        """Logical ids the job must dead-letter (v4 with the list; else empty)."""
        if self.bad_data is None:
            return frozenset()
        return frozenset(b.event_id for b in self.bad_data.dlq_events())

    @property
    def bad_list_missing(self) -> bool:
        """True when bad events were sent but their list was left out."""
        return self.bad_data is not None and self.bad_data.bad_events is None


def _opt_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _parse_pacing(raw: Any) -> Pacing:
    mode = raw["mode"]
    if mode not in PACE_MODES:
        raise ValueError(f"unknown mode {mode!r}")
    bands: dict[str, Band] = {}
    for name in BANDS:
        b = raw["bands"][name]
        if b["expected"] not in EXPECTATIONS:
            raise ValueError(f"band {name}: unknown expected {b['expected']!r}")
        bands[name] = Band(int(b["min_delay_ms"]), int(b["max_delay_ms"]), b["expected"])
    measured = raw["measured"]
    return Pacing(
        mode=mode,
        time_scale=float(raw["time_scale"]),
        assumed_watermark_s=float(raw["assumed_watermark_s"]),
        late_fraction=float(raw["late_fraction"]),
        late_too_late_share=float(raw["late_too_late_share"]),
        bands=bands,
        future_events=_opt_int(measured["future_events"]),
        max_ahead_ms=_opt_int(measured["max_ahead_ms"]),
        max_behind_ms=_opt_int(measured["max_behind_ms"]),
    )


def _parse_late_events(
    data: dict[str, Any], counts: dict[str, int], delivered_ids: set[str]
) -> tuple[dict[str, LateEvent] | None, str | None]:
    """Return (late_events, omitted_reason) and cross-check them against the rest."""
    raw = data["late_events"]
    reason = data["late_events_omitted_reason"]
    if raw is None:
        if not reason:
            raise ValueError("late_events is null but no late_events_omitted_reason is given")
        return None, str(reason)
    if reason is not None:
        raise ValueError("late_events is present but late_events_omitted_reason is set")
    late: dict[str, LateEvent] = {}
    for entry in raw:
        item = LateEvent(
            event_id=str(entry["event_id"]),
            match_id=str(entry["match_id"]),
            delay_ms=int(entry["delay_ms"]),
            band=entry["band"],
            expected=entry["expected"],
            stream_lag_ms=int(entry["stream_lag_ms"]),
        )
        if item.band not in BANDS or item.expected not in EXPECTATIONS:
            raise ValueError(f"late event {item.event_id}: bad band or expected")
        if item.event_id in late:
            raise ValueError(f"late event {item.event_id} is listed twice")
        if item.event_id not in delivered_ids:
            raise ValueError(f"late event {item.event_id} is not a delivered event")
        late[item.event_id] = item
    listed = {name: sum(1 for e in late.values() if e.band == name) for name in BANDS}
    if listed != counts:
        raise ValueError(f"late_counts {counts} do not match the listed events {listed}")
    return late, None


def _parse_bad_data(raw: Any, delivered_ids: set[str]) -> BadData | None:
    """The v4 `bad_data` block, cross-checked (counts == list, ids delivered)."""
    if raw is None:
        return None
    entries_raw = raw["bad_events"]
    reason = raw["bad_events_omitted_reason"]
    entries: dict[str, BadEventEntry] | None = None
    if entries_raw is None:
        if not reason:
            raise ValueError("bad_events is null but no bad_events_omitted_reason is given")
    else:
        if reason is not None:
            raise ValueError("bad_events is present but bad_events_omitted_reason is set")
        entries = {}
        for e in entries_raw:
            cls = e["expected_class"]
            if cls not in BAD_CLASSES:
                raise ValueError(f"bad event {e.get('event_id')}: unknown class {cls!r}")
            key_kind = e["expected_key_kind"]
            if (cls == "dlq") != (key_kind in KEY_KINDS) or (cls == "flag" and key_kind):
                raise ValueError(f"bad event {e.get('event_id')}: key kind {key_kind!r} "
                                 f"does not fit class {cls!r}")
            item = BadEventEntry(
                event_id=str(e["event_id"]),
                sent_event_id=None if e["sent_event_id"] is None else str(e["sent_event_id"]),
                match_id=str(e["match_id"]), event_type=str(e["event_type"]),
                kind=str(e["kind"]), variant=e["variant"], field=e["field"],
                expected_class=cls,
                expected_code=str(e["expected_reason" if cls == "dlq" else "expected_flag"]),
                expected_key_kind=key_kind,
                kafka_partition=int(e["kafka_partition"]), kafka_offset=int(e["kafka_offset"]))
            if item.kafka_partition < 0 or item.kafka_offset < 0:
                raise ValueError(f"bad event {item.event_id}: negative partition or offset")
            if item.event_id in entries:
                raise ValueError(f"bad event {item.event_id} is listed twice")
            if item.event_id not in delivered_ids:
                raise ValueError(f"bad event {item.event_id} is not a delivered event")
            entries[item.event_id] = item
    bd = BadData(
        bad_fraction=float(raw["bad_fraction"]),
        enabled_kinds=tuple(str(k) for k in raw["enabled_kinds"]),
        rules_version_expected=int(raw["rules_version_expected"]),
        new_region_match=raw["new_region_match"],
        bad_counts={str(k): int(v) for k, v in raw["bad_counts"].items()},
        expected_rejections={str(k): int(v) for k, v in raw["expected_rejections"].items()},
        expected_flags={str(k): int(v) for k, v in raw["expected_flags"].items()},
        expected_by_key_kind={str(k): int(v) for k, v in raw["expected_by_key_kind"].items()},
        bad_events=entries, bad_events_omitted_reason=None if reason is None else str(reason))
    if sum(bd.expected_by_key_kind.values()) != bd.expected_dlq_count:
        raise ValueError(f"expected_by_key_kind {bd.expected_by_key_kind} does not add up to "
                         f"expected_rejections {bd.expected_dlq_count}")
    if entries is not None:
        listed: dict[str, dict[str, int]] = {"bad_counts": {}, "expected_rejections": {},
                                             "expected_flags": {}}
        for b in entries.values():
            listed["bad_counts"][b.kind] = listed["bad_counts"].get(b.kind, 0) + 1
            part = "expected_rejections" if b.expected_class == "dlq" else "expected_flags"
            listed[part][b.expected_code] = listed[part].get(b.expected_code, 0) + 1
        for name, got in listed.items():
            if got != getattr(bd, name):
                raise ValueError(f"{name} {getattr(bd, name)} do not match the listed bad "
                                 f"events {got}")
        by_key = {k: sum(1 for b in bd.dlq_events() if b.expected_key_kind == k)
                  for k in sorted(KEY_KINDS)}
        if by_key != {k: bd.expected_by_key_kind.get(k, 0) for k in sorted(KEY_KINDS)}:
            raise ValueError(f"expected_by_key_kind {bd.expected_by_key_kind} does not match "
                             f"the listed bad events {by_key}")
        seen: dict[tuple[int, int], str] = {}
        for b in entries.values():
            at = (b.kafka_partition, b.kafka_offset)
            if at in seen:
                raise ValueError(f"bad events {seen[at]} and {b.event_id} share {at}")
            seen[at] = b.event_id
    return bd


def parse_manifest(data: dict[str, Any], path: Path | str = "<memory>") -> Manifest:
    """Validate and convert a manifest dict.

    Raises:
        ManifestError: Unsupported version, missing event ids, or malformed fields.
    """
    path = Path(path)
    # v1 always wrote manifest_version; default to 1 defensively for hand-made files.
    version = data.get("manifest_version", 1)
    if version not in SUPPORTED_VERSIONS:
        raise ManifestError(
            f"{path}: manifest_version {version!r} not supported "
            f"(supported: {sorted(SUPPORTED_VERSIONS)})"
        )
    try:
        matches = data["matches"]
        run_id = str(data["run_id"])
        delivered = int(data["totals"]["events_delivered"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ManifestError(f"{path}: missing or invalid field: {exc}") from exc

    pairs: list[tuple[str, str]] = []
    for match_id, entry in matches.items():
        if "event_ids" not in entry:
            raise ManifestError(f"{path}: no event_ids (run used --no-manifest-ids)")
        pairs.extend((match_id, event_id) for event_id in entry["event_ids"])

    offsets: dict[int, OffsetRange] | None = None
    if version >= 2:
        try:
            offsets = {
                int(p): OffsetRange(int(o["min"]), int(o["max"]), int(o["count"]))
                for p, o in data["kafka_offsets"].items()
            }
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ManifestError(f"{path}: invalid kafka_offsets: {exc}") from exc

    pacing: Pacing | None = None
    late_counts: dict[str, int] | None = None
    late_events: dict[str, LateEvent] | None = None
    omitted_reason: str | None = None
    if version >= 3:
        try:
            pacing = _parse_pacing(data["pacing"])
            late_counts = {name: int(data["late_counts"][name]) for name in BANDS}
            late_events, omitted_reason = _parse_late_events(
                data, late_counts, {event_id for _, event_id in pairs}
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ManifestError(f"{path}: invalid pacing/late_events: {exc!r}") from exc

    bad_data: BadData | None = None
    if version >= 4:
        try:
            bad_data = _parse_bad_data(data["bad_data"], {event_id for _, event_id in pairs})
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ManifestError(f"{path}: invalid bad_data: {exc!r}") from exc

    return Manifest(
        path=path,
        version=version,
        run_id=run_id,
        seed=data.get("seed"),
        topic=data.get("topic"),
        events_delivered=delivered,
        event_pairs=tuple(pairs),
        kafka_offsets=offsets,
        pacing=pacing,
        late_counts=late_counts,
        late_events=late_events,
        late_events_omitted_reason=omitted_reason,
        bad_data=bad_data,
    )


def load_manifest(path: Path | str) -> Manifest:
    """Read and parse a manifest file.

    Raises:
        ManifestError: The file is not valid JSON or not a usable manifest.
        OSError: The file cannot be read.
    """
    path = Path(path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestError(f"{path}: not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ManifestError(f"{path}: top level must be a JSON object")
    return parse_manifest(data, path)
