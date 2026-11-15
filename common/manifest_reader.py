"""Read simulator run manifests of any supported version (standard library only).

Used by verification (Spark container) and the e2e orchestrator (laptop), so it
must not import pyspark or confluent_kafka.

Supported versions:
    1  event ids per match, counts, checksum. No Kafka offsets.
    2  as v1, plus `kafka_offsets` {partition: {min, max, count}}.
    3  as v2, plus `pacing`, `late_counts` and `late_events` (deliberate lateness).

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

SUPPORTED_VERSIONS = frozenset({1, 2, 3})
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

    @property
    def event_ids(self) -> frozenset[str]:
        return frozenset(event_id for _, event_id in self.event_pairs)


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
