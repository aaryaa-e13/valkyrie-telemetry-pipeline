"""Read simulator run manifests of any supported version (standard library only).

Used by verification (Spark container) and the e2e orchestrator (laptop), so it
must not import pyspark or confluent_kafka.

Supported versions:
    1  event ids per match, counts, checksum. No Kafka offsets.
    2  as v1, plus `kafka_offsets` {partition: {min, max, count}}.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUPPORTED_VERSIONS = frozenset({1, 2})


class ManifestError(ValueError):
    """The manifest cannot be used (unknown version, missing ids, bad structure)."""


@dataclass(frozen=True)
class OffsetRange:
    """Offsets of a run's acknowledged messages in one partition (inclusive)."""

    min: int
    max: int
    count: int


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

    @property
    def event_ids(self) -> frozenset[str]:
        return frozenset(event_id for _, event_id in self.event_pairs)


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

    return Manifest(
        path=path,
        version=version,
        run_id=run_id,
        seed=data.get("seed"),
        topic=data.get("topic"),
        events_delivered=delivered,
        event_pairs=tuple(pairs),
        kafka_offsets=offsets,
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
