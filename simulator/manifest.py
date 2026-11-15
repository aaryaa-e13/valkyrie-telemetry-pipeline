"""Run manifest: the ground truth of what a simulator run put into Kafka.

Phase 4 chaos tests compare the pipeline's output against this file to prove
"unique events in == rows out". Only events the broker *acknowledged* count as
delivered; failed events are listed separately.

The checksum is order-independent (sha256 over sorted ids), so it can be
compared with ids read back from Parquet/Delta in any order.

Versions:
    1  event ids, counts, checksum.
    2  adds `kafka_offsets`: per partition, the min/max offset and count of this
       run's acknowledged messages. A consumer has fully read the run once its
       committed (exclusive) end offset is > max for every listed partition.
    3  adds pacing and deliberate lateness (Phase 1 slice e):
       `pacing`       mode, time_scale, assumed watermark, the delay bands and
                      what the send loop measured (max_ahead_ms, max_behind_ms).
       `late_counts`  delivered late events per band. Always written.
       `late_events`  one entry per delivered late event, in send order. An
                      event without an entry was sent on time. The list is null
                      when it was left out; `late_events_omitted_reason` says why:
                      "no_manifest_ids" (--no-manifest-ids) or
                      "too_many_late_events" (more than MAX_LATE_EVENTS_LISTED).
       Late events are still ordinary delivered events: they stay in
       `event_ids`, the totals and the checksum. v3 only annotates them.
Readers must accept all versions (see common/manifest_reader.py).
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from simulator.events import GameEvent
from simulator.schedule import (
    BAND_SLIGHT,
    BAND_VERY,
    EXPECT_KEEP,
    EXPECT_MAY_DROP,
    SLIGHT_BAND,
    VERY_BAND,
    PlannedSend,
)
from simulator.sender import SendStats

MANIFEST_VERSION = 3
# Above this many late events the per-event list is left out (about 20 MB of JSON).
MAX_LATE_EVENTS_LISTED = 100_000
OMITTED_NO_IDS = "no_manifest_ids"
OMITTED_TOO_MANY = "too_many_late_events"
CHECKSUM_ALGORITHM = "sha256(sorted event_ids joined by newline)"


def event_ids_checksum(event_ids: Iterable[str]) -> str:
    """Order-independent checksum of a set of event ids."""
    joined = "\n".join(sorted(set(event_ids)))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _band_definition(
    multiples: tuple[float, float], watermark_s: float, expected: str
) -> dict[str, Any]:
    lo, hi = multiples
    return {
        "min_x_watermark": lo,
        "max_x_watermark": hi,
        "min_delay_ms": round(lo * watermark_s * 1000),
        "max_delay_ms": round(hi * watermark_s * 1000),
        "expected": expected,
    }


def _pacing_section(config: Mapping[str, Any], stats: SendStats | None) -> dict[str, Any]:
    """The `pacing` block: settings as used, band definitions, measurements."""
    watermark_s = float(config.get("assumed_watermark_s", 30.0))
    return {
        "mode": config.get("pace", "backfill"),
        "time_scale": float(config.get("time_scale", 1.0)),
        "assumed_watermark_s": watermark_s,
        "late_fraction": float(config.get("late_fraction", 0.0)),
        "late_too_late_share": float(config.get("late_too_late_share", 0.2)),
        "bands": {
            BAND_SLIGHT: _band_definition(SLIGHT_BAND, watermark_s, EXPECT_KEEP),
            BAND_VERY: _band_definition(VERY_BAND, watermark_s, EXPECT_MAY_DROP),
        },
        # Measured by the send loop; null when nothing was measured.
        "measured": {
            "sent": stats.sent if stats else None,
            "future_events": stats.future_events if stats else None,
            "max_ahead_ms": stats.max_ahead_ms if stats else None,
            "max_behind_ms": stats.max_behind_ms if stats else None,
        },
    }


def _late_sections(
    planned: Sequence[PlannedSend] | None, delivered: set[str], include_event_ids: bool
) -> dict[str, Any]:
    """`late_counts`, `late_events` and `late_events_omitted_reason`.

    Only delivered events are counted and listed, so the late list is always a
    subset of the manifest's delivered event ids.
    """
    late = [p for p in (planned or []) if p.band is not None and p.event.event_id in delivered]
    counts = {
        BAND_SLIGHT: sum(1 for p in late if p.band == BAND_SLIGHT),
        BAND_VERY: sum(1 for p in late if p.band == BAND_VERY),
    }
    reason: str | None = None
    if not include_event_ids:
        reason = OMITTED_NO_IDS
    elif len(late) > MAX_LATE_EVENTS_LISTED:
        reason = OMITTED_TOO_MANY
    entries: list[dict[str, Any]] | None = None
    if reason is None:
        entries = [
            {
                "event_id": p.event.event_id,
                "match_id": p.event.match_id,
                "delay_ms": p.delay_ms,
                "band": p.band,
                "expected": p.expected,
                "stream_lag_ms": p.stream_lag_ms,
            }
            for p in late  # planned send order
        ]
    return {
        "late_counts": counts,
        "late_events_omitted_reason": reason,
        "late_events": entries,
    }


def build_manifest(
    *,
    run_id: str,
    config: Mapping[str, Any],
    start_time: datetime,
    events: list[GameEvent],
    delivered_ids: Iterable[str],
    failed_ids: Iterable[str],
    partitions_by_match: Mapping[str, set[int]],
    offsets_by_partition: Mapping[int, Any] | None = None,
    include_event_ids: bool = True,
    interrupted: bool = False,
    planned: Sequence[PlannedSend] | None = None,
    stats: SendStats | None = None,
) -> dict[str, Any]:
    """Assemble the manifest dict.

    Args:
        run_id: Unique id of this run.
        config: Effective simulator settings.
        start_time: First match start used for generation.
        events: All generated events.
        delivered_ids: Ids acknowledged by the broker.
        failed_ids: Ids whose delivery failed.
        partitions_by_match: Kafka partition(s) each match landed on.
        offsets_by_partition: partition -> object with `min`, `max`, `count` of the
            acknowledged offsets (see simulator.producer.PartitionOffsets).
        include_event_ids: If False, omit per-match id lists (smaller file for big runs).
        interrupted: True if the run was stopped early.
        planned: The send plan (simulator.schedule.build_schedule). Supplies the
            lateness annotations; None means every event was sent on time.
        stats: What the send loop measured; None writes nulls.
    """
    delivered = set(delivered_ids)
    failed = sorted(set(failed_ids))
    by_match: dict[str, list[GameEvent]] = defaultdict(list)
    for e in events:
        by_match[e.match_id].append(e)

    matches: dict[str, Any] = {}
    for match_id in sorted(by_match):
        match_events = sorted(by_match[match_id], key=lambda e: e.match_seq)
        delivered_in_match = [e.event_id for e in match_events if e.event_id in delivered]
        entry: dict[str, Any] = {
            "events_generated": len(match_events),
            "events_delivered": len(delivered_in_match),
            "max_match_seq": match_events[-1].match_seq,
            "partitions": sorted(partitions_by_match.get(match_id, set())),
        }
        if include_event_ids:
            entry["event_ids"] = delivered_in_match  # in match_seq order
        matches[match_id] = entry

    return {
        "manifest_version": MANIFEST_VERSION,
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "interrupted": interrupted,
        "seed": config.get("seed"),
        "start_time": start_time.isoformat(),
        "topic": config.get("topic"),
        "config": dict(config),
        "totals": {
            "matches": len(by_match),
            "events_generated": len(events),
            "events_delivered": len(delivered),
            "events_failed": len(failed),
        },
        "delivered_checksum": {
            "algorithm": CHECKSUM_ALGORITHM,
            "value": event_ids_checksum(delivered),
        },
        # JSON object keys must be strings, so partitions are written as "0", "1", ...
        "kafka_offsets": {
            str(p): {"min": o.min, "max": o.max, "count": o.count}
            for p, o in sorted((offsets_by_partition or {}).items())
        },
        "pacing": _pacing_section(config, stats),
        **_late_sections(planned, delivered, include_event_ids),
        "matches": matches,
        "failed_event_ids": failed,
    }


def write_manifest(manifest: Mapping[str, Any], directory: str | Path) -> Path:
    """Write the manifest atomically (temp file + rename) and return its path.

    Atomic write means a crash never leaves a half-written manifest behind.
    """
    out_dir = Path(directory)
    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / f"run_{manifest['run_id']}.json"
    fd, tmp_name = tempfile.mkstemp(dir=out_dir, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        # mkstemp creates 0600 files; make the manifest readable by other
        # containers/users (e.g. Spark or test jobs) like a normal file.
        os.chmod(tmp_name, 0o644)
        os.replace(tmp_name, final_path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise
    return final_path
