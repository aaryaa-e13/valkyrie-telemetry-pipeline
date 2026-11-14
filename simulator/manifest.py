"""Run manifest: the ground truth of what a simulator run put into Kafka.

Phase 4 chaos tests compare the pipeline's output against this file to prove
"unique events in == rows out". Only events the broker *acknowledged* count as
delivered; failed events are listed separately.

The checksum is order-independent (sha256 over sorted ids), so it can be
compared with ids read back from Parquet/Delta in any order.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from simulator.events import GameEvent

MANIFEST_VERSION = 1
CHECKSUM_ALGORITHM = "sha256(sorted event_ids joined by newline)"


def event_ids_checksum(event_ids: Iterable[str]) -> str:
    """Order-independent checksum of a set of event ids."""
    joined = "\n".join(sorted(set(event_ids)))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def build_manifest(
    *,
    run_id: str,
    config: Mapping[str, Any],
    start_time: datetime,
    events: list[GameEvent],
    delivered_ids: Iterable[str],
    failed_ids: Iterable[str],
    partitions_by_match: Mapping[str, set[int]],
    include_event_ids: bool = True,
    interrupted: bool = False,
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
        include_event_ids: If False, omit per-match id lists (smaller file for big runs).
        interrupted: True if the run was stopped early.
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
