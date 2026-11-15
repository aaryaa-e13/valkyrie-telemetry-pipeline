"""Independent timing check: read a run back from Kafka and compare timestamps.

For every message of one simulator run this compares

    CreateTime   when the Kafka client stamped the message at send time
    event_time   when the event "happened" (a field inside the message)

and reports whether any event was sent BEFORE its own event_time ("ahead",
an event from the future: the bug recorded in DECISIONS D1.7c). It does not
trust the simulator's own measurements; it only uses the manifest to know
which offsets and event_ids belong to the run.

Usage:
    python -m e2e.timing_check --manifest manifests/run_X.json
    make timing-check MANIFEST=manifests/run_X.json

Output: exactly one JSON line on stdout. Fields:
    ahead            events with event_time - CreateTime > tolerance_ms
    max_ahead_ms     largest event_time - CreateTime (<= 0: nothing from the future)
    max_behind_ms    largest CreateTime - event_time (how old the oldest event was)
    out_of_order     events that arrived after a newer event (higher match_seq)
                     of the same match; expected > 0 only with late events
    result           PASS if ahead == 0 and every event of the run was read

Why the tolerance is 1 ms: both timestamps have whole-millisecond precision,
so rounding alone can make them differ by up to 1 ms.

Exit codes: 0 PASS, 1 FAIL, 2 setup problem (unusable manifest, no offsets in
a v1 manifest, Kafka unreachable, topic not using CreateTime).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from common.manifest_reader import Manifest, ManifestError, load_manifest

EXIT_PASS, EXIT_FAIL, EXIT_SETUP = 0, 1, 2
DEFAULT_TOLERANCE_MS = 1


class SetupError(RuntimeError):
    """The check could not be performed (as opposed to: performed and failed)."""


@dataclass(frozen=True)
class Observed:
    """One message of the run as read from Kafka."""

    partition: int
    offset: int
    event_id: str
    match_id: str
    match_seq: int
    event_time_ms: int
    create_time_ms: int


def parse_event_time_ms(text: str) -> int:
    """Convert the wire format (e.g. 2026-11-15T10:12:33.146Z) to epoch milliseconds."""
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"event_time without timezone: {text!r}")
    return round(dt.timestamp() * 1000)


def summarize(
    rows: Iterable[Observed], expected_ids: frozenset[str], tolerance_ms: int = DEFAULT_TOLERANCE_MS
) -> dict[str, Any]:
    """Compute the timing report from the messages that were read. Pure.

    Args:
        rows: Messages in the order they were read. Order only matters within a
            partition, where it must be offset order.
        expected_ids: The run's delivered event_ids (from the manifest).
        tolerance_ms: An event counts as "ahead" only beyond this.
    """
    first_by_id: dict[str, Observed] = {}
    repeats = 0
    for row in sorted(rows, key=lambda r: (r.partition, r.offset)):
        if row.event_id not in expected_ids:
            continue
        if row.event_id in first_by_id:
            repeats += 1
            continue
        first_by_id[row.event_id] = row

    ahead = 0
    max_ahead: int | None = None
    max_behind: int | None = None
    out_of_order = 0
    highest_seq: dict[tuple[int, str], int] = {}
    for row in first_by_id.values():  # (partition, offset) order
        ahead_ms = row.event_time_ms - row.create_time_ms
        if ahead_ms > tolerance_ms:
            ahead += 1
        max_ahead = ahead_ms if max_ahead is None else max(max_ahead, ahead_ms)
        max_behind = -ahead_ms if max_behind is None else max(max_behind, -ahead_ms)
        key = (row.partition, row.match_id)
        if row.match_seq < highest_seq.get(key, 0):
            out_of_order += 1
        highest_seq[key] = max(highest_seq.get(key, 0), row.match_seq)

    complete = len(first_by_id) == len(expected_ids)
    reasons: list[str] = []
    if ahead:
        reasons.append(f"{ahead} event(s) sent before their event_time")
    if not complete:
        reasons.append(f"read {len(first_by_id)} of {len(expected_ids)} events")
    return {
        "events_expected": len(expected_ids),
        "events_read": len(first_by_id),
        "repeated_in_range": repeats,
        "tolerance_ms": tolerance_ms,
        "ahead": ahead,
        "max_ahead_ms": max_ahead,
        "max_behind_ms": max_behind,
        "out_of_order": out_of_order,
        "reasons": reasons,
        "result": "PASS" if not reasons else "FAIL",
    }


def read_run(manifest: Manifest, bootstrap_servers: str, timeout_s: float) -> list[Observed]:
    """Read the run's offset ranges from Kafka (no consumer group, nothing committed).

    Raises:
        SetupError: Kafka problems, or the topic does not use CreateTime.
    """
    # Imported here so `summarize` and its unit tests need no Kafka client.
    from confluent_kafka import (
        TIMESTAMP_CREATE_TIME,
        Consumer,
        KafkaError,
        KafkaException,
        TopicPartition,
    )

    assert manifest.kafka_offsets is not None and manifest.topic is not None
    last = {p: r.max for p, r in manifest.kafka_offsets.items()}
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "valkyrie-timing-check",   # required by the client; never committed
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })
    rows: list[Observed] = []
    done: set[int] = set()
    deadline = time.monotonic() + timeout_s
    try:
        consumer.assign([TopicPartition(manifest.topic, p, r.min)
                         for p, r in manifest.kafka_offsets.items()])
        while len(done) < len(last) and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:   # nothing more there
                    done.add(msg.partition())
                    continue
                raise SetupError(f"kafka error: {msg.error()}")
            partition, offset = msg.partition(), msg.offset()
            if partition in done:
                continue
            if offset >= last[partition]:
                done.add(partition)
            if offset > last[partition]:
                continue
            ts_type, ts_ms = msg.timestamp()
            if ts_type != TIMESTAMP_CREATE_TIME:
                raise SetupError("topic does not use CreateTime timestamps; cannot compare")
            try:
                event = json.loads(msg.value())
                rows.append(Observed(
                    partition=partition, offset=offset, event_id=str(event["event_id"]),
                    match_id=str(event["match_id"]), match_seq=int(event["match_seq"]),
                    event_time_ms=parse_event_time_ms(event["event_time"]),
                    create_time_ms=int(ts_ms),
                ))
            except (ValueError, KeyError, TypeError):
                continue                            # not one of our events
    except KafkaException as exc:
        raise SetupError(f"cannot read from Kafka at {bootstrap_servers}: {exc}") from exc
    finally:
        consumer.close()
    if len(done) < len(last):
        raise SetupError(
            f"timed out after {timeout_s:.0f}s reading from Kafka at {bootstrap_servers} "
            f"(finished partitions {sorted(done)} of {sorted(last)})")
    return rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare Kafka CreateTime with event_time for a run")
    p.add_argument("--manifest", required=True, help="path to the run manifest (v2 or v3)")
    p.add_argument("--bootstrap-servers",
                   default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--tolerance-ms", type=int, default=DEFAULT_TOLERANCE_MS)
    p.add_argument("--timeout", type=float, default=30.0, help="seconds to wait for Kafka")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Prints one JSON line and returns the exit code."""
    args = parse_args(argv)
    report: dict[str, Any] = {"check": "timing", "manifest": args.manifest}
    try:
        manifest = load_manifest(args.manifest)
        if manifest.kafka_offsets is None or not manifest.topic:
            raise SetupError(f"manifest v{manifest.version} has no kafka_offsets/topic")
        report.update({
            "manifest_version": manifest.version, "seed": manifest.seed,
            "pace": manifest.pacing.mode if manifest.pacing else None,
            "late_listed": len(manifest.late_events) if manifest.late_events is not None else None,
        })
        rows = read_run(manifest, args.bootstrap_servers, args.timeout)
    except (ManifestError, OSError, SetupError) as exc:
        report.update({"result": "ERROR", "error": str(exc), "exit_code": EXIT_SETUP})
        print(json.dumps(report))
        return EXIT_SETUP
    report.update(summarize(rows, manifest.event_ids, args.tolerance_ms))
    report["exit_code"] = EXIT_PASS if report["result"] == "PASS" else EXIT_FAIL
    print(json.dumps(report))
    return int(report["exit_code"])


if __name__ == "__main__":
    sys.exit(main())
