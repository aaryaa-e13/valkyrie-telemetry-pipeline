"""Build tests/fixtures/bad_run.json: one real smoke-bad-like run, for the Spark tests.

The Spark image has neither PyYAML nor confluent-kafka, so the Spark tests cannot import
simulator.config / generator / main / producer. This builder runs on the host (unit venv)
and uses the REAL code: simulator/config.bad.yaml, the generator, a covering seed
(corrupt.find_covering_seed from 42), corrupt.corrupt_run, prepare_run and build_manifest.

Times are fixed (NOW below): backfill, so the last event_time is NOW and every message is
sent after it (send i at NOW + 1 s + i ms); ingest_time = Kafka time + 200 ms. The Spark
tests write these ingest times into bronze, so the S1 gate sees the same "now" whenever
the test runs.

tests/unit/test_bad_run_fixture.py rebuilds this and fails if the checked-in file differs
(regenerate: .venv/bin/python -m tests.fixtures.build_bad_run).
"""

from __future__ import annotations

import base64
import dataclasses
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from simulator import corrupt
from simulator.config import load_config
from simulator.events import format_event_time
from simulator.generator import generate_events
from simulator.main import prepare_run
from simulator.manifest import build_manifest
from simulator.producer import PartitionOffsets

PATH = Path(__file__).with_name("bad_run.json")
NOW = datetime(2026, 11, 28, 12, 0, 0, tzinfo=UTC)
BASE_SEED = 42
TOPIC = "game-events"
SEND_GAP = timedelta(milliseconds=1)
INGEST_LAG = timedelta(milliseconds=200)


def build() -> dict[str, Any]:
    """The fixture as a dict: {"seed", "tried", "manifest", "messages"}."""
    cfg = load_config("simulator/config.bad.yaml")
    kinds = cfg.enabled_bad_kinds()
    found = corrupt.find_covering_seed(
        lambda s: generate_events(dataclasses.replace(cfg, seed=s), NOW),
        BASE_SEED, cfg.bad_fraction, kinds)
    if found.seed is None:
        raise RuntimeError(f"no covering seed: {found.missing}")
    cfg = dataclasses.replace(cfg, seed=found.seed)
    start, plan = prepare_run(cfg, now=NOW)
    events = [p.event for p in plan]
    bad = corrupt.corrupt_run(events, cfg.seed, cfg.bad_fraction, kinds)
    messages = []
    for i, e in enumerate(events):
        kts = NOW + timedelta(seconds=1) + i * SEND_GAP
        b = bad.get(e.event_id)
        value = b.value if b is not None else e.to_json_bytes()
        messages.append({
            "offset": i, "event_id": e.event_id, "key": e.match_id,
            "event_time": format_event_time(e.event_time),
            "kafka_timestamp": format_event_time(kts),
            "ingest_time": format_event_time(kts + INGEST_LAG),
            "value_b64": base64.b64encode(value).decode("ascii"),
            "bad_kind": None if b is None else b.kind,
            "good": e.to_dict() if b is not None else None,
        })
    manifest = build_manifest(
        run_id="fixture-bad-run", config=cfg.public_dict(), start_time=start, events=events,
        delivered_ids=[e.event_id for e in events], failed_ids=[],
        partitions_by_match={e.match_id: {0} for e in events},
        offsets_by_partition={0: PartitionOffsets(0, len(events) - 1, len(events))},
        planned=plan, bad=bad,
        bad_offsets={e.event_id: (0, i) for i, e in enumerate(events) if e.event_id in bad})
    manifest["created_at"] = NOW.isoformat(timespec="milliseconds")    # the one wall-clock field
    return {"seed": cfg.seed, "tried": found.tried, "now": format_event_time(NOW),
            "manifest": manifest, "messages": messages}


def dumps(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=1, sort_keys=True) + "\n"


if __name__ == "__main__":
    data = build()
    PATH.write_text(dumps(data), encoding="utf-8")
    print(f"wrote {PATH}: seed {data['seed']} (tried {data['tried']}), "
          f"{len(data['messages'])} messages, "
          f"{sum(1 for m in data['messages'] if m['bad_kind'])} bad")
