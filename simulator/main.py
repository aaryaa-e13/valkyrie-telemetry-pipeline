"""Simulator CLI: generate matches, send them to Kafka, write the run manifest.

Examples:
    python -m simulator.main --config simulator/config.tiny.yaml
    python -m simulator.main --seed 7 --matches 100 --rate 500
    python -m simulator.main --config simulator/config.tiny.yaml --dry-run

Exit codes: 0 all delivered, 1 some events failed/undelivered, 2 setup error,
130 interrupted (Ctrl+C).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import UTC, datetime

from common.logging_setup import setup_logging
from simulator.config import SimulatorConfig, load_config
from simulator.generator import generate_events
from simulator.manifest import build_manifest, write_manifest
from simulator.producer import EventProducer

log = logging.getLogger("simulator")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line flags (all optional; they override YAML and env)."""
    p = argparse.ArgumentParser(description="VALKYRIE game telemetry simulator")
    p.add_argument("--config", default="simulator/config.yaml", help="YAML config path")
    p.add_argument("--seed", type=int)
    p.add_argument("--matches", dest="num_matches", type=int)
    p.add_argument("--players-per-team", dest="players_per_team", type=int)
    p.add_argument("--start-time", dest="start_time",
                   help="ISO-8601 with timezone, e.g. 2026-11-14T12:00:00Z (default: now)")
    p.add_argument("--bootstrap-servers", dest="bootstrap_servers")
    p.add_argument("--topic")
    p.add_argument("--rate", dest="rate_per_s", type=float, help="events/second, 0 = unlimited")
    p.add_argument("--manifest-dir", dest="manifest_dir")
    p.add_argument("--no-manifest-ids", dest="manifest_include_event_ids",
                   action="store_const", const=False, help="omit per-event ids from manifest")
    p.add_argument("--log-level", dest="log_level")
    p.add_argument("--dry-run", action="store_true",
                   help="print events as JSON lines; do not touch Kafka or write a manifest")
    return p.parse_args(argv)


def _send_all(producer: EventProducer, events: list, rate_per_s: float) -> int:
    """Send events, optionally rate-limited. Returns how many were queued."""
    started = time.monotonic()
    for i, event in enumerate(events):
        if rate_per_s > 0:
            delay = started + i / rate_per_s - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        producer.send(event)
    return len(events)


def run(cfg: SimulatorConfig, dry_run: bool = False) -> int:
    """Execute one simulator run. Returns the process exit code."""
    start_time = cfg.start_time or datetime.now(UTC).replace(microsecond=0)
    events = generate_events(cfg, start_time)
    log.info("generated events", extra={"fields": {
        "events": len(events), "matches": cfg.num_matches, "seed": cfg.seed,
        "start_time": start_time.isoformat(),
    }})

    if dry_run:
        for e in events:
            sys.stdout.write(e.to_json_bytes().decode("utf-8") + "\n")
        return 0

    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}_seed{cfg.seed}"
    producer = EventProducer(cfg.bootstrap_servers, cfg.topic, cfg.client_id)
    try:
        partitions = producer.check_topic()
    except RuntimeError as exc:
        log.error("kafka preflight failed", extra={"fields": {
            "error": str(exc), "bootstrap_servers": cfg.bootstrap_servers}})
        return 2
    log.info("kafka ready", extra={"fields": {
        "bootstrap_servers": cfg.bootstrap_servers, "topic": cfg.topic,
        "partitions": partitions}})

    interrupted = False
    try:
        _send_all(producer, events, cfg.rate_per_s)
    except KeyboardInterrupt:
        interrupted = True
        log.warning("interrupted; flushing what was already queued")
    remaining = producer.flush(cfg.flush_timeout_s)

    tracker = producer.tracker
    manifest = build_manifest(
        run_id=run_id,
        config=cfg.public_dict(),
        start_time=start_time,
        events=events,
        delivered_ids=tracker.delivered_ids,
        failed_ids=tracker.failed_ids,
        partitions_by_match=tracker.partitions_by_match,
        include_event_ids=cfg.manifest_include_event_ids,
        interrupted=interrupted,
    )
    path = write_manifest(manifest, cfg.manifest_dir)

    summary = {
        "run_id": run_id,
        "generated": len(events),
        "delivered": len(tracker.delivered_ids),
        "failed": len(tracker.failed_ids),
        "undelivered_after_flush": remaining,
        "manifest": str(path),
        "partition_by_match": {m: sorted(p) for m, p in tracker.partitions_by_match.items()},
    }
    if interrupted:
        log.warning("run interrupted", extra={"fields": summary})
        return 130
    if tracker.failed_ids or remaining:
        log.error("run finished with delivery failures", extra={"fields": summary})
        return 1
    log.info("run complete", extra={"fields": summary})
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    overrides = {k: v for k, v in vars(args).items() if k not in {"config", "dry_run"}}
    try:
        cfg = load_config(args.config, overrides=overrides)
    except (OSError, ValueError, TypeError) as exc:
        setup_logging("INFO", component="simulator")
        log.error("invalid configuration", extra={"fields": {"error": str(exc)}})
        return 2
    setup_logging(cfg.log_level, component="simulator")
    return run(cfg, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
