"""Simulator CLI: generate matches, send them to Kafka, write the run manifest.

Examples:
    python -m simulator.main --config simulator/config.tiny.yaml
    python -m simulator.main --seed 7 --matches 100 --rate 500
    python -m simulator.main --config simulator/config.tiny.yaml --dry-run
    python -m simulator.main --config simulator/config.tiny.yaml --pace realtime --time-scale 10
    python -m simulator.main --config simulator/config.tiny.yaml --pace realtime \
        --time-scale 10 --late-fraction 0.3

Pace (see docs/DECISIONS.md D1.17):
    backfill (default)  send as fast as possible; the timeline is placed so its
                        last event_time is "now", so no event is from the future.
    realtime            send each event when the wall clock reaches its event_time.

Exit codes: 0 all delivered, 1 some events failed/undelivered, 2 setup error,
130 interrupted (Ctrl+C).
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from common.logging_setup import setup_logging
from simulator import corrupt
from simulator.config import PACE_REALTIME, PACES, SimulatorConfig, load_config
from simulator.events import GameEvent
from simulator.generator import generate_events
from simulator.manifest import build_manifest, find_manifests_for_seed, write_manifest
from simulator.producer import EventProducer
from simulator.schedule import PlannedSend, backfill_start, build_schedule, retime
from simulator.sender import SendStats, send_backfill, send_realtime

log = logging.getLogger("simulator")

# Realtime pace: the timeline starts this long after "now", so the first events
# are not already overdue by the time the schedule has been built.
REALTIME_LEAD = timedelta(seconds=1)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line flags (all optional; they override YAML and env)."""
    p = argparse.ArgumentParser(description="VALKYRIE game telemetry simulator")
    p.add_argument("--config", default="simulator/config.yaml", help="YAML config path")
    p.add_argument("--seed", type=int)
    p.add_argument("--matches", dest="num_matches", type=int)
    p.add_argument("--players-per-team", dest="players_per_team", type=int)
    p.add_argument("--start-time", dest="start_time",
                   help="backfill only: ISO-8601 with timezone, e.g. 2026-11-14T12:00:00Z "
                        "(default: place the timeline so that it ends now)")
    p.add_argument("--pace", choices=PACES, help="backfill (default) or realtime")
    p.add_argument("--time-scale", dest="time_scale", type=float,
                   help="speed-up of game time, e.g. 10 = ten times faster")
    p.add_argument("--late-fraction", dest="late_fraction", type=float,
                   help="share of events sent deliberately late (0..1, default 0)")
    p.add_argument("--late-too-late-share", dest="late_too_late_share", type=float,
                   help="share of the late events that are VERY late (0..1, default 0.2)")
    p.add_argument("--assumed-watermark-s", dest="assumed_watermark_s", type=float,
                   help="assumed Phase 2 watermark in seconds; delays are multiples of it")
    p.add_argument("--bad-fraction", dest="bad_fraction", type=float,
                   help="share of events that get one deliberate fault (0..1, default 0)")
    p.add_argument("--bootstrap-servers", dest="bootstrap_servers")
    p.add_argument("--topic")
    p.add_argument("--rate", dest="rate_per_s", type=float,
                   help="backfill only: events/second, 0 = unlimited")
    p.add_argument("--manifest-dir", dest="manifest_dir")
    p.add_argument("--no-manifest-ids", dest="manifest_include_event_ids",
                   action="store_const", const=False, help="omit per-event ids from manifest")
    p.add_argument("--log-level", dest="log_level")
    p.add_argument("--dry-run", action="store_true",
                   help="print events as JSON lines; do not touch Kafka or write a manifest")
    return p.parse_args(argv)


def prepare_run(
    cfg: SimulatorConfig, *, now: datetime, dry_run: bool = False
) -> tuple[datetime, list[PlannedSend]]:
    """Generate the events, place them in time and plan the send order.

    Pure apart from the `now` you pass in: same cfg + same `now` = same result.

    Where the timeline is placed:
        backfill, no start_time   so that the last event_time is `now`
        backfill, with start_time at start_time (must end at or before `now`)
        realtime                  starting one second after `now`

    Args:
        cfg: Simulator configuration.
        now: Current wall-clock time (timezone-aware).
        dry_run: Nothing will be sent, so the two start_time checks are skipped.

    Returns:
        (start_time, plan): the timeline start and the planned sends in order.

    Raises:
        ValueError: A start_time that cannot work with the chosen pace.
    """
    explicit = cfg.start_time
    ref_start = explicit or now.replace(microsecond=0)
    events = generate_events(cfg, ref_start)

    if cfg.pace == PACE_REALTIME:
        if explicit is not None and not dry_run:
            raise ValueError(
                "start_time cannot be used with pace=realtime: realtime event_times "
                "follow the wall clock (it is allowed together with --dry-run)")
        # Round up to a whole millisecond (the wire format's precision).
        now_ms = now.replace(microsecond=now.microsecond // 1000 * 1000)
        start = explicit or now_ms + timedelta(milliseconds=1) + REALTIME_LEAD
    elif explicit is not None:
        start = explicit
    else:
        start = backfill_start(events, ref_start=ref_start, now=now, time_scale=cfg.time_scale)

    events = retime(events, ref_start=ref_start, new_start=start, time_scale=cfg.time_scale)
    if cfg.pace != PACE_REALTIME and explicit is not None and not dry_run and events:
        last = events[-1].event_time
        if last > now:
            raise ValueError(
                f"start_time {explicit.isoformat()} puts the last event at {last.isoformat()}, "
                f"which is in the future (now is {now.isoformat()}). Use an earlier "
                "start_time, or omit it so the timeline ends now")
    return start, build_schedule(events, cfg.seed, cfg.lateness())


def _timing_fields(
    cfg: SimulatorConfig, start_time: datetime, plan: list[PlannedSend], now: datetime
) -> dict[str, object]:
    """Log fields describing where the timeline sits relative to the wall clock."""
    fields: dict[str, object] = {
        "pace": cfg.pace, "time_scale": cfg.time_scale, "start_time": start_time.isoformat(),
    }
    if plan:
        first = min(p.event.event_time for p in plan)
        last = max(p.event.event_time for p in plan)
        bands = Counter(p.band for p in plan if p.band is not None)
        fields.update({
            "wall_clock": now.isoformat(timespec="milliseconds"),
            "first_event_time": first.isoformat(timespec="milliseconds"),
            "last_event_time": last.isoformat(timespec="milliseconds"),
            # Positive = in the past, negative = still in the future.
            "first_event_age_s": round((now - first).total_seconds(), 3),
            "last_event_age_s": round((now - last).total_seconds(), 3),
            "late_slight": bands.get("slight", 0),
            "late_very": bands.get("very", 0),
            "last_send_in_s": round((plan[-1].send_at - now).total_seconds(), 1) + 0.0,
        })
    return fields


def bad_events(cfg: SimulatorConfig, events: list[GameEvent]) -> dict[str, corrupt.BadEvent]:
    """The run's corrupted events (bad-data mode; {} when off). The send plan is not
    touched: realtime waits use the real event_time, so a future-time event is not held
    back."""
    return corrupt.corrupt_run(events, cfg.seed, cfg.bad_fraction, cfg.enabled_bad_kinds())


def _utc_now() -> datetime:
    return datetime.now(UTC)


def run(
    cfg: SimulatorConfig, dry_run: bool = False, *, clock: Callable[[], datetime] = _utc_now
) -> int:
    """Execute one simulator run. Returns the process exit code.

    Args:
        cfg: Simulator configuration.
        dry_run: Print the events instead of sending them.
        clock: Returns the current time; replaced by a fixed clock in tests.
    """
    if dry_run:
        try:
            _, plan = prepare_run(cfg, now=clock(), dry_run=True)
        except ValueError as exc:
            log.error("invalid configuration", extra={"fields": {"error": str(exc)}})
            return 2
        bad = bad_events(cfg, [p.event for p in plan])
        for planned in plan:  # printed in planned send order (bad events as sent)
            b = bad.get(planned.event.event_id)
            value = planned.event.to_json_bytes() if b is None else b.value
            sys.stdout.write(value.decode("utf-8") + "\n")
        return 0

    earlier = find_manifests_for_seed(cfg.manifest_dir, cfg.seed)
    if earlier:
        # Not an error: sending duplicates on purpose is a valid test (D1.6).
        log.warning(
            "seed was used before: this run sends the SAME event_ids again, so every event "
            "will exist more than once in Kafka and in raw Parquet. Use a new --seed for "
            "new data",
            extra={"fields": {"seed": cfg.seed, "earlier_runs": len(earlier),
                              "earlier_manifests": [str(f) for f in earlier[-3:]]}})

    run_id = f"{clock():%Y%m%dT%H%M%SZ}_seed{cfg.seed}"
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

    # "now" is taken after the Kafka check, which can take seconds: realtime
    # pace needs the timeline to start at the real current time.
    now = clock()
    try:
        start_time, plan = prepare_run(cfg, now=now)
    except ValueError as exc:
        log.error("invalid configuration", extra={"fields": {"error": str(exc)}})
        return 2
    events = [p.event for p in plan]
    timing = _timing_fields(cfg, start_time, plan, now)
    log.info("generated events", extra={"fields": {
        "events": len(events), "matches": cfg.num_matches, "seed": cfg.seed, **timing}})

    bad = bad_events(cfg, events)
    if bad:
        log.info("bad-data mode", extra={"fields": {
            "bad_events": len(bad), "bad_fraction": cfg.bad_fraction,
            "kinds": dict(Counter(b.kind for b in bad.values()))}})
    producer.tracker.offsets_for = frozenset(bad)

    def send(event: GameEvent) -> None:
        b = bad.get(event.event_id)
        if b is None:
            producer.send(event)
        else:
            producer.send(event, b.value)

    stats = SendStats()
    interrupted = False
    try:
        if cfg.pace == PACE_REALTIME:
            send_realtime(plan, send, wait=producer.poll, stats=stats)
        else:
            send_backfill(plan, send, rate_per_s=cfg.rate_per_s, stats=stats)
    except KeyboardInterrupt:
        interrupted = True
        log.warning("interrupted; flushing what was already queued")
    remaining = producer.flush(cfg.flush_timeout_s)
    if stats.future_events:
        log.warning("events were sent before their event_time", extra={"fields": {
            "future_events": stats.future_events, "max_ahead_ms": stats.max_ahead_ms}})

    tracker = producer.tracker
    manifest = build_manifest(
        run_id=run_id,
        config=cfg.public_dict(),
        start_time=start_time,
        events=events,
        delivered_ids=tracker.delivered_ids,
        failed_ids=tracker.failed_ids,
        partitions_by_match=tracker.partitions_by_match,
        offsets_by_partition=tracker.offsets_by_partition,
        include_event_ids=cfg.manifest_include_event_ids,
        interrupted=interrupted,
        planned=plan,
        stats=stats,
        bad=bad if cfg.bad_fraction > 0 else None,
        bad_offsets=tracker.message_offsets,
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
        "pace": cfg.pace,
        "sent": stats.sent,
        "future_events": stats.future_events,
        "max_ahead_ms": stats.max_ahead_ms,
        "max_behind_ms": stats.max_behind_ms,
        "bad_events": len(bad),
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
