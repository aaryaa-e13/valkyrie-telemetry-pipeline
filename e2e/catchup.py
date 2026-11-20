"""Pure logic for "has Spark processed this run?" (no Docker, no I/O).

Everything here is a plain function over manifests, status files and log
lines, so it is unit-tested on the laptop (tests/unit/test_catchup.py).

Key rules (see docs/DECISIONS.md D1.15 and D1.16):

* Contiguity: a run's offsets in one partition must be exactly
  min..max with count == max - min + 1. Otherwise another producer wrote into
  that range while the run was being sent, and "caught up" or "verified"
  would be ambiguous.
* Caught up: for every partition the run wrote to, the committed (exclusive)
  end offset is > max.
* Freshness: the evidence must come from a batch committed AFTER the run was
  sent (higher batch_id than the baseline taken before the run, or a new
  query_id), for the manifest's topic. Otherwise an old status file could
  look caught up, e.g. after the topic was recreated and offsets restarted.
* Spark 4.1.4 does NOT post progress events while idle (measured, D1.16),
  so a stale `updated_at` means nothing by itself. It only matters while
  offsets are pending.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from common.manifest_reader import OffsetRange

# --- contiguity ------------------------------------------------------------


def check_contiguity(offsets: Mapping[int, OffsetRange]) -> list[str]:
    """Return one human-readable problem per non-contiguous partition (empty = OK)."""
    problems: list[str] = []
    for partition in sorted(offsets):
        r = offsets[partition]
        span = r.max - r.min + 1
        if r.count == span:
            continue
        if r.count < span:
            problems.append(
                f"partition {partition}: run wrote {r.count} messages but its offsets span "
                f"{r.min}..{r.max} ({span} slots) -> {span - r.count} offsets in that range "
                "came from another producer (another writer was active during the run)"
            )
        else:
            problems.append(
                f"partition {partition}: manifest claims {r.count} messages in only "
                f"{span} offsets ({r.min}..{r.max}) -> manifest is inconsistent (simulator bug?)"
            )
    return problems


# --- status snapshots --------------------------------------------------------


@dataclass(frozen=True)
class StatusSnapshot:
    """The parts of the status file the orchestrator needs."""

    query_id: str
    batch_id: int
    updated_at: datetime | None
    # topic -> partition -> committed end offset (exclusive)
    end_offsets: dict[str, dict[int, int]]


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp ending in Z or +00:00. None if missing/invalid."""
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def snapshot_from_status(status: Mapping[str, Any] | None) -> StatusSnapshot | None:
    """Convert a status dict (from streaming.status.read_status) to a snapshot.

    Returns None for a missing or structurally unusable status.
    """
    if not status or status.get("source", "kafka") != "kafka":
        return None
    try:
        last = status["last_batch"]
        raw_end = last.get("end_offset") or {}
        end = {
            str(topic): {int(p): int(o) for p, o in parts.items()}
            for topic, parts in raw_end.items()
        }
        return StatusSnapshot(
            query_id=str(status["query_id"]),
            batch_id=int(last["batch_id"]),
            updated_at=parse_timestamp(status.get("updated_at")),
            end_offsets=end,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def pre_run_conflicts(
    baseline: StatusSnapshot | None, offsets: Mapping[int, OffsetRange], topic: str
) -> list[str]:
    """Problems that prove the status file does not describe this topic's history.

    Before the run was sent, Spark cannot already have committed past the run's
    first offset. If the baseline status claims it did, the topic was recreated
    (offsets restarted at 0) or the status file belongs to another topic/cluster.
    """
    if baseline is None:
        return []
    committed = baseline.end_offsets.get(topic, {})
    problems = []
    for partition in sorted(offsets):
        end = committed.get(partition)
        if end is not None and end > offsets[partition].min:
            problems.append(
                f"partition {partition}: status file (taken BEFORE the run was sent) already "
                f"has committed end offset {end}, but the run starts at offset "
                f"{offsets[partition].min}. The topic was probably recreated (offsets "
                "restarted) while the Spark checkpoint/status kept the old positions."
            )
    return problems


# --- lag and caught-up ---------------------------------------------------------


@dataclass(frozen=True)
class PartitionLag:
    """Progress of one partition of the run."""

    partition: int
    run: OffsetRange
    committed_end: int | None   # None: no committed batch for this topic/partition
    remaining: int              # run offsets not yet committed

    @property
    def done(self) -> bool:
        return self.remaining == 0


def compute_lag(
    offsets: Mapping[int, OffsetRange], snapshot: StatusSnapshot | None, topic: str
) -> list[PartitionLag]:
    """Per-partition remaining offsets of the run, based on `snapshot`."""
    committed = snapshot.end_offsets.get(topic, {}) if snapshot else {}
    rows = []
    for partition in sorted(offsets):
        r = offsets[partition]
        end = committed.get(partition)
        # Offsets min..max; everything below `end` (exclusive) is committed.
        done_up_to = min(max(end if end is not None else r.min, r.min), r.max + 1)
        rows.append(PartitionLag(partition, r, end, r.max + 1 - done_up_to))
    return rows


@dataclass(frozen=True)
class CatchUp:
    """Result of one catch-up check."""

    caught_up: bool
    fresh: bool
    reasons: tuple[str, ...]   # why not caught up (or notes when caught up)
    lag: tuple[PartitionLag, ...]


def check_caught_up(
    baseline: StatusSnapshot | None,
    current: StatusSnapshot | None,
    offsets: Mapping[int, OffsetRange],
    topic: str,
) -> CatchUp:
    """Decide whether `current` proves the run is committed.

    Requires all three: fresh evidence (new batch or new query since the
    baseline), the manifest's topic in the status, and end > max everywhere.
    """
    reasons: list[str] = []
    lag = tuple(compute_lag(offsets, current, topic))
    if current is None:
        return CatchUp(False, False, ("status file missing or unreadable",), lag)

    if baseline is None:
        fresh = True
    elif current.query_id != baseline.query_id:
        fresh = True
        reasons.append(
            f"query_id changed {baseline.query_id} -> {current.query_id} "
            "(checkpoint was reset; the new query re-reads the topic)"
        )
    else:
        fresh = current.batch_id > baseline.batch_id
        if not fresh:
            reasons.append(
                f"no batch committed since the run was sent (last batch_id is still "
                f"{current.batch_id})"
            )
    if topic not in current.end_offsets:
        reasons.append(
            f"status file has offsets for {sorted(current.end_offsets)}, not for topic {topic!r}"
        )
    pending = [row for row in lag if not row.done]
    if pending:
        reasons.append(f"{sum(r.remaining for r in pending)} run offsets not committed yet "
                       f"in partitions {[r.partition for r in pending]}")
    caught_up = fresh and topic in current.end_offsets and not pending
    return CatchUp(caught_up, fresh, tuple(reasons), lag)


# --- silver (Delta source) catch-up -----------------------------------------------


@dataclass(frozen=True)
class DeltaSnapshot:
    """Last committed batch of a query that reads a Delta table (silver)."""

    query_id: str
    batch_id: int
    updated_at: datetime | None
    reservoir_id: str
    reservoir_version: int
    index: int
    is_starting_version: bool


def delta_snapshot_from_status(status: Mapping[str, Any] | None) -> DeltaSnapshot | None:
    """Snapshot of a "delta"-source status file; None if missing or not a Delta source."""
    if not status or status.get("source") != "delta":
        return None
    try:
        last = status["last_batch"]
        end = last["end_offset"]
        return DeltaSnapshot(
            query_id=str(status["query_id"]), batch_id=int(last["batch_id"]),
            updated_at=parse_timestamp(status.get("updated_at")),
            reservoir_id=str(end["reservoir_id"]),
            reservoir_version=int(end["reservoir_version"]), index=int(end["index"]),
            is_starting_version=bool(end.get("is_starting_version", False)),
        )
    except (KeyError, TypeError, ValueError):
        return None


@dataclass(frozen=True)
class SilverCatchUp:
    """Result of one silver catch-up check."""

    caught_up: bool
    reasons: tuple[str, ...]


def check_silver_caught_up(
    current: DeltaSnapshot | None, bronze_table_id: str | None, bronze_version: int
) -> SilverCatchUp:
    """Has silver consumed bronze up to and including `bronze_version`?

    Rule (DECISIONS D2.3, spike Q4): a committed Delta source end offset
    {reservoir_version: V, index: i, is_starting_version: false} means every
    bronze version < V is fully consumed. So silver has consumed
    `bronze_version` iff V > bronze_version. Conservative on purpose: an offset
    in the middle of that version (V == bronze_version, i >= 0) waits for the
    next batch. A starting-version offset (silver's first snapshot read, still
    in progress) never counts. The reservoir id must be the bronze table's id
    (skipped with a note when the id is unknown after log cleanup).
    """
    if current is None:
        return SilverCatchUp(False, ("silver status file missing, unreadable or not a "
                                     "Delta-source status",))
    reasons: list[str] = []
    ok = True
    if bronze_table_id is None:
        reasons.append("bronze table id unknown (log cleaned up); reservoir id not checked")
    elif current.reservoir_id != bronze_table_id:
        ok = False
        reasons.append(f"silver reads table {current.reservoir_id}, bronze is "
                       f"{bronze_table_id} (bronze recreated?)")
    if current.is_starting_version:
        ok = False
        reasons.append(f"silver is still reading its initial snapshot of bronze version "
                       f"{current.reservoir_version} (batch {current.batch_id})")
    elif current.reservoir_version <= bronze_version:
        ok = False
        reasons.append(f"silver has consumed bronze versions < {current.reservoir_version} "
                       f"(batch {current.batch_id}); waiting for version {bronze_version}")
    return SilverCatchUp(ok, tuple(reasons))


# --- container logs -------------------------------------------------------------


@dataclass(frozen=True)
class LogFacts:
    """Facts extracted from the streaming job's JSON log lines."""

    batch_ids: tuple[int, ...]          # batch ids from "batch progress" lines
    status_write_warnings: int          # "could not write status file"
    query_started: bool
    errors: tuple[str, ...]             # msg of ERROR lines
    started_run_ids: tuple[str, ...] = ()   # run_id of each "query started" line

    @property
    def max_batch_id(self) -> int | None:
        return max(self.batch_ids) if self.batch_ids else None


def parse_log_lines(lines: Iterable[str], query: str = "bronze") -> LogFacts:
    """Extract facts about one query from container log lines.

    Since slice 3 the container runs bronze and silver; progress and start lines
    carry `"query": <name>` and only `query`'s lines count. Lines without that
    field (written before slice 3) count as bronze. ERROR lines and status
    write warnings count for every query. Non-JSON lines are ignored.
    """
    batch_ids: list[int] = []
    warnings = 0
    started = False
    errors: list[str] = []
    run_ids: list[str] = []
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        msg = rec.get("msg")
        mine = rec.get("query", "bronze") == query
        if msg == "batch progress" and mine and isinstance(rec.get("batch_id"), int):
            batch_ids.append(rec["batch_id"])
        elif msg == "could not write status file":
            warnings += 1
        elif msg == "query started" and mine:
            started = True
            if rec.get("run_id"):
                run_ids.append(str(rec["run_id"]))
        if rec.get("level") == "ERROR":
            errors.append(str(msg))
    return LogFacts(tuple(batch_ids), warnings, started, tuple(errors), tuple(run_ids))


# --- diagnosis --------------------------------------------------------------------


@dataclass(frozen=True)
class ContainerState:
    """Subset of `docker inspect` .State."""

    exists: bool
    status: str = "missing"          # running, exited, created, ...
    exit_code: int | None = None
    oom_killed: bool = False
    started_at: datetime | None = None

    @property
    def running(self) -> bool:
        return self.exists and self.status == "running"

    def describe(self, now: datetime) -> str:
        if not self.exists:
            return "missing (container does not exist)"
        if self.running:
            up = f", up {(now - self.started_at).total_seconds():.0f}s" if self.started_at else ""
            return f"running{up}"
        oom = ", OOMKilled=true" if self.oom_killed else ""
        return f"{self.status} (exit code {self.exit_code}{oom})"


def describe_status_file(current: StatusSnapshot | None, raw_present: bool, now: datetime) -> str:
    """One line: MISSING, UNREADABLE, or age + batch + query_id."""
    if current is None:
        return "UNREADABLE (bad JSON or unknown format)" if raw_present else "MISSING"
    age = (
        f"updated_at {(now - current.updated_at).total_seconds():.1f}s ago"
        if current.updated_at else "updated_at missing"
    )
    return (f"{age}, last batch_id {current.batch_id}, query_id {current.query_id}, "
            f"topics {sorted(current.end_offsets)}")


def diagnose(
    *,
    container: ContainerState,
    remaining_at_start: int,
    remaining_now: int,
    status: StatusSnapshot | None,
    wait_logs: LogFacts,
    waited_s: float,
    trigger_interval_s: float,
) -> tuple[str, str]:
    """Return (label, explanation) for a run that did not catch up.

    Labels: stopped, crashed, status file not updating, slow, too early to tell,
    stuck.
    `wait_logs` must only contain log lines written during the wait. Because
    Spark posts no progress events while idle (D1.16), an old `updated_at` is
    never used on its own; we look for batches that were committed (log lines)
    but not reflected in the status file.
    """
    if not container.running:
        if container.exists and container.exit_code == 0 and not container.oom_killed:
            return ("stopped", "the Spark container exited cleanly (code 0), i.e. it was "
                               "stopped on purpose (e.g. `make spark-stop`); start it again")
        return ("crashed", "the Spark container is not running and did not exit cleanly; "
                           "see its last log lines below")
    status_batch = status.batch_id if status else None
    log_batch = wait_logs.max_batch_id
    if wait_logs.status_write_warnings or (
        log_batch is not None and (status_batch is None or log_batch > status_batch)
    ):
        return ("status file not updating",
                f"Spark committed batches during the wait (latest logged batch_id {log_batch}) "
                f"but the status file shows batch_id {status_batch}; "
                f"{wait_logs.status_write_warnings} 'could not write status file' warnings")
    if wait_logs.batch_ids or remaining_now < remaining_at_start:
        return ("slow",
                f"Spark committed {len(wait_logs.batch_ids)} batches during the wait and "
                f"remaining offsets went {remaining_at_start} -> {remaining_now}; "
                "it is progressing, give it a longer --timeout")
    if waited_s < 2 * trigger_interval_s:
        return ("too early to tell",
                f"waited only {waited_s:.0f}s, less than two trigger intervals "
                f"({trigger_interval_s:.0f}s each); no batch was expected yet")
    extra = "" if wait_logs.query_started or status else " (no 'query started' line seen)"
    return ("stuck",
            f"container running, {remaining_now} run offsets pending, and no batch committed "
            f"in {waited_s:.0f}s{extra}; check Kafka and the log lines below")


def format_lag_table(lag: Iterable[PartitionLag], remaining_at_start: Mapping[int, int]) -> str:
    """Render the lag table shown on timeout and in demo output."""
    header = (f"{'partition':>9}  {'run offsets':>13}  {'committed_end':>13}  "
              f"{'remaining':>9}  {'remaining@start':>15}  progress")
    lines = [header]
    for row in lag:
        start = remaining_at_start.get(row.partition, row.remaining)
        if row.done:
            progress = "done"
        elif row.remaining < start:
            progress = "moving"
        else:
            progress = "no change"
        end = "-" if row.committed_end is None else str(row.committed_end)
        lines.append(f"{row.partition:>9}  {f'{row.run.min}..{row.run.max}':>13}  {end:>13}  "
                     f"{row.remaining:>9}  {start:>15}  {progress}")
    return "\n".join(lines)
