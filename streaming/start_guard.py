"""Bronze start-up guard: checkpoint and Delta table must describe the same history.

Standard library only (unit-testable without Spark). streaming.bronze feeds it
the checkpoint state and the table's newest txn version for the checkpoint's
query id; see DECISIONS D2.2.

Write order for batch N of the Delta sink (D1.15, D2.1 C12):
    offsets/N  ->  Delta commit with txn(appId = query id, version = N)  ->  commits/N

So, with C = newest commits/N, P = newest offsets/N and T = the table's newest
txn version for the checkpoint's query id, the only legal states are:
    T == C                  normal stop, or a crash before the Delta commit of C+1
    T == C + 1 and P >= C+1 a crash after the Delta commit of C+1 but before
                            commits/C+1; Spark re-runs C+1 and the sink skips it
                            ("Skipping already complete epoch")
T can never be *behind* C: commits/N is written only after the Delta commit of
N, and the sink commits a txn even for a batch with zero output rows (tested in
tests/spark/test_bronze_table.py). Everything else means checkpoint and table
disagree, and starting would either append messages twice or silently resume
past rows the table does not have.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


class BronzeStartupError(Exception):
    """The job must not start (checkpoint/table mismatch or unexpected table layout)."""


@dataclass(frozen=True)
class CheckpointState:
    """What a Structured Streaming checkpoint folder says (local paths only)."""

    query_id: str | None        # from `metadata`; stable across restarts
    last_planned: int | None    # newest offsets/N (Spark writes it before running N)
    last_committed: int | None  # newest commits/N (written after the sink commit)


def _newest_batch(folder: Path) -> int | None:
    if not folder.is_dir():
        return None
    ids = [int(f.name) for f in folder.iterdir() if f.name.isdigit()]
    return max(ids) if ids else None


def read_checkpoint_state(checkpoint_path: str | Path) -> CheckpointState:
    """Read query id and newest planned/committed batch. Missing parts are None."""
    cp = Path(checkpoint_path)
    query_id = None
    meta = cp / "metadata"
    if meta.is_file():
        first = meta.read_text(encoding="utf-8").strip().splitlines()[:1]
        if first:
            query_id = json.loads(first[0]).get("id")
    return CheckpointState(query_id, _newest_batch(cp / "offsets"), _newest_batch(cp / "commits"))


@dataclass(frozen=True)
class StartDecision:
    """mode: fresh | resume | refuse. `problem` explains a refusal."""

    mode: str
    problem: str | None = None
    note: str | None = None


def decide(state: CheckpointState, txn: int | None,
           table_has_rows: Callable[[], bool]) -> StartDecision:
    """Classify a start-up. `txn` is the table's newest txn version for
    state.query_id (None: no table, or no txn for that id). `table_has_rows` is
    only called when the answer depends on it (it costs a Spark read)."""
    p, c = state.last_planned, state.last_committed
    if p is None:  # the checkpoint never planned a batch
        if table_has_rows():
            return StartDecision("refuse", (
                "table has data but the checkpoint has no batches (missing or reset). "
                "Starting would re-read the topic and append every message again under a "
                "new query id"))
        return StartDecision("fresh")
    if state.query_id is None:
        return StartDecision("refuse", "checkpoint has batches but no query id in `metadata`")
    q = state.query_id
    if c is None:  # batch 0 planned, never committed (crash during the first batch)
        if txn == 0 and p == 0:
            return StartDecision("resume", note="batch 0 is in the table but not in commits/; "
                                 "Spark re-runs it and the sink skips it")
        if txn is None and not table_has_rows():
            return StartDecision("resume", note="batch 0 never reached the table")
        return StartDecision("refuse", (
            f"checkpoint (query {q}) committed no batch, but the table has "
            + ("data from another query" if txn is None else f"txn version {txn}")))
    if txn == c:
        return StartDecision("resume")
    if txn == c + 1 and p >= c + 1:
        return StartDecision("resume", note=(
            f"table already has batch {c + 1} (crash after its Delta commit, before "
            f"commits/{c + 1}); Spark re-runs it and the sink skips it"))
    if txn is None:
        return StartDecision("refuse", (
            f"checkpoint (query {q}) committed batch {c}, but the table is missing or has no "
            "txn for this query id (deleted or replaced). Resuming would skip every row "
            "before the checkpoint's offsets"))
    if txn < c:
        return StartDecision("refuse", (
            f"table is behind the checkpoint: newest txn for query {q} is {txn}, checkpoint "
            f"committed {c} (table restored from an older copy?). Resuming would lose "
            f"batches {txn + 1}..{c}"))
    return StartDecision("refuse", (
        f"table is ahead of the checkpoint: txn {txn} for query {q}, checkpoint planned "
        f"only up to {p} (another writer used this query id, or the checkpoint is old)"))
