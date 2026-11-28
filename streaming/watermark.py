"""Watermark rules of the silver job (DECISIONS D2.4). Standard library only.

For batch N of the silver query with id q (W = WATERMARK_DELAY_S, fixed per query):

    m_k    = max over the KEYED rows of batch k of least(event_time, kafka_timestamp)
             (rows with a NULL kafka_timestamp do not count; D3.7: nor rows with
             event_time > ingest_time + 5 min, the S1 condition). Stored in batch k's
             control row as batch_max_clamped_event_time.
    wm(N)  = -infinity (None)  if batch N STARTS inside the Delta source's initial
                               snapshot: N == 0, or offsets/N-1 says
                               isStartingVersion = true (slice 4, A1);
             max{m_k : k < N}  - W otherwise (-infinity if there is no m_k).
    late   iff event_time <= wm(N)   (raw event_time; the clamp only feeds the max).

Why the START of the batch (A1, measured with Delta 4.4.1): the batch that reads the
last snapshot files ENDS with isStartingVersion = false, so its own offsets/N would
call it a tail batch. offsets/N-1 is where batch N starts; if that is inside the
snapshot, batch N holds snapshot files and nothing in it is late. Effect: no row is
late while silver re-reads bronze's starting snapshot (after a checkpoint reset).

Determinism on replay: offsets/N-1 never changes once written, the control rows
k < N never change (appendOnly + txn), and W is fixed per query id. So a re-run of
batch N computes the same wm and the same split.

None always means -infinity. It is never put into SQL: the caller builds
`is_late = false` in Python for it (a NULL comparison would put a row in neither
table).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SnapshotStatusError(RuntimeError):
    """The checkpoint's source offset cannot tell whether a batch starts in the snapshot.

    Never defaulted (A2): the batch or the start-up fails, and the pipeline exits 2.
    The class name is in the message so the pipeline can recognise it through Spark's
    StreamingQueryException wrapper.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"SnapshotStatusError: {message}")


class SplitError(ValueError):
    """late + on-time rows do not add up to the batch's keyed unique ids."""


def source_offset(checkpoint_path: str | Path, batch_id: int) -> dict:
    """The (single) source's END offset of batch `batch_id`, from offsets/<batch_id>.

    The file holds "v1", one metadata line, then one JSON line per source.

    Raises:
        SnapshotStatusError: file missing or unreadable, not exactly one source
            line, or the line is not a JSON object.
    """
    path = Path(checkpoint_path) / "offsets" / str(batch_id)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SnapshotStatusError(f"cannot read {path}: {exc}") from None
    if len(lines) != 3 or lines[0] != "v1":
        raise SnapshotStatusError(f"{path}: expected 'v1', a metadata line and exactly one "
                                  f"source line, got {len(lines)} lines")
    try:
        offset = json.loads(lines[2])
    except ValueError as exc:
        raise SnapshotStatusError(f"{path}: source offset is not JSON ({exc})") from None
    if not isinstance(offset, dict):
        raise SnapshotStatusError(f"{path}: source offset is not a JSON object: {lines[2]!r}")
    return offset


def batch_starts_in_snapshot(checkpoint_path: str | Path, batch_id: int) -> bool:
    """True if batch `batch_id` starts inside the Delta source's initial snapshot (A1).

    Batch 0 always does (silver sets no startingVersion, so a new query starts with
    bronze's snapshot). Batch N > 0 starts where N-1 ended: offsets/N-1.

    Raises:
        SnapshotStatusError: offsets/N-1 is missing or unreadable, or has no boolean
            isStartingVersion (A2: never defaulted).
    """
    if isinstance(batch_id, bool) or not isinstance(batch_id, int) or batch_id < 0:
        raise SnapshotStatusError(f"batch id must be an int >= 0, got {batch_id!r}")
    if batch_id == 0:
        return True
    offset = source_offset(checkpoint_path, batch_id - 1)
    flag = offset.get("isStartingVersion")
    if not isinstance(flag, bool):
        raise SnapshotStatusError(
            f"{Path(checkpoint_path) / 'offsets' / str(batch_id - 1)}: isStartingVersion is "
            f"{'missing' if 'isStartingVersion' not in offset else repr(flag)}; refusing to "
            "guess whether batch {batch_id} re-reads bronze's snapshot".replace(
                "{batch_id}", str(batch_id)))
    return flag


def watermark_us(prior_max_us: int | None, delay_s: int, in_snapshot: bool) -> int | None:
    """wm(N) in epoch microseconds, or None for -infinity.

    Args:
        prior_max_us: max m_k over this query's control rows with batch_id < N
            (None: no such row has a value).
        delay_s: W, an int >= 1.
        in_snapshot: batch_starts_in_snapshot(...) for batch N.
    """
    if isinstance(delay_s, bool) or not isinstance(delay_s, int) or delay_s < 1:
        raise ValueError(f"delay_s must be an int >= 1, got {delay_s!r}")
    if in_snapshot or prior_max_us is None:
        return None
    return int(prior_max_us) - delay_s * 1_000_000


def us_to_datetime(us: int | None) -> datetime | None:
    """Epoch microseconds -> timezone-aware UTC datetime (exact, no float)."""
    return None if us is None else EPOCH + timedelta(microseconds=int(us))


def check_split(keyed_unique: int, deduped: int, late: int, ontime: int,
                rejected: int = 0) -> None:
    """Every keyed id is in exactly one class (D4; Phase 3 adds `rejected`, plan 2.4).
    Called before any write.

    Raises:
        SplitError: a count is negative, the deduped rows are not one per keyed id,
            or rejected + late + on-time != keyed_unique.
    """
    if min(keyed_unique, deduped, late, ontime, rejected) < 0:
        raise SplitError(f"negative count: keyed_unique={keyed_unique} deduped={deduped} "
                         f"late={late} ontime={ontime} rejected={rejected}")
    if deduped != keyed_unique:
        raise SplitError(f"in-batch dedupe kept {deduped} rows for {keyed_unique} keyed ids")
    if rejected + late + ontime != keyed_unique:
        raise SplitError(f"rejected {rejected} + late {late} + on-time {ontime} != keyed unique "
                         f"ids {keyed_unique}: some rows would be in no table (or two); "
                         "nothing written")


def recorded_delay_problem(recorded: int | None, configured: int, query_id: str) -> str | None:
    """Q2: W may not change for a query id. None if fine, else the refusal text."""
    if recorded is None or int(recorded) == int(configured):
        return None
    return (f"WATERMARK_DELAY_S={configured} but query {query_id} recorded "
            f"watermark_delay_s={recorded} in its control rows. W is fixed per query id so a "
            "replayed batch gets the same watermark. To change W, reset the silver checkpoint "
            "(new query id), or set WATERMARK_DELAY_S back to "
            f"{recorded} (DECISIONS D2.4)")
