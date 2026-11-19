"""Unit tests for e2e.catchup (pure logic, no Docker)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from common.manifest_reader import OffsetRange
from e2e import catchup
from e2e.catchup import ContainerState, LogFacts, StatusSnapshot

TOPIC = "game-events"
NOW = datetime(2026, 11, 15, 12, 0, 0, tzinfo=UTC)


def snap(batch_id: int, ends: dict[int, int], query_id: str = "q1",
         topic: str = TOPIC) -> StatusSnapshot:
    return StatusSnapshot(query_id, batch_id, NOW, {topic: ends})


RUN = {2: OffsetRange(5, 10, 6), 4: OffsetRange(14, 26, 13)}


# --- contiguity ---------------------------------------------------------------

def test_contiguous_run_has_no_problems() -> None:
    assert catchup.check_contiguity(RUN) == []


def test_interleaved_writer_is_reported_with_gap_size() -> None:
    problems = catchup.check_contiguity({4: OffsetRange(14, 30, 13)})
    assert len(problems) == 1
    assert "partition 4" in problems[0]
    assert "17 slots" in problems[0] and "4 offsets" in problems[0]
    assert "another producer" in problems[0]


def test_count_larger_than_span_is_a_manifest_bug() -> None:
    problems = catchup.check_contiguity({1: OffsetRange(0, 2, 5)})
    assert "inconsistent" in problems[0]


def test_only_bad_partitions_are_reported() -> None:
    problems = catchup.check_contiguity({0: OffsetRange(0, 4, 5), 3: OffsetRange(0, 9, 5)})
    assert len(problems) == 1 and problems[0].startswith("partition 3")


# --- snapshot parsing ------------------------------------------------------------

def _status(batch_id: int = 9, ends: dict | None = None) -> dict:
    return {
        "status_version": 1, "query_id": "q1", "updated_at": "2026-11-15T12:00:00.000Z",
        "last_batch": {"batch_id": batch_id,
                       "end_offset": {TOPIC: {k: v for k, v in (ends or {"2": 11}).items()}}},
    }


def test_snapshot_from_status_converts_types() -> None:
    s = catchup.snapshot_from_status(_status(ends={"2": 11, "4": 27}))
    assert s == StatusSnapshot("q1", 9, NOW, {TOPIC: {2: 11, 4: 27}})


@pytest.mark.parametrize("bad", [None, {}, {"query_id": "q"}, {"query_id": "q", "last_batch": 3},
                                 {"query_id": "q", "last_batch": {"batch_id": "x"}}])
def test_snapshot_from_bad_status_is_none(bad: dict | None) -> None:
    assert catchup.snapshot_from_status(bad) is None


def test_snapshot_with_null_end_offset_has_no_topics() -> None:
    status = _status()
    status["last_batch"]["end_offset"] = None
    s = catchup.snapshot_from_status(status)
    assert s is not None and s.end_offsets == {}


def test_parse_timestamp_variants() -> None:
    assert catchup.parse_timestamp("2026-11-15T12:00:00.000Z") == NOW
    assert catchup.parse_timestamp("2026-11-15T12:00:00+00:00") == NOW
    assert catchup.parse_timestamp("garbage") is None
    assert catchup.parse_timestamp(None) is None


# --- lag and caught-up -------------------------------------------------------------

def test_lag_counts_remaining_offsets_within_run_range() -> None:
    lag = catchup.compute_lag(RUN, snap(9, {2: 8, 4: 3}), TOPIC)
    assert [(r.partition, r.remaining) for r in lag] == [(2, 3), (4, 13)]


def test_lag_without_status_is_whole_run() -> None:
    lag = catchup.compute_lag(RUN, None, TOPIC)
    assert [r.remaining for r in lag] == [6, 13]
    assert all(r.committed_end is None for r in lag)


def test_end_equal_to_max_is_not_caught_up_end_is_exclusive() -> None:
    res = catchup.check_caught_up(snap(8, {}), snap(9, {2: 10, 4: 27}), RUN, TOPIC)
    assert not res.caught_up
    assert [r.remaining for r in res.lag] == [1, 0]


def test_end_greater_than_max_everywhere_is_caught_up() -> None:
    res = catchup.check_caught_up(snap(8, {}), snap(9, {2: 11, 4: 27}), RUN, TOPIC)
    assert res.caught_up and res.fresh


def test_stale_status_that_looks_caught_up_is_rejected() -> None:
    """Topic recreated: an old status already has large offsets, no new batch since."""
    old = snap(50, {2: 900, 4: 900})
    res = catchup.check_caught_up(old, old, RUN, TOPIC)
    assert not res.caught_up and not res.fresh
    assert "no batch committed since the run was sent" in res.reasons[0]


def test_new_query_id_counts_as_fresh_with_a_note() -> None:
    res = catchup.check_caught_up(snap(50, {}), snap(0, {2: 11, 4: 27}, query_id="q2"),
                                  RUN, TOPIC)
    assert res.caught_up
    assert any("query_id changed" in r for r in res.reasons)


def test_wrong_topic_is_never_caught_up() -> None:
    res = catchup.check_caught_up(None, snap(1, {2: 99, 4: 99}, topic="other"), RUN, TOPIC)
    assert not res.caught_up
    assert any("not for topic 'game-events'" in r for r in res.reasons)


def test_missing_status_is_not_caught_up() -> None:
    res = catchup.check_caught_up(None, None, RUN, TOPIC)
    assert not res.caught_up and res.reasons == ("status file missing or unreadable",)


def test_no_baseline_accepts_any_status_written_later() -> None:
    assert catchup.check_caught_up(None, snap(0, {2: 11, 4: 27}), RUN, TOPIC).caught_up


# --- pre-run guard (topic recreated) ---------------------------------------------------

def test_baseline_past_run_start_means_topic_was_recreated() -> None:
    problems = catchup.pre_run_conflicts(snap(50, {2: 900, 4: 14}), RUN, TOPIC)
    assert len(problems) == 1 and problems[0].startswith("partition 2")
    assert "recreated" in problems[0]


def test_baseline_at_run_start_is_fine() -> None:
    assert catchup.pre_run_conflicts(snap(8, {2: 5, 4: 14}), RUN, TOPIC) == []
    assert catchup.pre_run_conflicts(None, RUN, TOPIC) == []
    assert catchup.pre_run_conflicts(snap(8, {2: 900}, topic="other"), RUN, TOPIC) == []


# --- logs ------------------------------------------------------------------------------

def test_parse_log_lines_extracts_facts_and_ignores_noise() -> None:
    lines = [
        "Using Spark's default log4j profile",
        json.dumps({"msg": "query started", "level": "INFO"}),
        json.dumps({"msg": "batch progress", "level": "INFO", "batch_id": 9}),
        json.dumps({"msg": "batch progress", "level": "INFO", "batch_id": 10}),
        json.dumps({"msg": "could not write status file", "level": "WARNING"}),
        json.dumps({"msg": "query failed", "level": "ERROR"}),
        "{not json",
    ]
    facts = catchup.parse_log_lines(lines)
    assert facts.batch_ids == (9, 10) and facts.max_batch_id == 10
    assert facts.status_write_warnings == 1
    assert facts.query_started and facts.errors == ("query failed",)


# --- diagnosis ----------------------------------------------------------------------------

RUNNING = ContainerState(exists=True, status="running", started_at=NOW - timedelta(minutes=5))
NO_LOGS = LogFacts((), 0, False, ())


def _diag(**kw):  # type: ignore[no-untyped-def]
    base = dict(container=RUNNING, remaining_at_start=10, remaining_now=10,
                status=snap(8, {}), wait_logs=NO_LOGS, waited_s=90.0, trigger_interval_s=10.0)
    base.update(kw)
    return catchup.diagnose(**base)[0]


def test_diagnose_crashed_when_container_not_running() -> None:
    dead = ContainerState(exists=True, status="exited", exit_code=137, oom_killed=True)
    assert _diag(container=dead) == "crashed"
    assert _diag(container=ContainerState(exists=False)) == "crashed"
    assert _diag(container=ContainerState(exists=True, status="exited", exit_code=1)) == "crashed"


def test_diagnose_stopped_when_container_exited_cleanly() -> None:
    clean = ContainerState(exists=True, status="exited", exit_code=0)
    assert _diag(container=clean) == "stopped"


def test_diagnose_status_not_updating_when_logs_are_ahead() -> None:
    logs = LogFacts((9, 10), 0, True, ())
    assert _diag(wait_logs=logs, status=snap(8, {})) == "status file not updating"
    assert _diag(wait_logs=logs, status=None) == "status file not updating"
    assert _diag(wait_logs=LogFacts((), 2, True, ())) == "status file not updating"


def test_diagnose_slow_when_batches_commit_but_not_done() -> None:
    assert _diag(wait_logs=LogFacts((9,), 0, True, ()), status=snap(9, {}),
                 remaining_now=4) == "slow"
    assert _diag(remaining_now=4) == "slow"


def test_diagnose_too_early_for_short_timeouts() -> None:
    assert _diag(waited_s=5.0) == "too early to tell"


def test_diagnose_stuck_when_nothing_happens() -> None:
    assert _diag() == "stuck"


# --- formatting ---------------------------------------------------------------------------

def test_container_describe() -> None:
    assert RUNNING.describe(NOW) == "running, up 300s"
    dead = ContainerState(exists=True, status="exited", exit_code=137, oom_killed=True)
    assert dead.describe(NOW) == "exited (exit code 137, OOMKilled=true)"


def test_describe_status_file() -> None:
    assert catchup.describe_status_file(None, False, NOW) == "MISSING"
    assert catchup.describe_status_file(None, True, NOW).startswith("UNREADABLE")
    line = catchup.describe_status_file(snap(9, {}), True, NOW + timedelta(seconds=47.2))
    assert line.startswith("updated_at 47.2s ago, last batch_id 9")


def test_lag_table_progress_column() -> None:
    lag = catchup.compute_lag(RUN, snap(9, {2: 11, 4: 20}), TOPIC)
    table = catchup.format_lag_table(lag, {2: 6, 4: 13})
    rows = table.splitlines()
    assert rows[1].endswith("done") and rows[2].endswith("moving")
    assert "14..26" in rows[2] and " 7 " in rows[2]


def test_docker_nanosecond_timestamps_are_trimmed() -> None:
    from e2e.docker_ops import _trim_nanos
    trimmed = _trim_nanos("2026-11-15T08:06:01.123456789Z")
    assert trimmed == "2026-11-15T08:06:01.123456Z"
    assert catchup.parse_timestamp(trimmed) == datetime(2026, 11, 15, 8, 6, 1, 123456, tzinfo=UTC)
    assert _trim_nanos("0001-01-01T00:00:00Z") == "0001-01-01T00:00:00Z"


@pytest.mark.parametrize("stderr", ["error: no such object: valkyrie-spark",
                                    "Error: No such object: valkyrie-spark",
                                    "Error response from daemon: No such container: x"])
def test_missing_container_error_any_case(stderr: str) -> None:
    from e2e.docker_ops import is_missing_container_error
    assert is_missing_container_error(stderr)
    assert not is_missing_container_error("permission denied while trying to connect")


# --- silver (Delta source) catch-up ----------------------------------------------


def _silver_status(version: int, index: int = -1, starting: bool = False,
                   reservoir: str = "bronze-id") -> dict:
    return {"status_version": 2, "source": "delta", "query_id": "S",
            "updated_at": "2026-11-19T10:00:00.000Z",
            "last_batch": {"batch_id": 4, "end_offset": {
                "reservoir_id": reservoir, "reservoir_version": version, "index": index,
                "is_starting_version": starting}}}


def test_kafka_snapshot_ignores_delta_status_and_vice_versa() -> None:
    assert catchup.snapshot_from_status(_silver_status(5)) is None
    assert catchup.delta_snapshot_from_status({"status_version": 2, "source": "kafka",
                                               "query_id": "q", "last_batch": {}}) is None
    snap = catchup.delta_snapshot_from_status(_silver_status(5))
    assert snap is not None and snap.reservoir_version == 5 and snap.batch_id == 4


@pytest.mark.parametrize(("version", "index", "starting", "expected"), [
    (8, -1, False, True),    # versions < 8 consumed; 7 is the target
    (7, -1, False, False),   # only versions < 7 consumed
    (7, 3, False, False),    # in the middle of version 7: wait (conservative)
    (9, 2, False, True),     # in the middle of 9: 7 is fully consumed
    (8, -1, True, False),    # still reading the initial snapshot
])
def test_silver_caught_up_rule(version: int, index: int, starting: bool, expected: bool) -> None:
    snap = catchup.delta_snapshot_from_status(_silver_status(version, index, starting))
    assert catchup.check_silver_caught_up(snap, "bronze-id", 7).caught_up is expected


def test_silver_wrong_table_or_missing_status_is_not_caught_up() -> None:
    other = catchup.delta_snapshot_from_status(_silver_status(9, reservoir="other"))
    res = catchup.check_silver_caught_up(other, "bronze-id", 7)
    assert not res.caught_up and "recreated" in res.reasons[0]
    assert not catchup.check_silver_caught_up(None, "bronze-id", 7).caught_up
    unknown = catchup.check_silver_caught_up(
        catchup.delta_snapshot_from_status(_silver_status(9)), None, 7)
    assert unknown.caught_up and "not checked" in unknown.reasons[0]


def test_log_facts_are_per_query() -> None:
    lines = [
        '{"msg": "query started", "query": "bronze", "run_id": "rb"}',
        '{"msg": "query started", "query": "silver", "run_id": "rs"}',
        '{"msg": "batch progress", "query": "bronze", "batch_id": 3}',
        '{"msg": "batch progress", "query": "silver", "batch_id": 9}',
        '{"msg": "batch progress", "batch_id": 2}',
    ]
    bronze = catchup.parse_log_lines(lines)
    silver = catchup.parse_log_lines(lines, query="silver")
    assert bronze.batch_ids == (3, 2) and bronze.started_run_ids == ("rb",)
    assert silver.batch_ids == (9,) and silver.started_run_ids == ("rs",)
