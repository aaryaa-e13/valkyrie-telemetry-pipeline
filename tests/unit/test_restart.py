"""Unit tests for e2e.restart (pure restart checks, evidence_version 2 = Delta)."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from common.manifest_reader import OffsetRange
from e2e import restart
from e2e.restart import Batch

TOPIC = "game-events"
E0 = {0: 100, 1: 50, 2: 70}
RUN = {0: OffsetRange(100, 104, 5), 2: OffsetRange(70, 72, 3)}   # 8 messages
FINAL = {0: 105, 1: 50, 2: 73}


def batch(bid: int, start: dict | None, end: dict | None, rows: int, run_id: str = "new") -> Batch:
    return Batch(bid, run_id, rows, start, end)


GOOD = [batch(8, dict(E0), dict(FINAL), 8)]


# --- inputs -----------------------------------------------------------------------

def test_parse_kafka_end_offsets() -> None:
    text = "game-events:0:850\ngame-events:1:510\nother:0:9\n"
    assert restart.parse_kafka_end_offsets(text, TOPIC) == {0: 850, 1: 510}


def test_parse_kafka_end_offsets_ignores_prefix_topics_and_fails_if_absent() -> None:
    assert restart.parse_kafka_end_offsets("game-events-x:0:5\ngame-events:0:1", TOPIC) == {0: 1}
    with pytest.raises(ValueError):
        restart.parse_kafka_end_offsets("other:0:9", TOPIC)


def test_baseline_pending() -> None:
    assert restart.baseline_pending({0: 10, 1: 0}, {0: 10}) == []        # empty partition ok
    assert restart.baseline_pending({0: 10}, {0: 12}) == []
    assert restart.baseline_pending({0: 10}, {0: 9}) == ["p0: kafka end 10, committed 9"]
    assert restart.baseline_pending({3: 4}, {}) == ["p3: kafka end 4, nothing committed"]


def test_run_start_problems() -> None:
    assert restart.run_start_problems(RUN, E0) == ([], [])
    writers, unprovable = restart.run_start_problems({0: OffsetRange(103, 107, 5)}, E0)
    assert "3 offsets in between came from another writer" in writers[0] and unprovable == []
    writers, unprovable = restart.run_start_problems({5: OffsetRange(0, 1, 2)}, E0)
    assert writers == [] and "E0 has no offset" in unprovable[0]


def test_history_batches_converts_and_sorts() -> None:
    status = {"history": [
        {"batch_id": 9, "run_id": "b", "num_input_rows": 3, "start_offset": None,
         "end_offset": {TOPIC: {"0": 5}}},
        {"batch_id": 8, "run_id": "a", "num_input_rows": 1,
         "start_offset": {TOPIC: {"0": 1}}, "end_offset": {TOPIC: {"0": 2}}},
    ]}
    b8, b9 = restart.history_batches(status, TOPIC)
    assert (b8.batch_id, b8.start, b8.end) == (8, {0: 1}, {0: 2})
    assert b9.start is None and b9.end == {0: 5}


# --- first post-restart batch -------------------------------------------------------

def _status(history: list[dict], query_id: str = "q") -> dict:
    return {"query_id": query_id, "history": history,
            "last_batch": history[-1] if history else None}


def _h(bid: int, run_id: str) -> dict:
    return {"batch_id": bid, "run_id": run_id, "num_input_rows": 1,
            "start_offset": {TOPIC: {"0": 0}}, "end_offset": {TOPIC: {"0": 1}}}


def test_first_post_restart_batch_is_b0_plus_1() -> None:
    status = _status([_h(6, "old"), _h(7, "old"), _h(8, "new"), _h(9, "new")])
    post, problems = restart.find_post_restart(status, q0="q", b0=7, r0="old",
                                               new_run_ids=["new"], topic=TOPIC)
    assert problems == [] and [b.batch_id for b in post] == [8, 9]


def test_first_batch_with_old_run_id_is_rejected() -> None:
    status = _status([_h(7, "old"), _h(8, "old")])
    _, problems = restart.find_post_restart(status, q0="q", b0=7, r0="old",
                                            new_run_ids=["new"], topic=TOPIC)
    assert any("pre-stop run_id" in p for p in problems)
    assert any("does not match" in p for p in problems)


def test_missing_b0_plus_1_in_history_cannot_prove() -> None:
    status = _status([_h(9, "new")])
    _, problems = restart.find_post_restart(status, q0="q", b0=7, r0="old",
                                            new_run_ids=["new"], topic=TOPIC)
    assert "history has no batch 8" in problems[0]


def test_other_query_id_and_no_batches_and_no_log_line() -> None:
    _, p1 = restart.find_post_restart(_status([_h(8, "new")], query_id="other"), q0="q", b0=7,
                                      r0="old", new_run_ids=["new"], topic=TOPIC)
    assert "checkpoint was reset" in p1[0]
    _, p2 = restart.find_post_restart(_status([_h(7, "old")]), q0="q", b0=7, r0="old",
                                      new_run_ids=["new"], topic=TOPIC)
    assert "no batch after B0=7" in p2[0]
    _, p3 = restart.find_post_restart(_status([_h(8, "new")]), q0="q", b0=7, r0="old",
                                      new_run_ids=[], topic=TOPIC)
    assert "no 'query started' log line" in p3[0]
    _, p4 = restart.find_post_restart(None, q0="q", b0=7, r0="old", new_run_ids=[], topic=TOPIC)
    assert "missing" in p4[0]


# --- no re-read ---------------------------------------------------------------------

def test_no_rewind_exact_match_passes() -> None:
    assert restart.check_no_rewind(GOOD, E0) == []


def test_rewind_by_one_is_detected() -> None:
    problems = restart.check_no_rewind([batch(8, {**E0, 1: 49}, FINAL, 9)], E0)
    assert problems == ["p1: REWIND, batch 8 starts at 49, E0 is 50 (1 offsets re-read)"]


def test_gap_by_one_is_detected() -> None:
    problems = restart.check_no_rewind([batch(8, {**E0, 0: 101}, FINAL, 7)], E0)
    assert problems == ["p0: GAP, batch 8 starts at 101, E0 is 100 (1 offsets never read)"]


def test_missing_start_offset_cannot_prove() -> None:
    assert "no start_offset" in restart.check_no_rewind([batch(8, None, FINAL, 8)], E0)[0]
    partial = {0: 100, 2: 70}
    assert restart.check_no_rewind([batch(8, partial, FINAL, 8)], E0) == [
        "p1: batch 8 has no start offset (cannot prove)"]


def test_later_batches_must_chain() -> None:
    mid = {0: 102, 1: 50, 2: 70}
    ok = [batch(8, dict(E0), mid, 2), batch(9, dict(mid), dict(FINAL), 6)]
    assert restart.check_no_rewind(ok, E0) == []
    bad = [batch(8, dict(E0), mid, 2), batch(9, {**mid, 0: 101}, dict(FINAL), 7)]
    assert "batch 9 start" in restart.check_no_rewind(bad, E0)[0]


def test_no_post_batches_fails() -> None:
    assert restart.check_no_rewind([], E0) == ["no post-restart batch to check"]


# --- no skip ------------------------------------------------------------------------

def test_no_skip_passes_when_everything_lines_up() -> None:
    assert restart.check_no_skip(GOOD, E0, FINAL, RUN, 8) == []


def test_rows_off_by_one_is_detected() -> None:
    problems = restart.check_no_skip([batch(8, E0, FINAL, 7)], E0, FINAL, RUN, 8)
    assert problems == ["rows read after restart 7 != run messages 8 (-1)"]


def test_end_not_e0_plus_count_is_detected() -> None:
    problems = restart.check_no_skip(GOOD, E0, {**FINAL, 2: 74}, RUN, 8)
    assert problems == ["p2: end 74 != E0 70 + run count 3"]


def test_untouched_partition_that_moved_is_detected() -> None:
    problems = restart.check_no_skip(GOOD, E0, {**FINAL, 1: 51}, RUN, 8)
    assert "p1: run did not write here, but end moved 50 -> 51" in problems[0]


def test_e0_missing_for_touched_partition_cannot_prove() -> None:
    e0 = {0: 100, 1: 50}                      # p2 missing, but the run wrote to p2
    problems = restart.check_no_skip(GOOD, e0, FINAL, RUN, 8)
    assert any("p2: run wrote here but E0 has no offset" in p for p in problems)


def test_e0_missing_for_untouched_partition_is_ignored() -> None:
    e0 = {0: 100, 2: 70}                      # p1 missing, run did not touch p1
    assert restart.check_no_skip(GOOD, e0, FINAL, RUN, 8) == []
    assert restart.check_no_rewind([batch(8, {0: 100, 2: 70}, FINAL, 8)], e0) == []


def test_no_final_end_cannot_prove() -> None:
    assert restart.check_no_skip(GOOD, E0, None, RUN, 8)[-1] == \
        "no committed end offset after catch-up"


# --- Delta checks (log delta, log vs Spark, txn identity) ---------------------------

def _log(rows: int = 8, txns: list[dict] | None = None, v0: int = 20, v1: int = 21) -> dict:
    if txns is None:
        txns = [{"delta_version": 21, "appId": "q", "version": 8}]
    return {"v_before": v0, "v_after": v1, "rows": rows, "files": 2, "commits": v1 - v0,
            "txns": txns}


def test_log_delta() -> None:
    assert restart.check_log_delta(_log(8), 8) == []
    assert "+3, EXTRA rows" in restart.check_log_delta(_log(11), 8)[0]
    assert "-1, MISSING rows" in restart.check_log_delta(_log(7), 8)[0]
    assert "cannot prove" in restart.check_log_delta({"error": "commit file missing"}, 8)[0]
    assert "cannot prove" in restart.check_log_delta(None, 8)[0]


def test_log_vs_spark() -> None:
    assert restart.check_log_vs_spark(_log(8), {"20": 1000, "21": 1008}) == []
    assert "!= Spark time-travel delta 9" in \
        restart.check_log_vs_spark(_log(8), {"20": 1000, "21": 1009})[0]
    assert "cannot prove" in restart.check_log_vs_spark(_log(8), {"20": 1000})[0]
    assert "cannot prove" in restart.check_log_vs_spark(_log(8), None)[0]


POST = [batch(8, dict(E0), dict(FINAL), 8)]


def test_txn_identity_ok() -> None:
    assert restart.check_txn_identity(_log(), "q", POST) == []


def test_txn_identity_run_id_instead_of_query_id_fails() -> None:
    """Condition: appId must be the stable query `id`, never the per-restart runId."""
    log = _log(txns=[{"delta_version": 21, "appId": "new", "version": 8}])  # "new" = run_id
    assert restart.check_txn_identity(log, "q", POST) == ["txn appId ['new'] != query id Q0 q"]


def test_txn_identity_commit_without_txn_and_wrong_versions() -> None:
    log = _log(txns=[], v0=20, v1=21)
    problems = restart.check_txn_identity(log, "q", POST)
    assert "commits without exactly one txn action: versions [21]" in problems[0]
    assert "txn versions [] != post-restart batch ids with rows [8]" in problems[1]
    twice = _log(txns=[{"delta_version": 21, "appId": "q", "version": 8}] * 2)
    assert "versions [21]" in restart.check_txn_identity(twice, "q", POST)[0]


def test_txn_identity_ignores_batches_without_rows() -> None:
    post = [*POST, batch(9, dict(FINAL), dict(FINAL), 0)]
    assert restart.check_txn_identity(_log(), "q", post) == []


def test_evidence_version_1_is_refused_with_pointer() -> None:
    problem = restart.evidence_problem({"evidence_version": 1})
    assert problem is not None and "tag `phase-1`" in problem
    assert "unknown evidence_version 3" in str(restart.evidence_problem({"evidence_version": 3}))
    with pytest.raises(ValueError, match="phase-1"):
        restart.evaluate({"evidence_version": 1})


# --- evaluate (whole evidence) ------------------------------------------------------

def evidence() -> dict[str, Any]:
    history = [
        {"batch_id": 7, "run_id": "old", "num_input_rows": 4,
         "start_offset": {TOPIC: {"0": 96, "1": 50, "2": 70}},
         "end_offset": {TOPIC: {str(k): v for k, v in E0.items()}}},
        {"batch_id": 8, "run_id": "new", "num_input_rows": 8,
         "start_offset": {TOPIC: {str(k): v for k, v in E0.items()}},
         "end_offset": {TOPIC: {str(k): v for k, v in FINAL.items()}}},
    ]
    return {
        "topic": TOPIC, "stop_exit_code": 0,
        "kafka_end_at_baseline": {"0": 100, "1": 50, "2": 70},
        "e0": {str(k): v for k, v in E0.items()}, "b0": 7, "q0": "q", "r0": "old",
        "new_run_ids": ["new"], "run_count": 8,
        "run_offsets": {"0": [100, 104, 5], "2": [70, 72, 3]},
        "status_after": {"query_id": "q", "history": history, "last_batch": history[-1]},
        "evidence_version": 2,
        "delta": {"table": "data/lake/bronze/game_events", "v0": 20, "v1": 21, "log": _log()},
        "version_counts": {"20": 1000, "21": 1008},
        "verify": {"result": "PASS", "found_unique_ids": 8, "expected_unique_ids": 8,
                   "duplicate_rows": 0},
    }


def test_evaluate_all_pass() -> None:
    checks = restart.evaluate(evidence())
    assert all(c.ok for c in checks), restart.format_checks(checks)
    assert restart.exit_code_for(checks) == 0


def _failed(ev: dict[str, Any]) -> set[str]:
    return {c.name for c in restart.evaluate(ev) if not c.ok}


def test_evaluate_stop_exit_code() -> None:
    ev = evidence()
    ev["stop_exit_code"] = 137
    assert _failed(ev) == {"graceful stop"}


def test_evaluate_extra_rows_only_fails_log_delta() -> None:
    """Data has rows the listener does not know about: only the independent check sees it."""
    ev = evidence()
    ev["delta"]["log"]["rows"] += 3
    ev["version_counts"]["21"] += 3
    assert _failed(ev) == {"delta log rows == run"}
    assert restart.exit_code_for(restart.evaluate(ev)) == 5


def test_evaluate_log_parser_disagrees_with_spark() -> None:
    ev = evidence()
    ev["version_counts"]["21"] += 1
    assert _failed(ev) == {"delta log == spark time travel"}


def test_evaluate_unreadable_log_cannot_prove() -> None:
    ev = evidence()
    ev["delta"]["log"] = {"error": "version 21: add without numRecords", "v_before": 20,
                          "v_after": 21}
    assert _failed(ev) == {"delta log rows == run", "delta log == spark time travel",
                           "txn appId == query id"}
    assert restart.exit_code_for(restart.evaluate(ev)) == 5


def test_evaluate_rewind() -> None:
    ev = copy.deepcopy(evidence())
    first = ev["status_after"]["history"][1]
    first["start_offset"][TOPIC]["0"] = 99
    first["num_input_rows"] = 9
    assert _failed(ev) == {"no re-read (start == E0)", "no skip (rows + offsets)"}


def test_evaluate_verify_only_gives_exit_1() -> None:
    ev = evidence()
    ev["verify"]["result"] = "FAIL"
    checks = restart.evaluate(ev)
    assert {c.name for c in checks if not c.ok} == {"verify --fail-on-duplicates"}
    assert restart.exit_code_for(checks) == 1


def test_evaluate_baseline_incomplete() -> None:
    ev = evidence()
    ev["kafka_end_at_baseline"]["1"] = 51
    assert _failed(ev) == {"baseline complete"}
