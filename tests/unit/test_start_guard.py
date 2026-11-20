"""Unit tests for streaming.start_guard (pure decision table, no Spark).

Spark tests in tests/spark/test_bronze_table.py create each state for real.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from streaming.start_guard import CheckpointState, decide, read_checkpoint_state


def _never() -> bool:
    raise AssertionError("table_has_rows must not be needed here")


def S(planned: int | None, committed: int | None, qid: str | None = "q") -> CheckpointState:  # noqa: N802
    return CheckpointState(qid, planned, committed)


@pytest.mark.parametrize(("state", "txn", "rows", "mode"), [
    # no batches in the checkpoint
    (S(None, None), None, False, "fresh"),
    (S(None, None), None, True, "refuse"),             # reset checkpoint, populated table
    (S(None, None, qid=None), None, True, "refuse"),   # checkpoint folder missing entirely
    # first batch planned, never committed
    (S(0, None), None, False, "resume"),               # batch 0 never reached the table
    (S(0, None), 0, None, "resume"),                   # batch 0 in table, commits/0 missing
    (S(0, None), None, True, "refuse"),                # table holds another query's data
    (S(0, None), 3, None, "refuse"),
    # committed batches
    (S(5, 5), 5, None, "resume"),                      # normal
    (S(6, 5), 5, None, "resume"),                      # crash before the Delta commit of 6
    (S(6, 5), 6, None, "resume"),                      # crash after it, before commits/6
    (S(5, 5), 6, None, "refuse"),                      # txn ahead of anything planned
    (S(5, 5), None, None, "refuse"),                   # table missing / replaced
    (S(5, 5), 4, None, "refuse"),                      # table behind (restored old copy)
    (S(6, 5), 7, None, "refuse"),
    (S(5, 5, qid=None), None, None, "refuse"),         # batches but no query id
])
def test_decision_table(state: CheckpointState, txn: int | None, rows: bool | None,
                        mode: str) -> None:
    decision = decide(state, txn, _never if rows is None else (lambda: rows))
    assert decision.mode == mode, decision
    assert (decision.problem is None) == (mode != "refuse")


def test_refusal_messages_name_the_case() -> None:
    assert "missing or has no txn" in str(decide(S(5, 5), None, _never).problem)
    assert "behind the checkpoint" in str(decide(S(5, 5), 4, _never).problem)
    assert "lose batches 5..5" in str(decide(S(5, 5), 4, _never).problem)
    assert "ahead of the checkpoint" in str(decide(S(5, 5), 6, _never).problem)
    assert "new query id" in str(decide(S(None, None), None, lambda: True).problem)
    assert "skips it" in str(decide(S(6, 5), 6, _never).note)


def test_read_checkpoint_state(tmp_path: Path) -> None:
    assert read_checkpoint_state(tmp_path / "missing") == CheckpointState(None, None, None)
    cp = tmp_path / "cp"
    (cp / "offsets").mkdir(parents=True)
    (cp / "commits").mkdir()
    (cp / "metadata").write_text(json.dumps({"id": "qid-1"}))
    assert read_checkpoint_state(cp) == CheckpointState("qid-1", None, None)
    for name in ("0", "1", "2", ".2.crc"):
        (cp / "offsets" / name).write_text("x")
    for name in ("0", "1", ".1.crc"):
        (cp / "commits" / name).write_text("x")
    assert read_checkpoint_state(cp) == CheckpointState("qid-1", 2, 1)


def test_committed_source_offset(tmp_path) -> None:  # noqa: ANN001
    from streaming.start_guard import committed_source_offset

    cp = tmp_path / "cp"
    assert committed_source_offset(cp) is None
    (cp / "offsets").mkdir(parents=True)
    (cp / "commits").mkdir()
    for n in (0, 1):
        (cp / "offsets" / str(n)).write_text(
            'v1\n{"batchWatermarkMs":0}\n'
            + json.dumps({"reservoirId": "r", "reservoirVersion": n + 3, "index": -1,
                          "isStartingVersion": False}) + "\n")
    (cp / "commits" / "0").write_text("v1\n{}\n")
    assert committed_source_offset(cp) == (0, {"reservoirId": "r", "reservoirVersion": 3,
                                               "index": -1, "isStartingVersion": False})
    (cp / "commits" / "1").write_text("v1\n{}\n")
    (cp / "offsets" / "1").write_text("v1\n")
    assert committed_source_offset(cp) == (1, None)
