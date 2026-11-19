"""Unit tests for streaming.status (status file written after each committed batch)."""

from __future__ import annotations

import json
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from streaming.config import BronzeJobConfig
from streaming.status import (
    HISTORY_LIMIT,
    STATUS_VERSION,
    batch_record,
    parse_offsets,
    read_status,
    update_status,
    write_status,
)

NOW = datetime(2026, 11, 14, 18, 0, 0, tzinfo=UTC)


def _record(batch_id: int, end: dict[str, int], run_id: str = "run-1", start=None) -> dict:
    return batch_record(
        batch_id=batch_id, run_id=run_id, num_input_rows=5, timestamp="2026-11-14T18:00:00.000Z",
        trigger_ms=100, start_offset=start,
        end_offset=json.dumps({"game-events": end}),  # Spark passes a JSON string
    )


# --- parse_offsets -----------------------------------------------------------------

@pytest.mark.parametrize("raw", [None, "None", "null", ""])
def test_missing_start_offset_becomes_none(raw: str | None) -> None:
    assert parse_offsets(raw) is None


def test_offsets_from_json_string_keep_string_partition_keys() -> None:
    assert parse_offsets('{"game-events":{"0":13,"5":11}}') == {"game-events": {"0": 13, "5": 11}}


def test_offsets_accept_mapping_and_coerce_ints() -> None:
    assert parse_offsets({"t": {0: "7"}}) == {"t": {"0": 7}}


@pytest.mark.parametrize("bad", ['["not", "an", "object"]', '{"t": 5}', "{not json"])
def test_malformed_offsets_raise(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_offsets(bad)


# --- update_status -----------------------------------------------------------------

def test_first_update_creates_status() -> None:
    s = update_status(None, _record(0, {"0": 3}), query_name="q", query_id="Q1", now=NOW)
    assert s["status_version"] == STATUS_VERSION
    assert s["query_id"] == "Q1"
    assert s["updated_at"] == "2026-11-14T18:00:00.000Z"
    assert s["last_batch"]["batch_id"] == 0
    assert s["last_batch"]["end_offset"] == {"game-events": {"0": 3}}
    assert s["last_batch"]["start_offset"] is None
    assert len(s["history"]) == 1


def test_history_appends_and_last_batch_is_highest() -> None:
    s = None
    for b in range(3):
        s = update_status(s, _record(b, {"0": b + 1}), query_name="q", query_id="Q1", now=NOW)
    assert [h["batch_id"] for h in s["history"]] == [0, 1, 2]
    assert s["last_batch"]["batch_id"] == 2


def test_repeated_batch_id_replaces_instead_of_duplicating() -> None:
    s = update_status(None, _record(4, {"0": 10}), query_name="q", query_id="Q1", now=NOW)
    s = update_status(s, _record(4, {"0": 12}), query_name="q", query_id="Q1", now=NOW)
    assert [h["batch_id"] for h in s["history"]] == [4]
    assert s["last_batch"]["end_offset"]["game-events"]["0"] == 12


def test_history_is_capped() -> None:
    s = None
    for b in range(HISTORY_LIMIT + 25):
        s = update_status(s, _record(b, {"0": b}), query_name="q", query_id="Q1", now=NOW)
    assert len(s["history"]) == HISTORY_LIMIT
    assert s["history"][0]["batch_id"] == 25
    assert s["last_batch"]["batch_id"] == HISTORY_LIMIT + 24


def test_history_survives_restart_with_same_query_id() -> None:
    s = update_status(None, _record(0, {"0": 3}, run_id="run-A"), query_name="q",
                      query_id="Q1", now=NOW)
    s = update_status(s, _record(1, {"0": 5}, run_id="run-B"), query_name="q",
                      query_id="Q1", now=NOW)
    assert [h["run_id"] for h in s["history"]] == ["run-A", "run-B"]


def test_new_query_id_resets_history() -> None:
    """A reset checkpoint gets a new query id; old offsets must not be mixed in."""
    s = update_status(None, _record(7, {"0": 50}), query_name="q", query_id="OLD", now=NOW)
    s = update_status(s, _record(0, {"0": 2}), query_name="q", query_id="NEW", now=NOW)
    assert [h["batch_id"] for h in s["history"]] == [0]
    assert s["query_id"] == "NEW"


def test_update_does_not_mutate_previous() -> None:
    s1 = update_status(None, _record(0, {"0": 1}), query_name="q", query_id="Q1", now=NOW)
    snapshot = json.dumps(s1, sort_keys=True)
    update_status(s1, _record(1, {"0": 2}), query_name="q", query_id="Q1", now=NOW)
    assert json.dumps(s1, sort_keys=True) == snapshot


# --- write/read --------------------------------------------------------------------

def test_write_then_read_roundtrip_atomic_and_world_readable(tmp_path: Path) -> None:
    path = tmp_path / "status" / "raw.json"  # parent folder created on demand
    s = update_status(None, _record(0, {"0": 3}), query_name="q", query_id="Q1", now=NOW)
    write_status(s, path)
    assert read_status(path) == s
    assert [p.name for p in path.parent.iterdir()] == ["raw.json"]  # no temp files left
    assert stat.S_IMODE(path.stat().st_mode) == 0o644


@pytest.mark.parametrize("content", [
    "",                                         # empty
    "{half a file",                             # torn / corrupt
    '{"status_version": 999, "last_batch": {}}',  # unknown version
    '{"status_version": 1}',                    # no last_batch
    "[1, 2, 3]",                                # wrong top-level type
])
def test_unusable_status_reads_as_none(tmp_path: Path, content: str) -> None:
    path = tmp_path / "raw.json"
    path.write_text(content)
    assert read_status(path) is None


def test_missing_status_reads_as_none(tmp_path: Path) -> None:
    assert read_status(tmp_path / "nope.json") is None


# --- config --------------------------------------------------------------------------

def test_status_path_default_and_explicit_disable() -> None:
    assert BronzeJobConfig.from_env({}).status_path == "/data/status/bronze.json"
    assert BronzeJobConfig.from_env({"STATUS_PATH": ""}).status_path == ""
    assert BronzeJobConfig.from_env({"STATUS_PATH": "/x/s.json"}).status_path == "/x/s.json"
