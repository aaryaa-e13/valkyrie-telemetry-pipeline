"""Unit tests for the pure part of e2e.timing_check (no Kafka needed)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from e2e.timing_check import EXIT_SETUP, Observed, main, parse_event_time_ms, summarize

T = 1_794_737_000_000  # some epoch-ms instant
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "manifests"


def _row(event_id: str, seq: int, event_ms: int, create_ms: int, *, partition: int = 0,
         offset: int | None = None, match_id: str = "m1") -> Observed:
    return Observed(partition=partition, offset=seq if offset is None else offset,
                    event_id=event_id, match_id=match_id, match_seq=seq,
                    event_time_ms=event_ms, create_time_ms=create_ms)


def test_parse_event_time_ms_reads_the_wire_format() -> None:
    assert parse_event_time_ms("2026-11-15T10:12:33.146Z") == 1794737553146
    assert parse_event_time_ms("2026-11-15T10:12:33.146+00:00") == 1794737553146
    with pytest.raises(ValueError):
        parse_event_time_ms("2026-11-15T10:12:33.146")


def test_on_time_run_passes() -> None:
    rows = [_row("a", 1, T, T), _row("b", 2, T + 500, T + 500), _row("c", 3, T + 900, T + 901)]
    r = summarize(rows, frozenset("abc"))
    assert r["result"] == "PASS"
    assert (r["ahead"], r["max_ahead_ms"], r["max_behind_ms"], r["out_of_order"]) == (0, 0, 1, 0)
    assert (r["events_expected"], r["events_read"]) == (3, 3)


def test_event_from_the_future_fails() -> None:
    """The D1.7c bug: event_time is later than the moment the message was sent."""
    rows = [_row("a", 1, T, T), _row("b", 2, T + 124_500, T + 3)]
    r = summarize(rows, frozenset("ab"))
    assert r["result"] == "FAIL"
    assert r["ahead"] == 1
    assert r["max_ahead_ms"] == 124_497
    assert "before their event_time" in r["reasons"][0]


@pytest.mark.parametrize(("ahead_ms", "expected_ahead"), [(0, 0), (1, 0), (2, 1)])
def test_one_millisecond_is_tolerated_two_are_not(ahead_ms: int, expected_ahead: int) -> None:
    r = summarize([_row("a", 1, T + ahead_ms, T)], frozenset("a"))
    assert r["tolerance_ms"] == 1
    assert r["ahead"] == expected_ahead
    assert r["result"] == ("PASS" if expected_ahead == 0 else "FAIL")


def test_backfill_events_are_behind_not_ahead() -> None:
    rows = [_row("a", 1, T - 110_000, T), _row("b", 2, T - 2, T)]
    r = summarize(rows, frozenset("ab"))
    assert (r["result"], r["max_ahead_ms"], r["max_behind_ms"]) == ("PASS", -2, 110_000)


def test_late_event_counts_as_out_of_order_within_its_match() -> None:
    rows = [
        _row("a", 1, T, T, offset=10),
        _row("c", 3, T + 2_000, T + 2_000, offset=11),
        _row("b", 2, T + 1_000, T + 9_000, offset=12),     # late: arrives after seq 3
        _row("x", 1, T + 5_000, T + 5_000, offset=13, match_id="m2"),   # other match: fine
    ]
    r = summarize(rows, frozenset("abcx"))
    assert r["out_of_order"] == 1
    assert r["result"] == "PASS"           # late is not "from the future"
    assert r["max_behind_ms"] == 8_000


def test_order_is_judged_per_partition_in_offset_order_not_read_order() -> None:
    rows = [_row("b", 2, T + 1, T + 1, offset=6), _row("a", 1, T, T, offset=5)]
    assert summarize(rows, frozenset("ab"))["out_of_order"] == 0


def test_missing_events_fail_and_foreign_or_repeated_messages_are_ignored() -> None:
    rows = [
        _row("a", 1, T, T, offset=1),
        _row("other-run", 1, T + 999_999, T, offset=2),    # not in the manifest: ignored
        _row("a", 1, T, T + 50, offset=3),                 # same id again: counted, not judged
    ]
    r = summarize(rows, frozenset("ab"))
    assert r["result"] == "FAIL"
    assert (r["events_read"], r["events_expected"], r["repeated_in_range"]) == (1, 2, 1)
    assert r["ahead"] == 0
    assert r["reasons"] == ["read 1 of 2 events"]


def test_empty_run_passes_with_null_measurements() -> None:
    r = summarize([], frozenset())
    assert (r["result"], r["max_ahead_ms"], r["max_behind_ms"]) == ("PASS", None, None)


def test_v1_manifest_gives_one_json_error_line_and_exit_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """v1 has no offsets, so the run cannot be located in Kafka. No Kafka is contacted."""
    code = main(["--manifest", str(FIXTURES / "v1_run_20261114T131326Z_seed42.json")])
    out = capsys.readouterr().out.strip().splitlines()
    assert code == EXIT_SETUP
    assert len(out) == 1
    report = json.loads(out[0])
    assert (report["result"], report["exit_code"]) == ("ERROR", 2)
    assert "no kafka_offsets" in report["error"]


def test_missing_manifest_file_gives_exit_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--manifest", str(FIXTURES / "does_not_exist.json")]) == EXIT_SETUP
    assert json.loads(capsys.readouterr().out)["result"] == "ERROR"
