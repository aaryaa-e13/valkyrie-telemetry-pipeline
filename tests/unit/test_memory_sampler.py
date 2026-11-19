"""Unit tests for e2e.memory_sampler (parsing and thresholds, no Docker)."""

from __future__ import annotations

import pytest

from e2e.memory_sampler import GIB, parse_mem, summarize


@pytest.mark.parametrize(("text", "expected"), [
    ("1.09GiB / 3GiB", int(1.09 * GIB)), ("835.1MiB / 3GiB", int(835.1 * 1024 ** 2)),
    ("512KiB / 1GiB", 512 * 1024)])
def test_parse_mem(text: str, expected: int) -> None:
    assert parse_mem(text) == expected


def test_summary_within_and_over_thresholds() -> None:
    ok = summarize([int(1.1 * GIB), int(1.4 * GIB), int(1.3 * GIB)],
                   {"measured": True, "heap_peak_bytes": int(0.8 * GIB), "gc_ms": 2_000,
                    "uptime_ms": 100_000}, {"exit_code": 0})
    assert (ok["container_peak_gib"], ok["container_end_gib"], ok["over_thresholds"]) == (
        1.4, 1.3, [])
    bad = summarize([int(2.6 * GIB)], {"measured": True, "heap_peak_bytes": int(1.7 * GIB),
                                       "gc_ms": 20_000, "uptime_ms": 100_000},
                    {"oom_killed": True, "exit_code": 137})
    assert len(bad["over_thresholds"]) == 4
    none = summarize([], {"measured": False, "reason": "down"}, {})
    assert none["container_peak_gib"] is None and none["over_thresholds"] == []
