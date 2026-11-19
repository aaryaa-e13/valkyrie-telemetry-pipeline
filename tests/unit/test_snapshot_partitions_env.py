"""Unit tests for streaming.config.delta_snapshot_partitions_from_env."""

from __future__ import annotations

import pytest

from streaming.config import delta_snapshot_partitions_from_env


@pytest.mark.parametrize(("env", "expected"), [
    ({}, None), ({"DELTA_SNAPSHOT_PARTITIONS": ""}, None),
    ({"DELTA_SNAPSHOT_PARTITIONS": "4"}, 4), ({"DELTA_SNAPSHOT_PARTITIONS": " 8 "}, 8)])
def test_values(env: dict[str, str], expected: int | None) -> None:
    assert delta_snapshot_partitions_from_env(env) == expected


@pytest.mark.parametrize("bad", ["0", "-2", "four", "1.5"])
def test_invalid_is_refused(bad: str) -> None:
    with pytest.raises(ValueError, match="DELTA_SNAPSHOT_PARTITIONS"):
        delta_snapshot_partitions_from_env({"DELTA_SNAPSHOT_PARTITIONS": bad})
