"""The test-only fault hook (streaming/faults.py, slice 6): inert unless BOTH vars are set.

K4: when inactive, hit() reads no environment, does no file I/O and creates no folder.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from streaming import faults

ON = {"VALKYRIE_FAULTS_ENABLED": "1", "VALKYRIE_FAULT_POINT": "after_late",
      "VALKYRIE_FAULT_TAG": "t1"}


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    yield
    faults._active = None


class NoEnv(dict[str, str]):
    """An environment that fails the test if anything reads it."""

    def get(self, *a: Any, **k: Any) -> Any:
        raise AssertionError("environment read while inactive")

    __getitem__ = get


@pytest.mark.parametrize("env", [
    {}, {"VALKYRIE_FAULT_POINT": "after_late"}, {"VALKYRIE_FAULT_TAG": "t1"},
    {**ON, "VALKYRIE_FAULTS_ENABLED": "true"}, {**ON, "VALKYRIE_FAULTS_ENABLED": "yes"},
    {**ON, "VALKYRIE_FAULTS_ENABLED": "0"}, {**ON, "VALKYRIE_FAULTS_ENABLED": ""},
], ids=["none", "point-only", "tag-only", "true", "yes", "zero", "empty"])
def test_inert_unless_both_are_set(env: dict[str, str]) -> None:
    assert faults.load(env) is None


def test_active_only_with_both() -> None:
    cfg = faults.load({**ON, "VALKYRIE_FAULT_DIR": "/x"})
    assert cfg is not None and cfg.point == "after_late"
    assert cfg.marker == Path("/x/t1/after_late.reached")
    assert faults.load(ON).marker_dir == Path("/data/faults/t1")  # type: ignore[union-attr]


@pytest.mark.parametrize("env", [
    {"VALKYRIE_FAULTS_ENABLED": "1"},             # enabled, no point: never silently inert
    {**ON, "VALKYRIE_FAULT_POINT": "inside_spark"}, {**ON, "VALKYRIE_FAULT_POINT": ""},
    {**ON, "VALKYRIE_FAULT_TAG": ""}, {**ON, "VALKYRIE_FAULT_TAG": "../x"},
])
def test_enabled_with_a_bad_point_or_tag_refuses(env: dict[str, str]) -> None:
    with pytest.raises(faults.FaultSetupError):
        faults.load(env)


def test_inert_run_creates_no_faults_dir_and_reads_no_env(tmp_path: Path,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    """K4: configure() with no fault env; then every hook call is free."""
    assert faults.configure({"VALKYRIE_FAULT_DIR": str(tmp_path / "faults")}) is None
    monkeypatch.setattr(faults.os, "environ", NoEnv())
    monkeypatch.setattr(faults, "fire", lambda *a, **k: pytest.fail("fired while inert"))
    for point in faults.POINTS:
        for batch in range(50):
            faults.hit(point, batch, 5, 5)
    assert not (tmp_path / "faults").exists()
    assert list(tmp_path.iterdir()) == []


def test_partial_env_warns_and_stays_inert(caplog: pytest.LogCaptureFixture) -> None:
    assert faults.configure({"VALKYRIE_FAULT_POINT": "after_late"}) is None
    assert "partially set" in caplog.text


def test_active_warns_loudly(caplog: pytest.LogCaptureFixture,
                             capsys: pytest.CaptureFixture[str]) -> None:
    faults.configure(ON)
    assert "FAULT INJECTION ACTIVE point=after_late" in caplog.text
    assert "FAULT INJECTION ACTIVE" in capsys.readouterr().err


def test_hit_fires_only_at_its_point_and_with_both_kinds(monkeypatch: pytest.MonkeyPatch,
                                                         tmp_path: Path) -> None:
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(faults, "fire", lambda *a, **k: calls.append(a))
    faults.configure({**ON, "VALKYRIE_FAULT_DIR": str(tmp_path)})
    faults.hit("after_silver", 3, 5, 5)       # other point
    faults.hit("after_late", 3, 0, 5)         # no late rows
    faults.hit("after_late", 3, 5, 0)         # no on-time rows
    assert calls == []
    faults.hit("after_late", 3, 5, 7)
    assert [(c[1], c[2], c[3]) for c in calls] == [(3, 5, 7)]


def test_fire_writes_the_marker_holds_then_falls_back_once(tmp_path: Path) -> None:
    cfg = faults.FaultConfig("after_silver", tmp_path / "t1", hold_s=1.0)
    t = [0.0]
    sleeps: list[float] = []
    killed: list[bool] = []

    def sleep(s: float) -> None:
        sleeps.append(s)
        t[0] += s

    assert faults.fire(cfg, 9, 2, 3, killer=lambda: killed.append(True), sleep=sleep,
                       clock=lambda: t[0]) is True
    rec = json.loads(cfg.marker.read_text())
    assert (rec["point"], rec["batch_id"], rec["late_rows"], rec["ontime_rows"]) == \
        ("after_silver", 9, 2, 3)
    assert killed == [True] and 19 <= len(sleeps) <= 21      # held ~1 s in 50 ms steps
    # The marker makes it fire at most once (a replay or a restart passes through).
    assert faults.fire(cfg, 9, 2, 3, killer=lambda: killed.append(True)) is False
    assert killed == [True]
    assert sorted(p.name for p in cfg.marker_dir.iterdir()) == ["after_silver.reached"]
