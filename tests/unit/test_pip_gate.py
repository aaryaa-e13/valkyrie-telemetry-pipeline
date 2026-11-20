"""Unit tests for docker/spark/pip_gate.py (pip freeze must equal the lock file)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("pip_gate",
                                                  ROOT / "docker" / "spark" / "pip_gate.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pg = _load()
PINS = {"pyspark": "4.1.4", "py4j": "0.10.9.9", "delta-spark": "4.4.1"}


def test_identical_sets_pass() -> None:
    assert pg.compare(PINS, dict(PINS), {"pyspark": "4.1.4"}) == []


def test_moved_pyspark_is_reported_twice() -> None:
    installed = {**PINS, "pyspark": "4.2.0"}
    problems = pg.compare(PINS, installed, {"pyspark": "4.1.4"})
    assert problems == ["EXPECT pyspark==4.1.4, installed: 4.2.0",
                        "MOVED pyspark: pinned 4.1.4, installed 4.2.0"]


def test_unpinned_and_missing_packages() -> None:
    installed = {k: v for k, v in PINS.items() if k != "py4j"} | {"zipp": "4.1.2"}
    assert pg.compare(PINS, installed, {}) == [
        "MISSING py4j==0.10.9.9 is pinned but not installed",
        "UNPINNED zipp==4.1.2 is installed but not in the lock file"]


def test_names_are_normalised() -> None:
    pins = pg.parse_pins(["Pygments==2.21.0", "importlib_metadata==8.7.1  # comment", ""])
    freeze = pg.parse_pins(["pygments==2.21.0", "importlib-metadata==8.7.1"])
    assert pins == freeze == {"pygments": "2.21.0", "importlib-metadata": "8.7.1"}


@pytest.mark.parametrize("line", ["pyspark>=4.1", "-r other.txt", "-e ."])
def test_non_exact_pins_rejected(line: str) -> None:
    with pytest.raises(ValueError, match="not an exact pin"):
        pg.parse_pins([line])


def test_real_lock_file_is_all_exact_pins() -> None:
    pins = pg.parse_pins((ROOT / "requirements" / "spark.txt").read_text().splitlines())
    assert pins["pyspark"] == "4.1.4" and pins["delta-spark"] == "4.4.1"
