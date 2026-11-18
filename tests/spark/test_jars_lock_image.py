"""The jar SHA-256 lock against the REAL image (runs in the Spark container).

Uses the copies baked into the image (/opt/valkyrie): the lock the build verified,
the fetch lists and Spark's jars folder. Proves the build check passes on the
real jars and fails when one hash is broken in a temp copy (DECISIONS D2.1).

Run with: make test-spark
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

pyspark = pytest.importorskip("pyspark")  # skip cleanly outside the Spark image

LOCK_DIR = Path("/opt/valkyrie")
if not (LOCK_DIR / "jars_lock.py").is_file():
    pytest.skip("not in the Spark image (no /opt/valkyrie)", allow_module_level=True)

JARS_DIR = Path(pyspark.__file__).parent / "jars"
LISTS = sorted(LOCK_DIR.glob("jars-*.txt"))


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("jars_lock_image", LOCK_DIR / "jars_lock.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["jars_lock_image"] = module
    spec.loader.exec_module(module)
    return module


jl = _load()


def _args(lock: Path) -> list[str]:
    return ["verify", "--lock", str(lock), "--jars-dir", str(JARS_DIR), *map(str, LISTS)]


def test_real_lock_matches_real_jars() -> None:
    assert len(LISTS) == 3  # kafka, delta, jackson
    assert jl.main(_args(LOCK_DIR / "jars.sha256")) == 0
    assert (LOCK_DIR / "lock-verified.txt").is_file()


def test_one_broken_hash_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    text = (LOCK_DIR / "jars.sha256").read_text()
    line = next(x for x in text.splitlines() if "delta-spark_4.1_2.13" in x)
    broken_hash = ("f" if line[0] != "f" else "e") + line[1:64]
    broken = tmp_path / "jars.sha256"
    broken.write_text(text.replace(line[:64], broken_hash, 1))
    assert jl.main(_args(broken)) == 1
    err = capsys.readouterr().err
    assert "MISMATCH delta-spark_4.1_2.13-4.4.1.jar: sha256" in err
