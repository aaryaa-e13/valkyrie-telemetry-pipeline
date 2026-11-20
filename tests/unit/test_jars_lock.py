"""Unit tests for docker/spark/jars_lock.py (SHA-256 lock of jars added to Spark).

The lock replaces checksum checks that Ivy does not do (DECISIONS D2.1). These
tests use fake jar files in a temp folder; the real image is tested in
tests/spark/test_jars_lock_image.py.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("jars_lock",
                                                  ROOT / "docker" / "spark" / "jars_lock.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["jars_lock"] = module  # dataclasses need the module registered
    spec.loader.exec_module(module)
    return module


jl = _load()

JARS = {"a-1.0.jar": ("org.example:a:1.0", b"jar A bytes"),
        "b-2.0.jar": ("org.example:b:2.0", b"jar B bytes")}


@pytest.fixture()
def setup(tmp_path: Path) -> tuple[Path, Path, Path]:
    """A jars folder, a fetch list and a matching lock file."""
    jars = tmp_path / "jars"
    jars.mkdir()
    lines = []
    lock = [jl.LOCK_HEADER.rstrip("\n")]
    for name, (coord, data) in JARS.items():
        (jars / name).write_bytes(data)
        lines.append(f"  add   {name} {coord}")
        lock.append(f"{hashlib.sha256(data).hexdigest()}  {name}  {coord}")
    lines.append("  skip  c-3.0.jar org.example:c:3.0 (Spark already ships c-3.1.jar)")
    (tmp_path / "jars-x.txt").write_text("# coordinate: org.example:a:1.0\n" + "\n".join(lines))
    (tmp_path / "jars.sha256").write_text("\n".join(lock) + "\n")
    return jars, tmp_path / "jars-x.txt", tmp_path / "jars.sha256"


def _verify(jars: Path, lst: Path, lock: Path) -> list[str]:
    return jl.verify(jl.parse_lock(lock.read_text()), jl.parse_lists([lst.read_text()]), jars)


def test_matching_lock_passes(setup: tuple[Path, Path, Path]) -> None:
    assert _verify(*setup) == []


def test_one_broken_hash_in_a_temp_copy_fails(setup: tuple[Path, Path, Path],
                                              tmp_path: Path) -> None:
    jars, lst, lock = setup
    broken = tmp_path / "broken.sha256"
    text = lock.read_text()
    first_hash = next(line for line in text.splitlines() if not line.startswith("#"))[:64]
    flipped = ("0" if first_hash[0] != "0" else "1") + first_hash[1:]
    broken.write_text(text.replace(first_hash, flipped, 1))
    problems = _verify(jars, lst, broken)
    assert len(problems) == 1 and problems[0].startswith("MISMATCH a-1.0.jar: sha256")
    # ... and through the CLI, as the Docker build runs it: exit code 1.
    assert jl.main(["verify", "--lock", str(broken), "--jars-dir", str(jars), str(lst)]) == 1
    assert jl.main(["verify", "--lock", str(lock), "--jars-dir", str(jars), str(lst)]) == 0


def test_changed_jar_content_fails(setup: tuple[Path, Path, Path]) -> None:
    jars, lst, lock = setup
    (jars / "b-2.0.jar").write_bytes(b"tampered")
    assert [p.split(":")[0] for p in _verify(jars, lst, lock)] == ["MISMATCH b-2.0.jar"]


def test_unlisted_jar_fails(setup: tuple[Path, Path, Path]) -> None:
    jars, lst, lock = setup
    (jars / "new-1.0.jar").write_bytes(b"new")
    lst.write_text(lst.read_text() + "\n  add   new-1.0.jar org.example:new:1.0\n")
    assert _verify(jars, lst, lock) == [
        "UNLISTED new-1.0.jar (org.example:new:1.0) was added but is not in the lock"]


def test_listed_jar_not_added_fails(setup: tuple[Path, Path, Path]) -> None:
    jars, lst, lock = setup
    lst.write_text("\n".join(line for line in lst.read_text().splitlines()
                             if "b-2.0.jar" not in line))
    assert _verify(jars, lst, lock) == ["MISSING  b-2.0.jar is in the lock but was not added"]


def test_listed_jar_missing_from_folder_fails(setup: tuple[Path, Path, Path]) -> None:
    jars, lst, lock = setup
    (jars / "a-1.0.jar").unlink()
    assert _verify(jars, lst, lock)[0].startswith("MISSING  a-1.0.jar was added but is not")


def test_coordinate_change_fails(setup: tuple[Path, Path, Path]) -> None:
    jars, lst, lock = setup
    lst.write_text(lst.read_text().replace("org.example:a:1.0", "org.evil:a:1.0"))
    assert _verify(jars, lst, lock) == [
        "MISMATCH a-1.0.jar: coordinate org.evil:a:1.0, lock says org.example:a:1.0"]


def test_marker_written_only_on_success(setup: tuple[Path, Path, Path],
                                        tmp_path: Path) -> None:
    jars, lst, lock = setup
    marker = tmp_path / "ok.txt"
    args = ["verify", "--lock", str(lock), "--jars-dir", str(jars), "--marker", str(marker),
            str(lst)]
    (jars / "a-1.0.jar").write_bytes(b"tampered")
    assert jl.main(args) == 1 and not marker.exists()


@pytest.mark.parametrize("bad", ["abc  x.jar  g:a:1", f"{'0' * 64}  x.jar"])
def test_malformed_lock_line_rejected(bad: str) -> None:
    with pytest.raises(ValueError, match="malformed"):
        jl.parse_lock(bad)


def test_add_line_without_coordinate_rejected() -> None:
    with pytest.raises(ValueError, match="without group:artifact:version"):
        jl.parse_lists(["  add   a-1.0.jar"])


def test_central_url_layout() -> None:
    assert jl.central_sha1_url("io.delta:delta-spark_4.1_2.13:4.4.1") == (
        "https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/4.4.1/"
        "delta-spark_4.1_2.13-4.4.1.jar.sha1")


def test_fetch_refuses_plain_http() -> None:
    with pytest.raises(ValueError, match="non-HTTPS"):
        jl.fetch_central_sha1("http://repo1.maven.org/x.jar.sha1")


def test_generate_cross_checks_against_central(setup: tuple[Path, Path, Path]) -> None:
    jars, lst, lock = setup
    added = jl.parse_lists([lst.read_text()])
    sha1 = {"/a/1.0/a-1.0.jar.sha1": hashlib.sha1(JARS["a-1.0.jar"][1]).hexdigest(),
            "/b/2.0/b-2.0.jar.sha1": hashlib.sha1(JARS["b-2.0.jar"][1]).hexdigest()}

    def fake_fetch(url: str) -> str:
        return next(v for k, v in sha1.items() if url.endswith(k))

    text, rows, ok = jl.generate(added, jars, fetch=fake_fetch)
    assert ok and text == lock.read_text()  # regenerated lock == the one we built
    assert all(r.startswith("OK") for r in rows)

    def wrong_fetch(url: str) -> str:
        return "0" * 40

    _, rows, ok = jl.generate(added, jars, fetch=wrong_fetch)
    assert not ok and all(r.startswith("DIFF") for r in rows)

    def failing_fetch(url: str) -> str:
        raise OSError("network down")

    _, rows, ok = jl.generate(added, jars, fetch=failing_fetch)
    assert not ok and "ERROR OSError" in rows[0]
