"""Build-time gate: the installed Python packages must equal the pinned lock file.

Run in the Spark image right after `pip install -r requirements/spark.txt`:

    python pip_gate.py requirements/spark.txt --expect pyspark==4.1.4

It compares `pip freeze` with the pins and fails (exit 1) if

  * a pinned package is missing or installed at another version (for example
    pip's resolver moved pyspark to satisfy a new dependency), or
  * a package is installed that is not pinned (an unpinned transitive dep), or
  * an --expect package is not at exactly that version (named separately so
    the build log says plainly "pyspark moved").

Why: pyspark must stay at the version the Kafka connector and the Delta jar
were built for (D1.1, D2.1). A resolver that silently moves it would give a
classpath mismatch that only shows up at runtime. Standard library only.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

_PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;#]+)$")


def normalize(name: str) -> str:
    """PEP 503 name normalisation: `Pygments`, `importlib_metadata` -> canonical form."""
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_pins(lines: Iterable[str]) -> dict[str, str]:
    """Parse `name==version` lines into {normalised name: version}.

    Comments and blank lines are ignored. Any other line (a range, an `-r`
    include, an editable install) is rejected, because the lock file must pin
    every package exactly.

    Raises:
        ValueError: A line is not an exact pin, or a package is pinned twice.
    """
    pins: dict[str, str] = {}
    for raw in lines:
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        match = _PIN.match(line)
        if not match:
            raise ValueError(f"not an exact pin: {raw.strip()!r}")
        name = normalize(match.group(1))
        if name in pins:
            raise ValueError(f"pinned twice: {match.group(1)}")
        pins[name] = match.group(2)
    return pins


def compare(pinned: dict[str, str], installed: dict[str, str],
            expect: dict[str, str]) -> list[str]:
    """Return human-readable problems (empty list = gate passes)."""
    problems: list[str] = []
    for name, version in sorted(expect.items()):
        got = installed.get(name)
        if got != version:
            problems.append(f"EXPECT {name}=={version}, installed: {got or 'missing'}")
    for name in sorted(pinned.keys() | installed.keys()):
        want, got = pinned.get(name), installed.get(name)
        if want == got:
            continue
        if want is None:
            problems.append(f"UNPINNED {name}=={got} is installed but not in the lock file")
        elif got is None:
            problems.append(f"MISSING {name}=={want} is pinned but not installed")
        else:
            problems.append(f"MOVED {name}: pinned {want}, installed {got}")
    return problems


def pip_freeze() -> list[str]:
    """`pip freeze` of the running interpreter (excludes pip/setuptools/wheel)."""
    out = subprocess.run([sys.executable, "-m", "pip", "freeze"], check=True,
                         capture_output=True, text=True).stdout
    return out.splitlines()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("lock_file", type=Path)
    p.add_argument("--expect", action="append", default=[], metavar="NAME==VERSION")
    args = p.parse_args(argv)

    pinned = parse_pins(args.lock_file.read_text().splitlines())
    installed = parse_pins(pip_freeze())
    expect = parse_pins(args.expect)
    problems = compare(pinned, installed, expect)
    if problems:
        print("pip gate FAILED:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    print(f"pip gate OK: {len(installed)} packages installed == {len(pinned)} pinned; "
          + ", ".join(f"{k}=={v}" for k, v in sorted(expect.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
