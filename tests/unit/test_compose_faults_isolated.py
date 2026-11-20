"""Normal compose can never enable the fault hooks (slice 6, F1).

The hooks need VALKYRIE_FAULTS_ENABLED=1 inside the Spark container. Compose passes
into a container only the variables its service lists (or an env_file), so a
variable exported in a shell or written in .env cannot reach the job unless a
compose file lists it. Only docker-compose.faults.yml does, and only
e2e/smoke_faults.py loads that file.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
AUTO_LOADED = ("docker-compose.override.yml", "docker-compose.override.yaml",
               "compose.yaml", "compose.yml", "compose.override.yml", "compose.override.yaml")


def test_default_compose_file_has_no_fault_variable_and_no_env_file() -> None:
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "VALKYRIE_FAULT" not in text
    services = yaml.safe_load(text)["services"]
    assert [name for name, svc in services.items() if "env_file" in svc] == []


def test_no_auto_loaded_override_file_exists() -> None:
    assert [n for n in AUTO_LOADED if (ROOT / n).exists()] == []


def test_env_example_cannot_add_compose_files_or_fault_variables() -> None:
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "COMPOSE_FILE" not in text and "VALKYRIE_FAULT" not in text


def test_faults_file_only_adds_the_fault_variables_to_spark() -> None:
    data = yaml.safe_load((ROOT / "docker-compose.faults.yml").read_text(encoding="utf-8"))
    assert list(data) == ["services"] and list(data["services"]) == ["spark"]
    assert list(data["services"]["spark"]) == ["environment"]
    assert sorted(data["services"]["spark"]["environment"]) == [
        "VALKYRIE_FAULTS_ENABLED", "VALKYRIE_FAULT_POINT", "VALKYRIE_FAULT_TAG"]


USE = re.compile(r"""(["'])docker-compose\.faults\.yml\1|-f\s+docker-compose\.faults\.yml""")


def test_only_smoke_faults_loads_the_faults_file() -> None:
    """Tracked files that USE docker-compose.faults.yml: a quoted path or `-f <file>`
    (prose mentions in docstrings and docs do not load it)."""
    files = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True,
                           check=True).stdout.split()
    users = {f for f in files
             if not f.startswith("docs/") and f != "tests/unit/test_compose_faults_isolated.py"
             and (ROOT / f).is_file() and USE.search((ROOT / f).read_text(errors="ignore"))}
    assert users <= {"e2e/smoke_faults.py"}, users
    assert USE.search('FAULTS_FILE = "docker-compose.faults.yml"')       # the pattern works
    assert USE.search("docker compose -f docker-compose.faults.yml up")
