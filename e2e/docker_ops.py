"""Thin subprocess wrappers around `docker` / `docker compose` (laptop side).

Every call has a timeout and raises CommandError with the command and its
output, so failures are visible instead of hanging or being swallowed.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass

from e2e.catchup import ContainerState, parse_timestamp

COMPOSE = ("docker", "compose")


class CommandError(RuntimeError):
    """A docker command failed or timed out."""


@dataclass(frozen=True)
class Result:
    """Outcome of a finished command."""

    returncode: int
    stdout: str
    stderr: str


def run(cmd: Sequence[str], *, timeout: float, stream: bool = False) -> Result:
    """Run `cmd`. With stream=True, output goes straight to the terminal.

    Raises:
        CommandError: The command could not be started or timed out.
    """
    try:
        proc = subprocess.run(
            list(cmd), timeout=timeout, text=True,
            capture_output=not stream, check=False,
        )
    except FileNotFoundError as exc:
        raise CommandError(f"cannot run {cmd[0]!r}: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise CommandError(f"timed out after {timeout:.0f}s: {' '.join(cmd)}") from exc
    return Result(proc.returncode, proc.stdout or "", proc.stderr or "")


def is_missing_container_error(stderr: str) -> bool:
    """True if `docker inspect` failed because the container does not exist.

    Docker versions differ: "Error: No such object: x" vs "error: no such object: x".
    """
    return "no such object" in stderr.lower() or "no such container" in stderr.lower()


def container_state(name: str) -> ContainerState:
    """Return the container's state; exists=False if there is no such container."""
    res = run(["docker", "inspect", "--format", "{{json .State}}", name], timeout=20)
    if res.returncode != 0:
        if is_missing_container_error(res.stderr):
            return ContainerState(exists=False)
        raise CommandError(f"docker inspect {name} failed: {res.stderr.strip()}")
    state = json.loads(res.stdout)
    return ContainerState(
        exists=True,
        status=str(state.get("Status", "unknown")),
        exit_code=state.get("ExitCode"),
        oom_killed=bool(state.get("OOMKilled")),
        started_at=parse_timestamp(_trim_nanos(state.get("StartedAt"))),
    )


def _trim_nanos(value: str | None) -> str | None:
    """Docker prints 9 fractional digits; Python 3.11 parses at most 6."""
    if not value or "." not in value:
        return value
    head, frac = value.split(".", 1)
    digits = "".join(ch for ch in frac if ch.isdigit())
    suffix = frac[len(digits):]
    return f"{head}.{digits[:6]}{suffix}"


def container_logs(name: str, *, tail: int | None = None, since: float | None = None) -> list[str]:
    """Return log lines (stdout and stderr merged). Empty list if unavailable."""
    cmd = ["docker", "logs"]
    if tail is not None:
        cmd += ["--tail", str(tail)]
    if since is not None:
        cmd += ["--since", f"{since:.3f}"]
    res = run([*cmd, name], timeout=30)
    if res.returncode != 0:
        return []
    return (res.stdout + res.stderr).splitlines()


def compose_up_spark() -> Result:
    """Start the streaming job (no-op if it is already running)."""
    return run([*COMPOSE, "--profile", "spark", "up", "-d", "spark"], timeout=300)


def compose_stop_spark() -> Result:
    """Graceful stop: SIGTERM, then up to stop_grace_period (60 s) before SIGKILL."""
    return run([*COMPOSE, "--profile", "spark", "stop", "spark"], timeout=180)


def topic_end_offsets_raw(container: str, topic: str, bootstrap: str = "kafka:29092") -> str:
    """Latest offsets per partition as printed by kafka-get-offsets.sh ("topic:p:offset").

    Raises:
        CommandError: The tool failed (e.g. Kafka down, unknown topic).
    """
    res = run(["docker", "exec", container, "/opt/kafka/bin/kafka-get-offsets.sh",
               "--bootstrap-server", bootstrap, "--topic", topic, "--time", "-1"], timeout=60)
    if res.returncode != 0:
        raise CommandError(f"kafka-get-offsets failed: {(res.stderr or res.stdout).strip()}")
    return res.stdout


def run_simulator(config: str, seed: int, extra: Sequence[str], *, stream: bool) -> Result:
    """Run one simulator container to completion (image built by `make e2e-images`)."""
    cmd = [*COMPOSE, "--profile", "sim", "run", "--rm", "-T", "simulator",
           "--config", config, "--seed", str(seed), *extra]
    return run(cmd, timeout=900, stream=stream)


def run_verify(manifest_rel: str) -> Result:
    """Run verify_manifest in the read-only spark-tools container."""
    cmd = [*COMPOSE, "--profile", "tools", "run", "--rm", "-T", "spark-tools",
           "python", "-m", "streaming.verify_manifest",
           "--manifest", f"/app/{manifest_rel}", "--fail-on-duplicates"]
    return run(cmd, timeout=600)
