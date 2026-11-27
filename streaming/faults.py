"""TEST-ONLY fault injection at the silver write boundaries (Phase 2 slice 6, DECISIONS D2.6).

Inert unless BOTH are set, and they are set only by docker-compose.faults.yml
(used only by e2e/smoke_faults.py; the default compose file cannot set them):

    VALKYRIE_FAULTS_ENABLED=1                  exactly "1"
    VALKYRIE_FAULT_POINT=<point>               one of POINTS
    VALKYRIE_FAULT_TAG=<run tag>               names the marker folder (required when active)
    VALKYRIE_FAULT_DIR                          marker root, default /data/faults

The environment is read ONCE, by configure() at start-up (streaming/pipeline.py).
When inactive, hit() is one comparison of a module global: no environment read, no
file I/O, no marker folder (condition K4).

When active, the first silver batch with late AND on-time rows that reaches the
point (and whose marker does not exist yet) does NOT continue: hit() writes the
marker `<root>/<tag>/<point>.reached` (JSON, fsync'd), logs `FAULT REACHED`, and
holds the batch still. e2e/smoke_faults.py sees the marker and SIGKILLs the whole
container from outside (`docker kill --signal KILL`): no finally, no atexit, no JVM
shutdown hook. If no kill arrives within HOLD_S, the hook logs `FAULT FALLBACK`
and SIGKILLs the JVM and itself (smoke-faults then reports exit 6).

The marker also makes the hook fire at most once per tag: a replayed batch, or a
restart with the env still set, passes through.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

log = logging.getLogger("streaming.faults")

ENABLED_VAR = "VALKYRIE_FAULTS_ENABLED"
POINT_VAR = "VALKYRIE_FAULT_POINT"
TAG_VAR = "VALKYRIE_FAULT_TAG"
DIR_VAR = "VALKYRIE_FAULT_DIR"
DEFAULT_DIR = "/data/faults"
POINTS = ("after_read", "after_dlq", "after_late", "after_silver", "after_control")
HOLD_S = 120.0


class FaultSetupError(ValueError):
    """The fault env is clearly meant to be on but is unusable (start-up exits 2)."""


@dataclass(frozen=True)
class FaultConfig:
    point: str
    marker_dir: Path
    hold_s: float = HOLD_S

    @property
    def marker(self) -> Path:
        return self.marker_dir / f"{self.point}.reached"


def load(env: Mapping[str, str]) -> FaultConfig | None:
    """None (inert) unless ENABLED == "1"; then the point and tag must be valid."""
    if env.get(ENABLED_VAR) != "1":
        return None
    point, tag = env.get(POINT_VAR, ""), env.get(TAG_VAR, "")
    if point not in POINTS:
        raise FaultSetupError(f"{ENABLED_VAR}=1 but {POINT_VAR}={point!r} is not one of "
                              f"{list(POINTS)}")
    if not tag or "/" in tag or tag.startswith("."):
        raise FaultSetupError(f"{ENABLED_VAR}=1 but {TAG_VAR}={tag!r} is not a folder name")
    return FaultConfig(point, Path(env.get(DIR_VAR) or DEFAULT_DIR) / tag)


_active: FaultConfig | None = None


def configure(env: Mapping[str, str] | None = None) -> FaultConfig | None:
    """Read the fault env once (start-up). Raises FaultSetupError (-> exit 2)."""
    global _active
    env = os.environ if env is None else env
    cfg = load(env)
    _active = cfg
    if cfg is not None:
        msg = (f"FAULT INJECTION ACTIVE point={cfg.point} markers={cfg.marker_dir} "
               "(TEST ONLY; set by docker-compose.faults.yml)")
        log.warning(msg, extra={"fields": {"point": cfg.point,
                                           "marker_dir": str(cfg.marker_dir)}})
        print(f"!!! {msg} !!!", file=sys.stderr, flush=True)
    elif env.get(ENABLED_VAR) is not None or env.get(POINT_VAR) is not None:
        log.warning("fault env partially set; hooks are inert", extra={"fields": {
            ENABLED_VAR: env.get(ENABLED_VAR), POINT_VAR: env.get(POINT_VAR)}})
    return cfg


def hit(point: str, batch_id: int, late_rows: int, ontime_rows: int) -> None:
    """A write boundary of silver batch `batch_id`. No-op unless active for `point`."""
    cfg = _active
    if cfg is None or cfg.point != point:
        return
    if late_rows <= 0 or ontime_rows <= 0:
        return
    fire(cfg, batch_id, late_rows, ontime_rows)


def fire(cfg: FaultConfig, batch_id: int, late_rows: int, ontime_rows: int,
         killer: Callable[[], None] | None = None, sleep: Callable[[float], None] = time.sleep,
         clock: Callable[[], float] = time.monotonic) -> bool:
    """Write the marker and hold the batch until killed. False if it fired before."""
    if cfg.marker.exists():
        return False
    cfg.marker_dir.mkdir(parents=True, exist_ok=True)
    record = {"point": cfg.point, "batch_id": int(batch_id), "late_rows": int(late_rows),
              "ontime_rows": int(ontime_rows), "pid": os.getpid(),
              "at": datetime.now(UTC).isoformat()}
    tmp = cfg.marker.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(record, fh)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(cfg.marker)
    log.warning(f"FAULT REACHED point={cfg.point} batch={batch_id}", extra={"fields": record})
    deadline = clock() + cfg.hold_s
    while clock() < deadline:
        sleep(0.05)
    log.error(f"FAULT FALLBACK point={cfg.point} batch={batch_id}: no external kill within "
              f"{cfg.hold_s:.0f} s; killing the JVM and this process")
    (killer or _kill_jvm_and_self)()
    return True


def _kill_jvm_and_self() -> None:  # pragma: no cover (kills the test runner)
    try:
        from pyspark import SparkContext

        gw = SparkContext._gateway
        if gw is not None and getattr(gw, "proc", None) is not None:
            os.kill(gw.proc.pid, signal.SIGKILL)
    finally:
        os.kill(os.getpid(), signal.SIGKILL)
