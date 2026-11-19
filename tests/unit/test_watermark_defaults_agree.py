"""Drift check (D3): the simulator's assumed W and the silver job's W agree.

No shared code on purpose (the simulator image does not ship streaming/): the
three defaults are checked against each other here instead.
"""

from __future__ import annotations

import re
from pathlib import Path

from simulator.config import SimulatorConfig, load_config
from simulator.schedule import LatenessConfig
from streaming.config import SilverJobConfig


def test_simulator_job_and_compose_defaults_are_all_60() -> None:
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")
    m = re.search(r"WATERMARK_DELAY_S: \$\{WATERMARK_DELAY_S:-(\d+)\}", compose)
    assert m is not None, "docker-compose.yml has no WATERMARK_DELAY_S default"
    values = {
        "SilverJobConfig": SilverJobConfig().watermark_delay_s,
        "compose": int(m.group(1)),
        "SimulatorConfig": SimulatorConfig().assumed_watermark_s,
        "LatenessConfig": LatenessConfig().assumed_watermark_s,
        "config.yaml": load_config("simulator/config.yaml", env={}).assumed_watermark_s,
        "config.tiny.yaml": load_config("simulator/config.tiny.yaml", env={}).assumed_watermark_s,
    }
    assert set(values.values()) == {60}, values
