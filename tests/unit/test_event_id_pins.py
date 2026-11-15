"""Regression pins: event_ids captured from the code BEFORE Phase 1 slice (e).

Slice (e) adds pacing and deliberate lateness. Neither may change what an
event *is*: the same seed must keep producing the same event_ids, because
event_id is the Phase 2 dedupe key and the manifest's ground truth.

These values were captured on 2026-11-15 from commit 5175f95 (default config,
num_matches=2). If a test here fails, generation changed: stop and find out
why before touching the pinned values.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from simulator.config import SimulatorConfig
from simulator.generator import generate_events
from simulator.manifest import event_ids_checksum

START = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)

# seed -> (number of events, first five event_ids in send order, checksum of all ids)
PINS: dict[int, tuple[int, list[str], str]] = {
    42: (
        109,
        [
            "b94f52aa-6a40-5de2-a491-f9642c310b98",
            "22694ab2-62cb-5cb0-a987-b733c67fcc7d",
            "3948ef92-70e5-5029-be31-512c0cf257e4",
            "d54a1f45-05d6-5006-9ca2-fa9d52b25fb4",
            "6cc4c3a8-177d-5d4d-a19b-813c0604aedf",
        ],
        "811087b5b6bfadeb5df774b5880ed49126daeaa959066446d4974be729d19261",
    ),
    7: (
        126,
        [
            "fd5c5ef7-6d32-51f8-9149-44cad88fc9cd",
            "92c6e7a4-f073-5748-9750-f0ff6188c9c9",
            "bdf323d7-0d80-5d7c-bcee-422a4623aa92",
            "b25809c0-122c-532f-adf6-8a2ee1b7f3ac",
            "d4871955-9d52-549d-b4e7-3f7243643d2d",
        ],
        "c50a89f85c8bb77078d780cdccc90598ba169d465b3ff54f399c00e406ba695d",
    ),
}


@pytest.mark.parametrize("seed", sorted(PINS))
def test_event_ids_are_unchanged_since_before_slice_e(seed: int) -> None:
    count, first_five, checksum = PINS[seed]
    events = generate_events(SimulatorConfig(seed=seed, num_matches=2), START)
    assert len(events) == count
    assert [e.event_id for e in events[:5]] == first_five
    assert event_ids_checksum(e.event_id for e in events) == checksum
