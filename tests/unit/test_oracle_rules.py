"""Pure oracle rules (streaming/oracle.py): R6c's formula, the gates' boundaries."""

from __future__ import annotations

from streaming.oracle import US, ControlRow, isolation_gate, r6c


def row(b: int, wm: int | None, m: int | None, snap: bool = False,
        w: int | None = 60) -> ControlRow:
    return ControlRow("q", b, snap, w, None if wm is None else wm * US,
                      None if m is None else m * US)


def test_r6c_formula_snapshot_and_pre_slice4_rows() -> None:
    rows = [row(0, None, None, w=None),          # pre-slice-4: skipped, NULL max ignored
            row(1, None, 100, snap=True),        # snapshot: wm -inf
            row(2, 40, None),                    # 100 - 60; a batch with no keyed row
            row(3, 40, 90),                      # max stays 100
            row(4, 40, 200)]
    assert r6c(rows) == {"batches_checked": 4, "pre_slice4_batches": 1,
                         "formula_mismatches": 0, "goes_down": 0, "examples": []}
    bad = r6c([*rows, row(5, 141, None)])          # should be 200 - 60 = 140
    assert (bad["formula_mismatches"], bad["examples"][0]["expected_us"]) == (1, 140 * US)


def test_r6c_snapshot_status_null_is_a_mismatch() -> None:
    assert r6c([ControlRow("q", 0, None, 60, None, None)])["formula_mismatches"] == 1


def test_isolation_boundary_is_strict() -> None:
    assert isolation_gate(60, 0, 60 * US)["checkable"] is False        # == min_t + W
    assert isolation_gate(60, 0, 60 * US - 1)["checkable"] is True
    assert isolation_gate(60, 0, None)["checkable"] is True            # no foreign row
    assert isolation_gate(None, 0, 0)["reason"].startswith("no W")
