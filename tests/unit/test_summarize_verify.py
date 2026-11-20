"""The smoke SUMMARY line carries the R4-R6 verdicts and both gates (slice 5)."""

from __future__ import annotations

from e2e.orchestrator import summarize_verify


def test_oracle_verdicts_and_gates_in_the_summary() -> None:
    v = {"R4": "not checkable: isolation: not isolated: ...", "R6a": "PASS"}
    rep = {"silver": {"oracle": {"verdicts": v, "w_check": {"reason": None, "w_s": 60},
                                 "isolation": {"reason": "not isolated: ...",
                                               "margin_ms": -5}}}}
    assert summarize_verify(rep)["silver"]["oracle"] == {
        "verdicts": v, "w_check": 60, "isolation_margin_ms": -5,
        "isolation": "not isolated: ..."}
    assert summarize_verify({})["silver"]["oracle"] == {
        "verdicts": None, "w_check": None, "isolation_margin_ms": None,
        "isolation": None}
