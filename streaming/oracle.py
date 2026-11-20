"""Pure parts of the oracle rules R4-R6 and their two gates (DECISIONS D2.5).

Standard library only: verify_manifest.py does the Spark reads and passes plain
values in here, so the rules are unit-tested without Spark. Nothing here imports
the silver job (streaming.silver / streaming.watermark): the watermark formula
and the clamp are written again on purpose, so a bug in the job's code cannot
hide in its own check (plan section 3, decision 4).

Notation (C = the manifest's delivered ids; S, L = silver and late ids from C):

    W check     manifest v3 with late_events, and ONE W over the control rows of
                every batch that inserted a run row, equal to the manifest's
                assumed_watermark_s. Else "not checkable: <reason>".
    isolation   foreign_max < min_t + W, where min_t = min event_time of the run's
                keyed bronze rows and foreign_max = max clamped time of every
                other keyed bronze row (the job's clamp: rows without a Kafka
                time never count). Else "not checkable: not isolated".
    R4          L ⊆ {late events with expected == "may_drop"}       (both gates)
    R5          every keep id (on time or slight) is in S            (both gates)
    R6a         late row: event_time <= watermark_used, and watermark_used ==
                its batch's control watermark_used                   (always)
    R6b         silver row of a slice-4 batch: event_time > wm of that batch
    R6c         control wm(N) == max(m_k, k < N) - W (NULL in the snapshot or
                when no m_k), and the non-NULL wm sequence never goes down
    R6d         control m_k >= max clamped time of the rows batch k inserted

"Not checkable" is never a PASS. A batch written before slice 4
(watermark_delay_s NULL) is counted and skipped by R6, never judged.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from common.manifest_reader import Manifest

US = 1_000_000
MAX_EXAMPLES = 20
R6_RULES = ("R6a", "R6b", "R6c", "R6d")
RULES = ("R4", "R5", *R6_RULES)


@dataclass(frozen=True)
class ControlRow:
    """The control-table columns the oracle needs (times in epoch microseconds)."""

    query_id: str
    batch_id: int
    in_snapshot: bool | None
    delay_s: int | None          # watermark_delay_s; None = written before slice 4
    wm_us: int | None            # watermark_used; None = -infinity
    max_us: int | None           # batch_max_clamped_event_time (m_k)

    @property
    def slice4(self) -> bool:
        return self.delay_s is not None


def not_checkable(reason: str) -> dict[str, Any]:
    return {"checkable": False, "reason": reason}


def w_gate(manifest: Manifest, run_batches: Iterable[tuple[str, int]],
           ctl: Mapping[tuple[str, int], ControlRow]) -> dict[str, Any]:
    """The W oracle check: manifest assumed_watermark_s == the job's recorded W."""
    batches = sorted(set(run_batches))
    out: dict[str, Any] = {"manifest_w_s": None, "control_w_s": [], "run_batches": len(batches)}
    if manifest.version != 3 or manifest.pacing is None:
        return {**out, **not_checkable(f"manifest v{manifest.version} has no lateness data")}
    out["manifest_w_s"] = manifest.pacing.assumed_watermark_s
    if manifest.late_events is None:
        return {**out, **not_checkable(
            f"late_events omitted: {manifest.late_events_omitted_reason}")}
    if not batches:
        return {**out, **not_checkable("no row of the run is in silver or late")}
    missing = [b for b in batches if b not in ctl]
    if missing:
        return {**out, **not_checkable(f"{len(missing)} batches without a control row, "
                                       f"e.g. {list(missing[0])}")}
    ws = sorted({ctl[b].delay_s for b in batches}, key=lambda w: (w is not None, w or 0))
    out["control_w_s"] = ws
    if None in ws:
        return {**out, **not_checkable("a batch of the run has W = NULL (pre-slice-4)")}
    if len(ws) > 1:
        return {**out, **not_checkable(f"W differs across the run's batches: {ws}")}
    w = ws[0]
    assert w is not None
    if float(w) != float(manifest.pacing.assumed_watermark_s):
        return {**out, **not_checkable(
            f"W differs: manifest {manifest.pacing.assumed_watermark_s:g} s, control {w} s")}
    return {**out, "checkable": True, "reason": None, "w_s": w}


def isolation_gate(w_s: int | None, min_t_us: int | None,
                   foreign_max_us: int | None) -> dict[str, Any]:
    """Isolated iff foreign_max < min_t + W (exact; see the module docstring)."""
    out: dict[str, Any] = {"w_s": w_s, "min_t_us": min_t_us, "foreign_max_us": foreign_max_us,
                           "margin_ms": None}
    if w_s is None:
        return {**out, **not_checkable("no W to check against (W check not checkable)")}
    if min_t_us is None:
        return {**out, **not_checkable("no keyed bronze row of the run")}
    if foreign_max_us is None:
        return {**out, "checkable": True, "reason": None,
                "note": "no foreign keyed bronze row with a Kafka time"}
    margin_us = min_t_us + w_s * US - foreign_max_us
    out["margin_ms"] = margin_us // 1000
    if margin_us <= 0:
        return {**out, **not_checkable(
            f"not isolated: a foreign bronze row has clamped time >= min_t + W "
            f"(margin {margin_us / 1000:.0f} ms)")}
    return {**out, "checkable": True, "reason": None}


def band_of(manifest: Manifest) -> dict[str, str]:
    """event_id -> 'ontime' | 'slight' | 'very' (v3 with late_events only)."""
    assert manifest.late_events is not None
    out = {e: "ontime" for e in manifest.event_ids}
    out.update({e: x.band for e, x in manifest.late_events.items()})
    return out


def band_counts(manifest: Manifest, s_ids: set[str], l_ids: set[str]) -> dict[str, Any] | None:
    """Per band: ids expected (from the manifest), found in silver, found in late."""
    if manifest.late_events is None:
        return None
    bands = band_of(manifest)
    out: dict[str, Any] = {}
    for name in ("ontime", "slight", "very"):
        ids = {e for e, b in bands.items() if b == name}
        out[name] = {"expected": len(ids), "silver": len(ids & s_ids), "late": len(ids & l_ids)}
    return out


def r4_r5(manifest: Manifest, s_ids: set[str], l_ids: set[str]) -> dict[str, Any]:
    """R4 (L ⊆ may_drop) and R5 (keep ⊆ S). Call only when both gates are checkable."""
    assert manifest.late_events is not None
    late = manifest.late_events
    may_drop = {e for e, x in late.items() if x.expected == "may_drop"}
    keep = set(manifest.event_ids) - may_drop
    r4 = sorted(l_ids - may_drop)
    r5 = sorted(keep - s_ids)

    def info(e: str) -> dict[str, Any]:
        x = late.get(e)
        return {"event_id": e, "band": x.band if x else "ontime",
                "delay_ms": x.delay_ms if x else 0}

    return {"may_drop_ids": len(may_drop), "keep_ids": len(keep),
            "r4_violations": len(r4), "r4_examples": [info(e) for e in r4[:MAX_EXAMPLES]],
            "r5_violations": len(r5), "r5_examples": [info(e) for e in r5[:MAX_EXAMPLES]]}


def r6c(rows: Iterable[ControlRow]) -> dict[str, Any]:
    """Recompute wm(N) for every slice-4 control row; check it never goes down."""
    by_query: dict[str, list[ControlRow]] = {}
    for r in rows:
        by_query.setdefault(r.query_id, []).append(r)
    mismatches: list[dict[str, Any]] = []
    goes_down: list[dict[str, Any]] = []
    checked = skipped = 0
    for q, qrows in sorted(by_query.items()):
        qrows.sort(key=lambda r: r.batch_id)
        prior: int | None = None          # max m_k over k < N, NULLs ignored
        last: ControlRow | None = None    # previous slice-4 row with a non-NULL wm
        for r in qrows:
            if r.slice4:
                checked += 1
                assert r.delay_s is not None
                if r.in_snapshot is None:
                    expected: int | None = None
                    why = "in_initial_snapshot is NULL"
                else:
                    expected = None if r.in_snapshot or prior is None \
                        else prior - r.delay_s * US
                    why = "formula"
                if r.in_snapshot is None or expected != r.wm_us:
                    mismatches.append({"query_id": q, "batch_id": r.batch_id, "why": why,
                                       "recorded_us": r.wm_us, "expected_us": expected})
                if r.wm_us is not None:
                    if last is not None and last.wm_us is not None and r.wm_us < last.wm_us:
                        goes_down.append({"query_id": q, "batch_id": r.batch_id,
                                          "wm_us": r.wm_us, "previous_batch": last.batch_id,
                                          "previous_wm_us": last.wm_us})
                    last = r
            else:
                skipped += 1
            if r.max_us is not None:
                prior = r.max_us if prior is None else max(prior, r.max_us)
    return {"batches_checked": checked, "pre_slice4_batches": skipped,
            "formula_mismatches": len(mismatches), "goes_down": len(goes_down),
            "examples": (mismatches + goes_down)[:MAX_EXAMPLES]}


def verdicts(oracle: Mapping[str, Any]) -> dict[str, str]:
    """Rule -> 'PASS' | 'FAIL' | 'not checkable: <reason>' for R4, R5, R6a-d."""
    out: dict[str, str] = {}
    w, iso = oracle["w_check"], oracle["isolation"]
    if not w["checkable"]:
        gate = f"not checkable: W check: {w['reason']}"
    elif not iso["checkable"]:
        gate = f"not checkable: isolation: {iso['reason']}"
    else:
        gate = ""
    r45 = oracle.get("r4_r5")
    for rule, key in (("R4", "r4_violations"), ("R5", "r5_violations")):
        out[rule] = gate or ("FAIL" if r45 is None or r45[key] > 0 else "PASS")
    r6 = oracle["r6"]
    out["R6a"] = "FAIL" if r6["r6a"]["violations"] > 0 else "PASS"
    out["R6b"] = "FAIL" if r6["r6b"]["violations"] > 0 else "PASS"
    out["R6c"] = ("FAIL" if r6["r6c"]["formula_mismatches"] + r6["r6c"]["goes_down"] > 0
                  else "PASS")
    out["R6d"] = "FAIL" if r6["r6d"]["violations"] > 0 else "PASS"
    for rule in R6_RULES:
        if out[rule] == "PASS" and r6["r6c"]["batches_checked"] == 0:
            out[rule] = "not checkable: no slice-4 control row"
    return out


def failure_notes(oracle: Mapping[str, Any], v: Mapping[str, str]) -> list[str]:
    """One human-readable line per FAIL (prefixed with the rule) or not-checkable rule."""
    notes: list[str] = []
    r45, r6 = oracle.get("r4_r5") or {}, oracle["r6"]
    text = {
        "R4": lambda: (f"{r45.get('r4_violations')} late ids of the run are not may_drop: "
                       f"{r45.get('r4_examples')}"),
        "R5": lambda: (f"{r45.get('r5_violations')} keep ids of the run are not in silver: "
                       f"{r45.get('r5_examples')}"),
        "R6a": lambda: (f"{r6['r6a']['violations']} late rows break event_time <= "
                        f"watermark_used == control wm: {r6['r6a']['detail']}"),
        "R6b": lambda: (f"{r6['r6b']['violations']} silver rows of slice-4 batches have "
                        f"event_time <= wm of their batch: {r6['r6b']['examples']}"),
        "R6c": lambda: (f"{r6['r6c']['formula_mismatches']} control rows differ from "
                        f"max(m_k, k < N) - W, {r6['r6c']['goes_down']} wm go down: "
                        f"{r6['r6c']['examples']}"),
        "R6d": lambda: (f"{r6['r6d']['violations']} control rows record m_k below the "
                        f"clamped time of a row the batch inserted: {r6['r6d']['examples']}"),
    }
    for rule in RULES:
        if v[rule] == "FAIL":
            notes.append(f"{rule}: {text[rule]()}")
    gates = sorted({x for x in (v["R4"], v["R5"]) if x.startswith("not checkable")})
    notes += [f"R4/R5 {g}" for g in gates]
    notes += [f"{rule} {v[rule]}" for rule in R6_RULES if v[rule].startswith("not checkable")]
    return notes
