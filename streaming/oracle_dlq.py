"""Pure parts of the Phase 3 oracle rules R8, R9 (plan section 2.8) and R10, stdlib only.

verify_manifest.py does the Spark reads and passes plain values in here, so the rules
are unit-tested without Spark. Nothing here imports the silver job.

    R8   per Phase 3 control row, the equations of plan 2.5 add up:
             rows_in       = keyless_rows + keyed_rows
             keyed_rows    = dup_in_batch + rejected_rows + late_rows + ontime_rows
             keyless_rows  = dlq_keyless_inserted + dlq_keyless_already_present
             rejected_rows = dlq_keyed_inserted + dlq_keyed_already_present
                             + rejected_dup_of_accepted
             late_rows     = late_inserted + late_already_present + late_dup_of_silver
                             + late_dup_of_dlq
             ontime_rows   = inserted + ontime_already_present + dup_of_late + dup_of_dlq
             sum(reject_counts) = keyless_rows + rejected_rows
         where keyed_rows = keyed_unique + dup_in_batch (BatchCounts).
    R9   whole tables at batch C's recorded cut (addendum B): S, L and D_open pairwise
         disjoint by event_id; no dlq_key twice; no keyed event_id twice in D;
         resolved ids in S or L. (The balances of plan 2.8 are in verify's
         consistency section, which already owned the D2.3 balance.)
    R10  good data is never dead-lettered: no DLQ row carries a run's event id (R10a)
         or sits at offsets only the run wrote (R10b). See r10_verdicts.

A control row written before Phase 3 (rules_version NULL) is counted and skipped by
R8, never judged. "Not checkable" is never a PASS.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

MAX_EXAMPLES = 20

# (left side, [terms of the right side]) of each R8 equation; "keyed_rows" is derived.
R8_EQUATIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("rows_in", ("keyless_rows", "keyed_rows")),
    ("keyed_rows", ("dup_in_batch", "rejected_rows", "late_rows", "ontime_rows")),
    ("keyless_rows", ("dlq_keyless_inserted_this_attempt", "dlq_keyless_already_present")),
    ("rejected_rows", ("dlq_keyed_inserted_this_attempt", "dlq_keyed_already_present",
                       "rejected_dup_of_accepted")),
    ("late_rows", ("late_inserted_this_attempt", "late_already_present",
                   "late_dup_of_silver", "late_dup_of_dlq")),
    ("ontime_rows", ("inserted_this_attempt", "ontime_already_present", "dup_of_late",
                     "dup_of_dlq")),
)


def r8_row_problems(row: Mapping[str, Any]) -> list[str]:
    """The equations of one Phase 3 control row that do not add up ([] = all hold)."""
    v: dict[str, int] = {}
    missing = []
    for _, terms in R8_EQUATIONS:
        for t in terms:
            if t == "keyed_rows":
                continue
            if row.get(t) is None:
                missing.append(t)
            else:
                v[t] = int(row[t])
    for t in ("rows_in", "keyed_unique"):
        if row.get(t) is None:
            missing.append(t)
        else:
            v[t] = int(row[t])
    if missing:
        return [f"NULL field(s) {sorted(set(missing))}"]
    v["keyed_rows"] = v["keyed_unique"] + v["dup_in_batch"]
    out = []
    for left, terms in R8_EQUATIONS:
        right = sum(v[t] for t in terms)
        if v[left] != right:
            out.append(f"{left} {v[left]} != {' + '.join(terms)} = {right}")
    rc = row.get("reject_counts") or {}
    total = sum(int(x) for x in rc.values())
    if total != v["keyless_rows"] + v["rejected_rows"]:
        out.append(f"sum(reject_counts) {total} != keyless_rows + rejected_rows = "
                   f"{v['keyless_rows'] + v['rejected_rows']}")
    if any(int(x) < 0 for x in v.values()):
        out.append("negative count")
    return out


def r8(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """R8 over control rows: counts, violations and up to MAX_EXAMPLES examples."""
    checked = legacy = violations = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        if row.get("rules_version") is None:
            legacy += 1
            continue
        checked += 1
        problems = r8_row_problems(row)
        if problems:
            violations += 1
            if len(examples) < MAX_EXAMPLES:
                examples.append({"query_id": row.get("silver_query_id"),
                                 "batch_id": row.get("batch_id"), "problems": problems})
    return {"batches_checked": checked, "legacy_batches_skipped": legacy,
            "violations": violations, "examples": examples}


R9_COUNTS: tuple[tuple[str, str], ...] = (
    ("s_and_d_open", "ids in both silver and D_open"),
    ("l_and_d_open", "ids in both late and D_open"),
    ("s_and_l", "ids in both silver and late"),
    ("duplicate_dlq_keys", "dlq_key values that occur more than once"),
    ("duplicate_keyed_dlq_ids", "keyed event ids that occur more than once in the DLQ"),
    ("resolved_not_accepted", "resolved DLQ ids in neither silver nor late"),
)


def r9_verdict(r9: Mapping[str, Any]) -> tuple[str, list[str]]:
    """'PASS' | 'FAIL' | 'not checkable: <reason>', with one note per failing check.

    The balances ("distinct keyed bronze ids == |S| + |L| + |D_open keyed|", "|D keyless|
    == sum keyless_rows") are judged in verify's consistency section, which already owned
    the D2.3 balance; they are not repeated here, so one fault gives one failing rule."""
    if not r9.get("checkable"):
        return f"not checkable: {r9.get('reason')}", [f"R9 not checkable: {r9.get('reason')}"]
    notes = [f"R9: {r9[k]} {text}" for k, text in R9_COUNTS if int(r9[k]) > 0]
    return ("FAIL" if notes else "PASS"), notes


# --- R10: good data is never dead-lettered (slice 1 fix) --------------------------------
#
# Manifests v1-v3 describe good data only, so no DLQ row may belong to the run.
#   R10a  DLQ rows (any key kind, resolved or not) whose event_id is one of the
#         manifest's ids. Not checkable when the manifest lists no ids.
#   R10b  DLQ rows not caught by R10a at the run's own Kafka offsets. A partition's range
#         [min, max] is the run's OWN range when count == max - min + 1: every offset in
#         it is one of the run's acknowledged messages, so a DLQ row there (keyless or
#         keyed) is the run's message -> FAIL. When count < max - min + 1 other producers
#         wrote inside the range: such rows cannot be attributed, so they are counted and
#         reported as "not checkable" (never a PASS). Not checkable without kafka_offsets
#         (manifest v1).


def own_range(lo: int, hi: int, count: int) -> bool:
    """True when every offset in [lo, hi] is one of the run's `count` messages."""
    return count == hi - lo + 1


def r10_verdicts(r10: Mapping[str, Any]) -> tuple[dict[str, str], list[str]]:
    """{'R10a': v, 'R10b': v} with v 'PASS' | 'FAIL' | 'not checkable: <reason>', + notes.

    r10 keys (verify_manifest.dlq_section):
        ids_listed            int, ids in the manifest
        by_id_rows            int, DLQ rows carrying one of those ids
        by_id_examples        list of dlq_key
        offsets_checkable     bool; offsets_reason str when False
        own_rows / own_keyless / own_examples      rows in own ranges (not by id)
        shared_rows / shared_keyless               rows in ranges shared with others
    """
    verdicts: dict[str, str] = {}
    notes: list[str] = []
    if int(r10.get("ids_listed") or 0) == 0:
        verdicts["R10a"] = "not checkable: the manifest lists no event ids"
        notes.append("R10a not checkable: the manifest lists no event ids")
    elif int(r10["by_id_rows"]) > 0:
        verdicts["R10a"] = "FAIL"
        notes.append(f"R10a: {r10['by_id_rows']} DLQ rows carry this run's event ids: good "
                     f"data rejected (manifest v1-v3): {r10.get('by_id_examples')}")
    else:
        verdicts["R10a"] = "PASS"
    if not r10.get("offsets_checkable"):
        reason = r10.get("offsets_reason")
        verdicts["R10b"] = f"not checkable: {reason}"
        notes.append(f"R10b not checkable: {reason}")
    elif int(r10["own_rows"]) > 0:
        verdicts["R10b"] = "FAIL"
        notes.append(f"R10b: {r10['own_rows']} DLQ rows ({r10['own_keyless']} keyless) at "
                     f"offsets only this run wrote: good data rejected: "
                     f"{r10.get('own_examples')}")
    elif int(r10["shared_rows"]) > 0:
        reason = (f"{r10['shared_rows']} DLQ rows ({r10['shared_keyless']} keyless) in "
                  "offset ranges other producers also wrote; they cannot be attributed")
        verdicts["R10b"] = f"not checkable: {reason}"
        notes.append(f"R10b not checkable: {reason}")
    else:
        verdicts["R10b"] = "PASS"
    return verdicts, notes
