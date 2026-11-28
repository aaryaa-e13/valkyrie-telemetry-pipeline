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


# --- slice 2: R10c, R10d, R11, R14 (pure verdicts; verify_manifest does the reads) ------
#
#   R10c  every rejection the manifest expects (v4 bad_data) is in the DLQ under the
#         expected dlq_key (id:<id> or off:<topic>/<p>/<offset>) with the expected
#         reason_code. A missing row, a wrong reason or a wrong key kind -> FAIL.
#   R10d  the flags the manifest expects == the quality_flags on the run's S ∪ L rows:
#         a flagged event carries exactly its flag, every other run row carries none.
#   R11   verify's own gates (streaming/oracle_gates.py) on the run's bronze rows give
#         exactly the run's DLQ rows (dlq_key, reason_code, reasons, rules_version) and
#         exactly the flags on the run's S ∪ L rows.
#   R14   stranded valid copy (addendum A): a keyed D_open id with a consumed bronze copy
#         that passes every hard rule (verify's gates, the DLQ row's rules version), or
#         sum(dup_of_dlq + late_dup_of_dlq) > 0 over Phase 3 batches -> FAIL.
# "Not checkable" is never a PASS.


def r10c_verdict(r: Mapping[str, Any]) -> tuple[str, list[str]]:
    """R10c from verify's counts: expected, missing, wrong_reason, wrong_key_kind."""
    if not r.get("checkable", True):
        return f"not checkable: {r.get('reason')}", [f"R10c not checkable: {r.get('reason')}"]
    bad = {k: int(r[k]) for k in ("missing", "wrong_reason", "wrong_key_kind")}
    if any(bad.values()):
        return "FAIL", [f"R10c: of {r['expected']} expected rejections, {bad['missing']} are "
                        f"not in the DLQ ({bad['wrong_key_kind']} of them under the other key "
                        f"kind) and {bad['wrong_reason']} have another reason_code: "
                        f"{r.get('examples')}"]
    return "PASS", []


def r10d_verdict(r: Mapping[str, Any]) -> tuple[str, list[str]]:
    """R10d from verify's counts: rows_checked, mismatched, null_flags."""
    if not r.get("checkable", True):
        return f"not checkable: {r.get('reason')}", [f"R10d not checkable: {r.get('reason')}"]
    if int(r.get("null_flags") or 0) > 0:
        reason = f"{r['null_flags']} run rows have quality_flags NULL (written before Phase 3)"
        return f"not checkable: {reason}", [f"R10d not checkable: {reason}"]
    if int(r["mismatched"]) > 0:
        return "FAIL", [f"R10d: {r['mismatched']} of the run's {r['rows_checked']} S ∪ L rows "
                        f"carry other flags than the manifest expects: {r.get('examples')}"]
    return "PASS", []


def r11_compare(recomputed_dlq: Mapping[str, tuple], actual_dlq: Mapping[str, tuple],
                recomputed_flags: Mapping[str, tuple[str, ...]],
                actual_flags: Mapping[str, tuple[str, ...] | None]) -> dict[str, Any]:
    """Compare verify's recompute with the job's tables (R11).

    recomputed_dlq / actual_dlq: dlq_key -> (reason_code, reasons tuple, rules_version).
    recomputed_flags: event_id -> flags, for the rows verify's gates accept.
    actual_flags: event_id -> flags (None = NULL) of the run's rows in S ∪ L.
    """
    missing = sorted(set(recomputed_dlq) - set(actual_dlq))
    extra = sorted(set(actual_dlq) - set(recomputed_dlq))
    differ = sorted(k for k in set(recomputed_dlq) & set(actual_dlq)
                    if tuple(recomputed_dlq[k]) != tuple(actual_dlq[k]))
    not_accepted = sorted(set(recomputed_flags) - set(actual_flags))
    not_expected = sorted(set(actual_flags) - set(recomputed_flags))
    flag_differ = sorted(e for e in set(recomputed_flags) & set(actual_flags)
                         if actual_flags[e] is None
                         or tuple(actual_flags[e] or ()) != tuple(recomputed_flags[e]))
    examples = [*(f"{k}: recompute {recomputed_dlq[k]}, not in DLQ" for k in missing),
                *(f"{k}: in DLQ {actual_dlq[k]}, recompute accepts it" for k in extra),
                *(f"{k}: recompute {recomputed_dlq[k]} != DLQ {actual_dlq[k]}" for k in differ),
                *(f"{e}: recompute accepts it, not in S ∪ L" for e in not_accepted),
                *(f"{e}: in S ∪ L, recompute rejects it" for e in not_expected),
                *(f"{e}: flags {actual_flags[e]} != recompute {recomputed_flags[e]}"
                  for e in flag_differ)][:MAX_EXAMPLES]
    return {"dlq_missing": len(missing), "dlq_extra": len(extra), "dlq_differ": len(differ),
            "accepted_missing": len(not_accepted), "accepted_extra": len(not_expected),
            "flags_differ": len(flag_differ), "examples": examples}


def r11_verdict(r: Mapping[str, Any]) -> tuple[str, list[str]]:
    if not r.get("checkable"):
        return f"not checkable: {r.get('reason')}", [f"R11 not checkable: {r.get('reason')}"]
    c = r["compare"]
    keys = ("dlq_missing", "dlq_extra", "dlq_differ", "accepted_missing", "accepted_extra",
            "flags_differ")
    if any(int(c[k]) for k in keys):
        return "FAIL", ["R11: verify's own gates disagree with the job on the run's rows ("
                        + ", ".join(f"{k} {c[k]}" for k in keys) + f"): {c['examples']}"]
    return "PASS", []


def r14_verdict(r: Mapping[str, Any]) -> tuple[str, list[str]]:
    """R14 from verify's counts: stranded ids/copies (bronze scan) and the job's counters."""
    if not r.get("checkable"):
        return f"not checkable: {r.get('reason')}", [f"R14 not checkable: {r.get('reason')}"]
    ids, counter = int(r["stranded_ids"]), int(r["counter_dup_of_dlq"])
    if ids == 0 and counter == 0:
        return "PASS", []
    agree = "the two signals agree" if (ids > 0) == (counter > 0) else \
        "the two signals DISAGREE (a bug in the job's counters or in verify)"
    return "FAIL", [f"R14: {ids} D_open ids have {r['stranded_copies']} consumed bronze "
                    f"copies that pass every hard rule, and the job counted "
                    f"dup_of_dlq + late_dup_of_dlq = {counter}; {agree}: valid events "
                    f"stranded in the DLQ (D3.1): {r.get('examples')}"]
