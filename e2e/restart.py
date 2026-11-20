"""Pure checks for the graceful-restart scenario (no Docker, no I/O).

Scenario (e2e/smoke_restart.py): baseline -> graceful stop -> run sent while
Spark is down -> restart -> catch-up -> these checks. All inputs come from
one "evidence" dict, so a saved evidence file can be re-checked offline
(`python -m e2e.smoke_restart --check-evidence FILE`). That is also how the
failure paths are demonstrated: edit a copy, re-check.

Names:
    E0  committed end offsets per partition after the stop (exclusive)
    B0  last committed batch_id after the stop
    Q0  query_id after the stop (stored in the checkpoint)
    R0  run_id of the process that committed B0 (changes on every restart).
        Not the same as verify's rule R0 (unique Kafka offset, D2.2).
    V0  bronze Delta version after the stop; V1 after catch-up

Delta checks (evidence_version 2, Phase 2 slice 2, DECISIONS D2.2): the rows
added by commits (V0, V1], read from the Delta log JSON by common.delta_log,
must equal the run's message count; Spark's time-travel counts at V0 and V1
must give the same difference; and every one of those commits must carry one
txn whose appId is Q0 (the query `id`, stable across restarts, NOT the run_id)
and whose version is a post-restart batch id. The baseline step makes sure no
batch was planned before the stop, so every post-restart batch with rows
wrote exactly one commit.

Silver checks (evidence_version 3, slice 3, DECISIONS D2.3): S0 = silver's
last committed batch and Delta source end offset at the stop, SV0/SV1 the silver
Delta versions at the stop and after silver caught up. The first silver batch
after the restart (S0 + 1, same query id) must START exactly at S0's end offset
(no re-read, no skipped bronze version), and the rows added to silver in
(SV0, SV1] must equal the run's unique ids (fresh seed: every id is new).
Version 2 evidence (bronze only) is still accepted; it gets no silver checks.

Late table (evidence_version 4, slice 4, DECISIONS D2.4): LV0/LV1 are the late
table's Delta versions at the stop and after silver caught up. A row of the run
can be routed to late (cross-run overlap), so the rule becomes: rows added to
silver in (SV0, SV1] + rows added to late in (LV0, LV1] == the run's unique ids.
Version 3 evidence keeps the silver-only rule.

First post-restart batch: in the status history (same Q0), the batch with
batch_id B0 + 1. Batch ids increase by exactly 1 per executed batch, so
anything else means the history does not contain it and we cannot prove
anything. Its run_id must differ from R0 and match the new process's
"query started" log line. If Spark had planned but not committed a batch
before the stop, it re-runs it as B0 + 1, and the same rule applies: it must
start exactly at E0.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from common.manifest_reader import OffsetRange

EVIDENCE_VERSION = 4
ACCEPTED_VERSIONS = (2, 3, 4)
PHASE1_POINTER = ("evidence_version 1 is Phase 1 (raw Parquet) evidence; the Delta rules "
                  "cannot check it. Re-check it with the Phase 1 code at tag `phase-1`: "
                  "git switch --detach phase-1 && python -m e2e.smoke_restart "
                  "--check-evidence FILE")


def _int_keys(d: Mapping[Any, Any] | None) -> dict[int, int] | None:
    return None if d is None else {int(k): int(v) for k, v in d.items()}


# --- inputs ---------------------------------------------------------------------


def parse_kafka_end_offsets(text: str, topic: str) -> dict[int, int]:
    """Parse kafka-get-offsets.sh output lines "topic:partition:offset".

    Raises:
        ValueError: No line for `topic`, or a malformed line for it.
    """
    out: dict[int, int] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith(f"{topic}:"):
            continue
        name, partition, offset = line.rsplit(":", 2)
        if name != topic:
            continue
        out[int(partition)] = int(offset)
    if not out:
        raise ValueError(f"no offsets for topic {topic!r} in: {text.strip()[:200]!r}")
    return out


@dataclass(frozen=True)
class Batch:
    """One committed batch from the status history, for one topic."""

    batch_id: int
    run_id: str | None
    num_input_rows: int
    start: dict[int, int] | None    # None: Spark reported no start offset
    end: dict[int, int] | None


def history_batches(status: Mapping[str, Any], topic: str) -> list[Batch]:
    """Status history as Batch records (sorted by batch_id)."""
    batches = []
    for h in status.get("history") or []:
        start = (h.get("start_offset") or {}).get(topic) if h.get("start_offset") else None
        end = (h.get("end_offset") or {}).get(topic) if h.get("end_offset") else None
        batches.append(Batch(int(h["batch_id"]), h.get("run_id"), int(h["num_input_rows"]),
                             _int_keys(start), _int_keys(end)))
    return sorted(batches, key=lambda b: b.batch_id)


def baseline_pending(kafka_end: Mapping[int, int], committed: Mapping[int, int]) -> list[str]:
    """Partitions where Spark has not committed everything Kafka holds (empty = done)."""
    pending = []
    for p in sorted(kafka_end):
        c = committed.get(p)
        if c is None:
            if kafka_end[p] > 0:
                pending.append(f"p{p}: kafka end {kafka_end[p]}, nothing committed")
        elif c < kafka_end[p]:
            pending.append(f"p{p}: kafka end {kafka_end[p]}, committed {c}")
    return pending


def run_start_problems(
    run: Mapping[int, OffsetRange], e0: Mapping[int, int]
) -> tuple[list[str], list[str]]:
    """(other-writer problems, cannot-prove problems) for run.min == E0.

    A run sent while Spark is down must start exactly at E0 in every partition
    it wrote to. A higher min means someone else wrote in between.
    """
    writers, unprovable = [], []
    for p in sorted(run):
        if p not in e0:
            unprovable.append(f"p{p}: run wrote here but E0 has no offset for it (cannot prove)")
        elif run[p].min != e0[p]:
            writers.append(f"p{p}: run starts at {run[p].min} but E0 is {e0[p]}: "
                           f"{run[p].min - e0[p]} offsets in between came from another writer")
    return writers, unprovable


# --- checks ---------------------------------------------------------------------


@dataclass(frozen=True)
class Check:
    """One named assertion with its outcome."""

    name: str
    ok: bool
    detail: str


def find_post_restart(
    status: Mapping[str, Any] | None, *, q0: str, b0: int, r0: str | None,
    new_run_ids: Iterable[str], topic: str,
) -> tuple[list[Batch], list[str]]:
    """(batches after B0, problems). Problems mean the first batch is not proven."""
    if not status:
        return [], ["status file missing after catch-up"]
    if status.get("query_id") != q0:
        return [], [f"query_id changed {q0} -> {status.get('query_id')}: checkpoint was reset, "
                    "offsets cannot be compared with E0"]
    post = [b for b in history_batches(status, topic) if b.batch_id > b0]
    if not post:
        return [], [f"no batch after B0={b0} in the status history"]
    first = post[0]
    problems = []
    if first.batch_id != b0 + 1:
        problems.append(f"history has no batch {b0 + 1} (first after B0 is {first.batch_id}; "
                        "evicted from the 100-entry history?)")
    if r0 is not None and first.run_id == r0:
        problems.append(f"batch {first.batch_id} has the pre-stop run_id {r0}: "
                        "it was not run by the restarted process")
    new_ids = list(new_run_ids)
    if not new_ids:
        problems.append("no 'query started' log line after the restart")
    elif first.run_id not in new_ids:
        problems.append(f"batch {first.batch_id} run_id {first.run_id} does not match the "
                        f"restarted query's run_id {new_ids}")
    return post, problems


def check_no_rewind(post: list[Batch], e0: Mapping[int, int]) -> list[str]:
    """First post-restart batch starts exactly at E0; later batches chain end -> start."""
    if not post:
        return ["no post-restart batch to check"]
    first = post[0]
    if first.start is None:
        return [f"batch {first.batch_id} has no start_offset (cannot prove)"]
    problems = []
    for p in sorted(e0):
        s = first.start.get(p)
        if s is None:
            problems.append(f"p{p}: batch {first.batch_id} has no start offset (cannot prove)")
        elif s < e0[p]:
            problems.append(f"p{p}: REWIND, batch {first.batch_id} starts at {s}, E0 is {e0[p]} "
                            f"({e0[p] - s} offsets re-read)")
        elif s > e0[p]:
            problems.append(f"p{p}: GAP, batch {first.batch_id} starts at {s}, E0 is {e0[p]} "
                            f"({s - e0[p]} offsets never read)")
    for prev, cur in zip(post, post[1:], strict=False):
        if prev.end != cur.start:
            problems.append(f"batch {cur.batch_id} start {cur.start} != batch {prev.batch_id} "
                            f"end {prev.end}")
    return problems


def check_no_skip(
    post: list[Batch], e0: Mapping[int, int], final_end: Mapping[int, int] | None,
    run: Mapping[int, OffsetRange], run_count: int,
) -> list[str]:
    """Rows read after restart == run count, and offsets line up with E0."""
    problems = []
    rows = sum(b.num_input_rows for b in post)
    if rows != run_count:
        problems.append(f"rows read after restart {rows} != run messages {run_count} "
                        f"({rows - run_count:+d})")
    writers, unprovable = run_start_problems(run, e0)
    problems += writers + unprovable
    if final_end is None:
        return [*problems, "no committed end offset after catch-up"]
    for p in sorted(run):
        if p in e0 and final_end.get(p) != e0[p] + run[p].count:
            problems.append(f"p{p}: end {final_end.get(p)} != E0 {e0[p]} + run count "
                            f"{run[p].count}")
    for p in sorted(set(e0) - set(run)):
        if final_end.get(p, e0[p]) != e0[p]:
            problems.append(f"p{p}: run did not write here, but end moved {e0[p]} -> "
                            f"{final_end.get(p)} (another writer?)")
    return problems


def evidence_problem(ev: Mapping[str, Any]) -> str | None:
    """None if these rules can check the evidence, else why not (v1 = Phase 1)."""
    version = ev.get("evidence_version")
    if version in ACCEPTED_VERSIONS:
        return None
    if version == 1:
        return PHASE1_POINTER
    return f"unknown evidence_version {version!r} (expected one of {ACCEPTED_VERSIONS})"


# --- silver (evidence_version 3) ----------------------------------------------------

OFFSET_FIELDS = ("reservoir_id", "reservoir_version", "index")


def check_silver_resume(status: Mapping[str, Any] | None, s0: Mapping[str, Any]
                        ) -> tuple[int | None, list[str]]:
    """(first post-restart silver batch id, problems): it starts exactly at S0's end."""
    if not status:
        return None, ["silver status file missing after catch-up"]
    if status.get("query_id") != s0["query_id"]:
        return None, [f"silver query_id changed {s0['query_id']} -> {status.get('query_id')} "
                      "(silver checkpoint reset)"]
    want = int(s0["batch_id"]) + 1
    first = next((h for h in status.get("history") or [] if int(h["batch_id"]) == want), None)
    if first is None:
        return None, [f"no silver batch {want} in the status history"]
    start = first.get("start_offset") or {}
    end0 = s0["end_offset"] or {}
    diff = [f"{k} {end0.get(k)} -> {start.get(k)}" for k in OFFSET_FIELDS
            if start.get(k) != end0.get(k)]
    if diff:
        return want, [f"silver batch {want} does not start at S0's end offset: "
                      + ", ".join(diff)]
    return want, []


def check_silver_rows(log: Mapping[str, Any] | None, run_unique_ids: int,
                      late_log: Mapping[str, Any] | None = None, with_late: bool = False
                      ) -> list[str]:
    """Rows added to silver in (SV0, SV1] (+ late in (LV0, LV1] if with_late) == run ids."""
    if problems := _log_error(log):
        return [p.replace("Delta log", "silver Delta log") for p in problems]
    if with_late and (problems := _log_error(late_log)):
        return [p.replace("Delta log", "late Delta log") for p in problems]
    assert log is not None
    late_rows = int(late_log["rows"]) if with_late and late_log is not None else 0
    if int(log["rows"]) + late_rows != run_unique_ids:
        span = f"silver rows added in versions {log['v_before']}..{log['v_after']}: {log['rows']}"
        if with_late and late_log is not None:
            span += (f" + late rows added in versions {late_log['v_before']}.."
                     f"{late_log['v_after']}: {late_rows}")
        return [f"{span} != run unique ids {run_unique_ids}"]
    return []


def _log_error(log: Mapping[str, Any] | None) -> list[str]:
    if not log:
        return ["cannot prove: no Delta log summary in the evidence"]
    if log.get("error"):
        return [f"cannot prove: Delta log unreadable: {log['error']}"]
    return []


def check_log_delta(log: Mapping[str, Any] | None, run_count: int) -> list[str]:
    """Rows added in commits (V0, V1] == the run's message count."""
    if problems := _log_error(log):
        return problems
    assert log is not None
    delta = int(log["rows"])
    if delta == run_count:
        return []
    kind = "EXTRA rows (re-read or duplicate writes)" if delta > run_count else "MISSING rows"
    return [f"Delta log rows added in versions {log['v_before']}..{log['v_after']}: {delta} != "
            f"run messages {run_count} ({delta - run_count:+d}, {kind})"]


def check_log_vs_spark(log: Mapping[str, Any] | None,
                       version_counts: Mapping[str, int] | None) -> list[str]:
    """Our log count == Spark's count(V1) - count(V0) (time travel, inside verify)."""
    if problems := _log_error(log):
        return problems
    assert log is not None
    v0, v1 = str(log["v_before"]), str(log["v_after"])
    if not version_counts or v0 not in version_counts or v1 not in version_counts:
        return [f"cannot prove: verify reported no counts for versions {v0} and {v1}"]
    spark_delta = int(version_counts[v1]) - int(version_counts[v0])
    if spark_delta != int(log["rows"]):
        return [f"Delta log delta {log['rows']} != Spark time-travel delta {spark_delta} "
                f"({version_counts[v0]} at v{v0} -> {version_counts[v1]} at v{v1})"]
    return []


def check_txn_identity(log: Mapping[str, Any] | None, q0: str,
                       post: list[Batch]) -> list[str]:
    """Each commit in (V0, V1] has one txn, appId == Q0 (query id), version == a batch id."""
    if problems := _log_error(log):
        return problems
    assert log is not None
    txns = list(log["txns"])
    problems = []
    per_commit: dict[int, int] = {}
    for t in txns:
        per_commit[int(t["delta_version"])] = per_commit.get(int(t["delta_version"]), 0) + 1
    without = [v for v in range(int(log["v_before"]) + 1, int(log["v_after"]) + 1)
               if per_commit.get(v, 0) != 1]
    if without:
        problems.append(f"commits without exactly one txn action: versions {without}")
    wrong = sorted({str(t["appId"]) for t in txns} - {q0})
    if wrong:
        problems.append(f"txn appId {wrong} != query id Q0 {q0}")
    versions = [int(t["version"]) for t in txns]
    expected = [b.batch_id for b in post if b.num_input_rows > 0]
    if versions != expected:
        problems.append(f"txn versions {versions} != post-restart batch ids with rows "
                        f"{expected}")
    return problems


def evaluate(ev: Mapping[str, Any]) -> list[Check]:
    """Run every check on an evidence dict (see smoke_restart.scenario).

    Raises:
        ValueError: the evidence is not version 2 (see evidence_problem).
    """
    if problem := evidence_problem(ev):
        raise ValueError(problem)
    topic = ev["topic"]
    e0 = _int_keys(ev["e0"]) or {}
    run = {int(p): OffsetRange(*r) for p, r in ev["run_offsets"].items()}
    run_count = int(ev["run_count"])
    status = ev.get("status_after")
    final_end = None
    if status and (status.get("last_batch") or {}).get("end_offset"):
        final_end = _int_keys(status["last_batch"]["end_offset"].get(topic))

    def check(name: str, problems: list[str], ok_detail: str) -> Check:
        return Check(name, not problems, "; ".join(problems) if problems else ok_detail)

    checks = [check("graceful stop", [] if ev["stop_exit_code"] == 0 else
                    [f"exit code {ev['stop_exit_code']} (expected 0)"],
                    "exit code 0")]
    checks.append(check("baseline complete",
                        baseline_pending(_int_keys(ev["kafka_end_at_baseline"]) or {}, e0),
                        "E0 >= Kafka end offsets in every partition"))
    post, problems = find_post_restart(status, q0=ev["q0"], b0=int(ev["b0"]), r0=ev.get("r0"),
                                       new_run_ids=ev.get("new_run_ids") or [], topic=topic)
    checks.append(check("first batch after restart", problems,
                        f"batch {post[0].batch_id} run_id {post[0].run_id}" if post else ""))
    rewind = check_no_rewind(post, e0) if not problems else ["not evaluated: first batch unproven"]
    checks.append(check("no re-read (start == E0)", rewind,
                        f"batch {post[0].batch_id if post else '?'} starts at E0 "
                        "in all partitions"))
    checks.append(check("no skip (rows + offsets)",
                        check_no_skip(post, e0, final_end, run, run_count),
                        f"{run_count} rows read after restart, offsets contiguous with E0"))
    log = (ev.get("delta") or {}).get("log")
    rows = (log or {}).get("rows")
    span = f"v{(log or {}).get('v_before')}..v{(log or {}).get('v_after')}"
    checks.append(check("delta log rows == run", check_log_delta(log, run_count),
                        f"+{rows} rows in {span}"))
    checks.append(check("delta log == spark time travel",
                        check_log_vs_spark(log, ev.get("version_counts")),
                        f"both +{rows}"))
    checks.append(check("txn appId == query id", check_txn_identity(log, ev["q0"], post),
                        f"{len((log or {}).get('txns') or [])} commits, appId {ev['q0']}, "
                        "versions == batch ids"))
    if ev["evidence_version"] >= 3:
        sv = ev.get("silver") or {}
        s0 = sv.get("s0") or {}
        first, problems = check_silver_resume(ev.get("silver_status_after"), s0)
        at = (s0.get("end_offset") or {}).get("reservoir_version")
        checks.append(check("silver resumes at S0 end", problems,
                            f"silver batch {first} starts at bronze version {at}"))
        slog = sv.get("log")
        if ev["evidence_version"] >= 4:
            llog = (ev.get("late") or {}).get("log")
            checks.append(check(
                "silver + late rows == run ids",
                check_silver_rows(slog, int(ev.get("run_unique_ids") or -1), llog, True),
                f"silver +{(slog or {}).get('rows')}, late +{(llog or {}).get('rows')} rows"))
        else:
            checks.append(check("silver rows == run ids",
                                check_silver_rows(slog, int(ev.get("run_unique_ids") or -1)),
                                f"+{(slog or {}).get('rows')} rows in "
                                f"v{(slog or {}).get('v_before')}.."
                                f"v{(slog or {}).get('v_after')}"))
    v = ev.get("verify") or {}
    checks.append(check("verify --fail-on-duplicates",
                        [] if v.get("result") == "PASS" else [f"verify {v.get('result')}: "
                                                              f"{v.get('notes')}"],
                        f"{v.get('found_unique_ids')}/{v.get('expected_unique_ids')} ids, "
                        f"{v.get('duplicate_rows')} duplicates"))
    return checks


def exit_code_for(checks: list[Check]) -> int:
    """0 all pass, 5 a restart rule failed, 1 only verify failed."""
    if any(not c.ok for c in checks if c.name != "verify --fail-on-duplicates"):
        return 5
    return 0 if all(c.ok for c in checks) else 1


def format_checks(checks: list[Check]) -> str:
    """Aligned PASS/FAIL table."""
    width = max(len(c.name) for c in checks)
    return "\n".join(f"  {'PASS' if c.ok else 'FAIL'}  {c.name:<{width}}  {c.detail}"
                     for c in checks)
