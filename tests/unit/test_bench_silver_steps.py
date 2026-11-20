"""The pure part of bench/silver_steps.py: which step an SQL execution belongs to."""

from __future__ import annotations

from typing import Any

from bench.silver_steps import (
    PATHS,
    Exec,
    batch_of,
    classify,
    nest,
    parse_time,
    steps_of,
    tables_read,
)


def scan(*names: str, log: bool = False) -> str:
    suffix = "/_delta_log/00000000000000000020.checkpoint.parquet" if log else ""
    return " ".join(f"Location: X[file:{PATHS[n]}{suffix}]" for n in names)


def ex(plan: str, merge: bool = False) -> dict[str, Any]:
    nodes = [{"nodeName": "Execute MergeIntoCommand"}] if merge else []
    return {"planDescription": plan, "description": "silver\nbatch = 3", "nodes": nodes}


def test_parse_time_and_batch_of() -> None:
    assert parse_time("1970-01-01T00:00:01.500GMT") == 1.5
    assert batch_of("silver\nid = a\nrunId = b\nbatch = 12", "silver") == 12
    assert batch_of("bronze\nid = a\nrunId = b\nbatch = 12", "silver") is None
    # Delta's nested executions carry the batch description with a suffix.
    assert batch_of("Delta: silver\nid = a\nrunId = b\nbatch = 12: Filtering", "silver") == 12
    assert batch_of("silver\nid = a\nrunId = b\nbatch = 12 - MERGE operation", "silver") == 12


def test_tables_read_separates_delta_log_only() -> None:
    assert tables_read(scan("late") + scan("silver", log=True)) == ({"late"}, {"silver"})
    assert tables_read("path -> /data/lake/_control/silver_batches,") == ({"control"}, set())


def test_classify_in_code_order() -> None:
    assert classify(ex(scan("control")), None) == "wm read"
    assert classify(ex(scan("bronze")), "wm read") == "plan counts"
    assert classify(ex(scan("bronze", "silver")), "plan counts") == "late write"
    # A MERGE reads both silver and late; its step comes from the count before it.
    assert classify(ex(scan("late", "silver"), merge=True), "late write") == "late write"
    assert classify(ex(scan("bronze", "late")), "late write") == "late anti-join + count"
    both = ex(scan("late", "silver"), merge=True)
    assert classify(both, "late anti-join + count") == "silver MERGE"
    assert classify(ex(scan("control")), "silver MERGE") == "control write"
    assert classify(ex(scan("silver", log=True)), "late anti-join + count") is None
    # The persisted micro-batch shows no table path: an action on it is a plan count.
    assert classify(ex("InMemoryTableScan HashAggregate"), "wm read") == "plan counts"
    assert classify(ex("LocalTableScan [path, partitionValues, deletionVector]"), None) is None
    assert classify(ex("Execute CreateViewCommand"), "late anti-join + count") is None


def test_steps_nest_children_and_give_snapshot_loads_the_next_step() -> None:
    def e(i: int, start: float, end: float, plan: str, merge: bool = False) -> Exec:
        return Exec(i, start, end, [i], ex(plan, merge))

    outer = e(0, 0.0, 10.0, "")
    execs = [outer, e(1, 0.1, 1.0, scan("control")), e(2, 1.5, 3.0, scan("bronze")),
             e(3, 3.2, 4.0, scan("bronze", "late")),
             e(4, 4.1, 4.5, "Execute CreateViewCommand"),          # source view -> MERGE
             e(5, 4.6, 8.0, scan("late", "silver"), merge=True),
             e(6, 5.0, 6.0, scan("silver", log=True)),             # inside the MERGE
             e(7, 8.5, 9.5, scan("control"))]
    out, top = nest(execs)
    assert out.id == 0 and [x.id for x in top] == [1, 2, 3, 4, 5, 7]
    steps = steps_of(out, top, {i: {"numTasks": 2} for i in range(8)})
    assert steps["silver MERGE"]["executions"] == [4, 5]
    assert steps["silver MERGE"]["jobs"] == 3                     # 4, 5 and child 6
    assert steps["silver MERGE"]["wall_ms"] == 4000               # 4.0 -> 8.0
    assert steps["silver MERGE"]["driver_ms"] == 4000 - 400 - 3400
    assert steps["silver MERGE"]["after_view_ms"] == 3500           # 4.5 -> 8.0
    assert steps["control write"]["wall_ms"] == 1500
    assert steps["other"]["wall_ms"] == 500
