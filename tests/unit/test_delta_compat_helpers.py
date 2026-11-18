"""Unit tests for the pure helpers of streaming.delta_compat_check (no Spark needed)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from streaming.delta_compat_check import (
    CLASS_LIST_MARK,
    STOCK_DUP_BASELINE,
    artifact_name,
    audit_decisions,
    classify_duplicates,
    jackson_problems,
    jar_version,
    overall,
    own_coordinate,
    parse_entries,
    read_commits,
    reconcile,
    same_artifact_twice,
    scan_log,
    stock_drift,
    txn_actions,
)

# --- file-name rule (shared with fetch_jars.sh) --------------------------------------


@pytest.mark.parametrize(("jar", "artifact", "version"), [
    ("jackson-core-2.21.2.jar", "jackson-core", "2.21.2"),
    ("jackson-annotations-2.21.jar", "jackson-annotations", "2.21"),
    ("delta-spark_4.1_2.13-4.4.1.jar", "delta-spark_4.1_2.13", "4.4.1"),
    ("zstd-jni-1.5.7-6.jar", "zstd-jni", "1.5.7-6"),
    ("log4j-1.2-api-2.24.3.jar", "log4j-1.2-api", "2.24.3"),
])
def test_artifact_name(jar: str, artifact: str, version: str) -> None:
    assert artifact_name(jar) == artifact
    assert jar_version(jar) == version


@pytest.mark.parametrize(("jar", "misparsed", "real"), [
    ("spark-sql-kafka-0-10_2.13-4.1.4.jar", "spark-sql-kafka-0", "spark-sql-kafka-0-10_2.13"),
    ("hive-shims-0.23-2.3.10.jar", "hive-shims", "hive-shims-0.23"),
])
def test_known_parser_limitation(jar: str, misparsed: str, real: str) -> None:
    """Documented limit (D2.1): a '-<digit>' inside an artifactId is read as a version."""
    assert artifact_name(jar) == misparsed != real


def test_same_artifact_twice() -> None:
    assert same_artifact_twice(["a-1.0.jar", "a-2.0.jar", "b-1.0.jar"]) == {
        "a": ["a-1.0.jar", "a-2.0.jar"]}


# --- add/skip audit -----------------------------------------------------------------

LIST = """# coordinate: x
  add   spark-sql-kafka-0-10_2.13-4.1.4.jar org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4
  skip  jsr305-3.0.2.jar com.google.code.findbugs:jsr305:3.0.2 \
(Spark already ships jsr305-3.0.0.jar)
  skip  foo-1.0.jar org.x:foo:1.0 (Spark already ships foo-2.0.jar)
"""


def test_parse_entries() -> None:
    entries = parse_entries([LIST])
    assert [e["action"] for e in entries] == ["add", "skip", "skip"]
    assert entries[1]["shipped"] == "jsr305-3.0.0.jar"
    assert entries[0]["shipped"] is None


def test_misparse_without_effect_is_reported_but_not_affected() -> None:
    jars = ["spark-sql-kafka-0-10_2.13-4.1.4.jar", "jsr305-3.0.0.jar", "foo-2.0.jar"]
    misparsed, affected, name_only = audit_decisions(
        parse_entries([LIST]), {"foo-2.0.jar": ("org.x", "foo")}, jars)
    assert len(misparsed) == 1 and "would be add" in misparsed[0]
    assert affected == []
    assert name_only == ["jsr305-3.0.2.jar (com.google.code.findbugs:jsr305:3.0.2) vs "
                         "jsr305-3.0.0.jar"]


def test_misparse_that_changes_an_add_is_affected() -> None:
    # With the real name, another jar matches: the jar should have been skipped.
    jars = ["spark-sql-kafka-0-10_2.13-4.1.4.jar", "spark-sql-kafka-0-10_2.13-4.1.3.jar"]
    _, affected, _ = audit_decisions(parse_entries([LIST])[:1], {}, jars)
    assert len(affected) == 1 and "would be skip" in affected[0]


def test_skip_caused_by_a_different_artifact_is_affected() -> None:
    _, affected, _ = audit_decisions(parse_entries([LIST])[2:], {"foo-2.0.jar": ("org.y", "bar")},
                                     ["foo-2.0.jar"])
    assert affected == ["skip foo-1.0.jar (org.x:foo:1.0): Spark's foo-2.0.jar is org.y:bar, "
                        "a different artifact"]


def test_own_coordinate_ignores_embedded_shaded_poms() -> None:
    props = [{"groupId": "com.fasterxml.jackson.core", "artifactId": "jackson-core"},
             {"groupId": "org.apache.parquet", "artifactId": "parquet-jackson"}]
    assert own_coordinate("parquet-jackson-1.16.0.jar", props) == (
        "org.apache.parquet", "parquet-jackson")
    assert own_coordinate("other-1.0.jar", props) is None


# --- duplicate classes and Jackson ----------------------------------------------------

def test_duplicates_split_into_added_and_stock() -> None:
    owners = {
        "org/apache/spark/unused/UnusedStubClass.class": ["new-1.jar", "spark-core.jar"],
        "org/x/A.class": ["new-1.jar", "old-1.jar"],
        "org/y/B.class": ["s1.jar", "s2.jar"],
        "org/z/C.class": ["s1.jar"],
    }
    added, stock = classify_duplicates(owners, {"new-1.jar"})
    assert added == {"org/x/A.class": ["new-1.jar", "old-1.jar"]}  # stub class ignored
    assert stock == {("s1.jar", "s2.jar"): ["org/y/B.class"]}


def test_stock_baseline_total_and_drift() -> None:
    assert sum(STOCK_DUP_BASELINE.values()) == 1364
    same = {k: ["c"] * v for k, v in STOCK_DUP_BASELINE.items()}
    assert stock_drift(same, STOCK_DUP_BASELINE) == []
    changed = dict(same)
    changed[("new-a.jar", "new-b.jar")] = ["x"]
    drift = stock_drift(changed, STOCK_DUP_BASELINE)
    assert len(drift) == 2 and drift[-1] == "total: baseline 1364, now 1365"


def test_jackson_rule_annotations_minor_only() -> None:
    good = {"jackson-databind-2.21.2.jar": "2.21.2", "jackson-core-2.21.2.jar": "2.21.2",
            "jackson-annotations-2.21.jar": "2.21",
            "jackson-datatype-jdk8-2.21.2.jar": "2.21.2"}
    assert jackson_problems(good) == []
    bad = {**good, "jackson-datatype-jdk8-2.21.2.jar": "2.13.5",
           "jackson-annotations-2.21.jar": "2.21.2"}
    assert jackson_problems(bad) == [
        "jackson-annotations-2.21.jar: version 2.21.2, expected 2.21",
        "jackson-datatype-jdk8-2.21.2.jar: version 2.13.5, expected 2.21.2"]
    assert jackson_problems({"jackson-core-2.21.2.jar": "2.21.2"}) == [
        "expected exactly one jackson-databind jar, found 0"]


# --- Delta log replay ---------------------------------------------------------------

def _commit(table: Path, version: int, actions: list[dict]) -> None:
    log = table / "_delta_log"
    log.mkdir(parents=True, exist_ok=True)
    (log / f"{version:020d}.json").write_text("\n".join(json.dumps(a) for a in actions))


def _add(path: str, n: int, dv: dict | None = None) -> dict:
    add = {"path": path, "stats": json.dumps({"numRecords": n})}
    if dv:
        add["deletionVector"] = dv
    return {"add": add}


def test_reconcile_with_removes_and_deletion_vectors(tmp_path: Path) -> None:
    dv = {"storageType": "u", "pathOrInlineDv": "abc", "offset": 1, "cardinality": 2}
    _commit(tmp_path, 0, [{"commitInfo": {}}, _add("f1", 5), _add("f2", 5)])
    _commit(tmp_path, 1, [{"txn": {"appId": "q", "version": 0}}, _add("f3", 4)])
    _commit(tmp_path, 2, [{"remove": {"path": "f1"}}, _add("f4", 3)])  # rewrite
    _commit(tmp_path, 3, [{"remove": {"path": "f2"}}, _add("f2", 5, dv)])  # DV on f2
    (tmp_path / "_delta_log" / "00000000000000000002.checkpoint.parquet").write_text("x")
    commits = read_commits(tmp_path)
    assert [v for v, _ in commits] == [0, 1, 2, 3]
    assert reconcile(commits) == {"live_files": 3, "removes": 2, "records": 4 + 3 + 3,
                                  "files_without_numRecords": 0,
                                  "files_with_deletion_vector": 1}
    assert txn_actions(commits) == [{"delta_version": 1, "appId": "q", "version": 0}]


def test_reconcile_counts_missing_stats(tmp_path: Path) -> None:
    _commit(tmp_path, 0, [{"add": {"path": "f1"}}])
    assert reconcile(read_commits(tmp_path))["files_without_numRecords"] == 1


# --- C13 log scan and the overall verdict -----------------------------------------------

def test_scan_log_finds_linkage_errors_but_not_class_lists() -> None:
    lines = [
        "INFO all good",
        "java.lang.NoSuchMethodError: 'void com.x.Y.z()'",
        f"{CLASS_LIST_MARK}stock duplicate org/x/NoClassDefFoundError.class in ['a', 'b']",
        "WARN Caused by: java.lang.ClassNotFoundException: org.foo.Bar",
        "INFO DeltaSink: Skipping already complete epoch 4, in query q",
        "DEBUG com.fasterxml.jackson something",
    ]
    r = scan_log(lines)
    assert r["error_hits"] == 2
    assert r["error_kinds"] == {"NoSuchMethodError": 1, "ClassNotFoundException": 1}
    assert len(r["skip_epoch_lines"]) == 1 and len(r["jackson_lines"]) == 1


def test_overall_needs_every_check_and_no_failures() -> None:
    checks = [{"id": "C1", "ok": True}, {"id": "C5", "ok": None}, {"id": "C13", "ok": True}]
    assert overall(checks, ["C1", "C5", "C13"]) == ("PASS", [])
    assert overall(checks, ["C1", "C2"]) == ("FAIL", ["C2"])  # missing check = fail
    assert overall([*checks[:1], {"id": "C5", "ok": False}], ["C1", "C5"]) == ("FAIL", ["C5"])
