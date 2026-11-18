"""Delta compatibility gate (Phase 2 slice 1, docs/DECISIONS.md D2.1).

Proves that `io.delta:delta-spark_4.1_2.13:4.4.1` works on our exact stack
(Spark 4.1.4, Scala 2.13.17, Java 17, Python 3.11) before anything depends on
it, and records facts that later slices need. Everything happens in temp
folders under /tmp in the container; ./data is never touched.

Checks (G = gate, a failure blocks the pin; R = records a fact):
  C1  G  versions: Spark, Scala, Java, pip delta-spark/pyspark, Delta jar
  C2  G  classpath, for the jars WE ADD (fetch_jars.sh): no other jar with the
         same file-name artifact or group:artifact; no class also in another
         jar (ignore list: UnusedStubClass only); no add/skip decision changed
         by a file-name misparse (misparses are printed); Jackson jars
         aligned; excluded jars absent; jar lock verified at build.
         Stock-Spark same-name pairs and duplicate classes are printed and
         compared with recorded baselines (WARNING on drift, not a failure).
  C3  G  path table: write 3 rows, read 3, first commit file exists
  C4  G  insert-only MERGE (SQL) twice: 5 rows, then still 5 (R: metric keys)
  C5  R  MERGE with a partition BETWEEN bound: files scanned vs total
  C6  G  12 commits -> Delta checkpoint parquet; fresh read gives 12 rows
  C7  G  sum of live add.stats.numRecords (minus deletion vectors) == count
  C8  G  streaming sink JSON -> Delta, availableNow, restart: each row once
         (R: txn actions appId/version)
  C9  G  sink replay: delete last commits/N, restart: batch N re-runs, Delta
         skips it, no new version, no duplicates
  C10 G  Delta source -> foreachBatch insert-only MERGE (Python API), restart:
         target == distinct source (R: source offsets JSON)
  C11 G  foreachBatch replay: delete last commits/N, restart: batch re-runs,
         MERGE inserts 0 rows, target unchanged
  C12 G  order: at every progress event, commits/N and the batch's Delta
         version already exist
  C13 G  log scan of the whole run: no linkage/class-loading errors
  C14 G  jackson-datatype-jdk8 2.21.2 on databind 2.21.2: Optional round trip
         (R: were Delta Kernel's Jackson-using classes loaded by C3-C12?)

Result: PASS only if every G check passed and no check raised. Exit 0/1.

How it runs: the parent process starts a child (`--child`) that runs C1-C12
and C14 with all output (Python and JVM) going to one log file. The parent
then scans that log for C13 and prints a single JSON report. This is the only
way to see JVM errors that never reach Python.

Run (inside the Spark image):
    python -m streaming.delta_compat_check [--log-file PATH] [--keep]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import zipfile
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger("streaming.delta_compat_check")

# --- expected stack -------------------------------------------------------------
EXPECTED_SPARK = "4.1.4"
EXPECTED_SCALA = "2.13.17"
EXPECTED_JAVA_MAJOR = 17
EXPECTED_DELTA = "4.4.1"
EXPECTED_DELTA_JAR = "delta-spark_4.1_2.13-4.4.1.jar"

LIST_DIR = Path("/opt/valkyrie")
LOCK_MARKER = LIST_DIR / "lock-verified.txt"

# The only class allowed in an added jar AND another jar: Spark's empty
# placeholder class that every Spark module jar carries.
IGNORED_DUP_CLASSES = frozenset({"org/apache/spark/unused/UnusedStubClass.class"})

# Duplicate classes among stock Spark 4.1.4 jars (pip pyspark 4.1.4), measured
# in slice 1. Drift only WARNS (a Spark upgrade should be noticed, not blocked).
STOCK_DUP_BASELINE: dict[tuple[str, ...], int] = {
    ("orc-core-2.2.2-shaded-protobuf.jar", "orc-format-1.1.1-shaded-protobuf.jar",
     "orc-mapreduce-2.2.2-shaded-protobuf.jar"): 735,
    ("parquet-column-1.16.0.jar", "parquet-hadoop-1.16.0.jar"): 382,
    ("javax.jdo-3.2.0-m3.jar", "jdo-api-3.0.1.jar"): 180,
    ("java-diff-utils-4.16.jar", "scala-compiler-2.13.17.jar"): 42,
    ("jta-1.1.jar", "transaction-api-1.1.jar"): 18,
    ("jline-2.14.6.jar", "leveldbjni-all-1.8.jar"): 4,
    ("hive-common-2.3.10.jar", "hive-storage-api-2.8.1.jar"): 3,
}

# Stock jar pairs that share a FILE-NAME artifact (pip pyspark 4.1.4), measured in
# slice 1: hive-shims-0.23 is misread by the file-name rule (its artifactId really
# is "hive-shims-0.23"); the two jline jars are different libraries (groupIds
# jline vs org.jline). Drift only WARNS, like STOCK_DUP_BASELINE.
STOCK_SAME_NAME_BASELINE: dict[str, tuple[str, ...]] = {
    "hive-shims": ("hive-shims-0.23-2.3.10.jar", "hive-shims-2.3.10.jar"),
    "jline": ("jline-2.14.6.jar", "jline-3.29.0-jdk8.jar"),
}

# Jars excluded from the Delta fetch on purpose (D2.1); they must stay out.
FORBIDDEN_JAR_PATTERNS = (
    re.compile(r"^commons-logging-.*\.jar$"),
    re.compile(r"^shims-.*\.jar$"),
    re.compile(r"^RoaringBitmap-0\..*\.jar$"),
    re.compile(r"^delta-spark_2\.13-.*\.jar$"),  # unsuffixed = built for Spark 4.2
)

# C13: Java linkage / class-loading failures (LinkageError and its subclasses,
# plus ClassNotFoundException). Wider than the plan's three names on purpose.
LOG_ERROR_RE = re.compile(
    r"\b(NoSuchMethodError|NoSuchFieldError|NoClassDefFoundError|ClassNotFoundException|"
    r"LinkageError|IncompatibleClassChangeError|AbstractMethodError|"
    r"UnsupportedClassVersionError|ClassFormatError|VerifyError)\b"
)
SKIP_EPOCH_RE = re.compile(r"Skipping already complete epoch", re.IGNORECASE)
# C2 writes class lists to the log with this prefix; C13 does not scan them (a
# class NAME like ...NoClassDefFoundError.class in a list is not an error).
CLASS_LIST_MARK = "[C2 class list] "

_ARTIFACT_RE = re.compile(r"-[0-9][^-]*(-[^-]+)?\.jar$")
_COMMIT_RE = re.compile(r"^\d{20}\.json$")


# --- results ------------------------------------------------------------------
@dataclass
class Check:
    """One check result. ok: True/False for G checks; None = R fact recorded."""

    id: str
    kind: str
    ok: bool | None
    summary: str
    details: dict[str, Any] = field(default_factory=dict)


# --- pure helpers (no pyspark; unit-tested in tests/unit) -------------------------
def artifact_name(jar: str) -> str:
    """`jackson-core-2.21.2.jar` -> `jackson-core` (same rule as fetch_jars.sh)."""
    return _ARTIFACT_RE.sub("", jar)


def jar_version(jar: str) -> str:
    """`jackson-core-2.21.2.jar` -> `2.21.2`."""
    return jar[len(artifact_name(jar)) + 1:-len(".jar")]


def parse_entries(list_texts: Iterable[str]) -> list[dict[str, str | None]]:
    """Add/skip decisions from /opt/valkyrie/jars-*.txt (written by fetch_jars.sh).

    Line formats:
        add   <file> <group:artifact:version>
        skip  <file> <group:artifact:version> (Spark already ships <jar>)
    """
    entries: list[dict[str, str | None]] = []
    for text in list_texts:
        for line in text.splitlines():
            parts = line.split()
            if len(parts) < 3 or parts[0] not in ("add", "skip"):
                continue
            shipped = parts[-1].rstrip(")") if parts[0] == "skip" else None
            entries.append({"action": parts[0], "file": parts[1], "coord": parts[2],
                            "shipped": shipped})
    return entries


def parse_added(list_texts: Iterable[str]) -> list[str]:
    """Jar file names from the "add" lines of /opt/valkyrie/jars-*.txt."""
    return [str(e["file"]) for e in parse_entries(list_texts) if e["action"] == "add"]


def _would_match(real_artifact: str, jars: Iterable[str], exclude: str) -> list[str]:
    """Jars that fetch_jars.sh's glob `<artifact>-[0-9]*.jar` would match."""
    pat = re.compile(re.escape(real_artifact) + r"-[0-9].*\.jar")
    return sorted(j for j in jars if j != exclude and pat.fullmatch(j))


def audit_decisions(entries: list[dict[str, str | None]],
                    own_coords: dict[str, tuple[str, str]], jars: list[str]
                    ) -> tuple[list[str], list[str], list[str]]:
    """Did the file-name rule misread any add/skip decision, and did that matter?

    For every entry, the artifact parsed from the file name is compared with the
    real artifactId (from Ivy). On a mismatch the decision is recomputed with the
    real name against the final jar list: "add" is right if no other jar matches
    `<real>-<digit>*.jar`; "skip" is right if some jar does. For a skip, the
    Spark jar that caused it must also have the same real artifactId, when that
    jar carries its own pom.properties.

    Returns:
        (misparsed names, decisions affected (= C2 failure), skips confirmed by
        file name only because the shipped jar has no pom.properties).
    """
    misparsed: list[str] = []
    affected: list[str] = []
    name_only: list[str] = []
    for e in entries:
        file, coord, action = str(e["file"]), str(e["coord"]), str(e["action"])
        real = coord.split(":")[1]
        if artifact_name(file) != real:
            matches = _would_match(real, jars, exclude=file)
            right = (not matches) if action == "add" else bool(matches)
            note = (f"{action} {file}: parsed '{artifact_name(file)}', real artifactId "
                    f"'{real}'; with the real name it would be "
                    f"{'skip' if matches else 'add'} {matches or ''}")
            misparsed.append(note)
            if not right:
                affected.append(note)
        shipped = e.get("shipped")
        if action == "skip" and shipped:
            ship = own_coords.get(shipped)
            if ship is None:
                name_only.append(f"{file} ({coord}) vs {shipped}")
            elif ship[1] != real:
                affected.append(f"skip {file} ({coord}): Spark's {shipped} is "
                                f"{ship[0]}:{ship[1]}, a different artifact")
    return misparsed, affected, name_only


def own_coordinate(jar: str, props: list[dict[str, str]]) -> tuple[str, str] | None:
    """(groupId, artifactId) of the jar itself among its pom.properties entries.

    A jar may embed pom.properties of shaded libraries; its own entry is the one
    whose artifactId starts the file name (longest match wins).
    """
    mine = [(p["groupId"], p["artifactId"]) for p in props
            if p.get("artifactId") and p.get("groupId") and jar.startswith(p["artifactId"] + "-")]
    return max(mine, key=lambda c: len(c[1])) if mine else None


def same_artifact_twice(jars: Iterable[str]) -> dict[str, list[str]]:
    """Artifacts present in more than one version (by file name)."""
    by_artifact: dict[str, list[str]] = defaultdict(list)
    for jar in jars:
        by_artifact[artifact_name(jar)].append(jar)
    return {a: sorted(js) for a, js in by_artifact.items() if len(js) > 1}


def classify_duplicates(owners: dict[str, list[str]], added: set[str]
                        ) -> tuple[dict[str, list[str]], dict[tuple[str, ...], list[str]]]:
    """Split duplicate classes into (involving an added jar, stock-only groups).

    Returns:
        ({class: [jars]} for classes in an added jar and another jar, minus the
         ignore list), ({sorted jar tuple: [classes]} for stock-only duplicates).
    """
    involving_added: dict[str, list[str]] = {}
    stock: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for cls, jars in owners.items():
        if len(jars) < 2 or cls in IGNORED_DUP_CLASSES:
            continue
        if added.intersection(jars):
            involving_added[cls] = sorted(jars)
        else:
            stock[tuple(sorted(jars))].append(cls)
    return involving_added, dict(stock)


def stock_drift(stock: dict[tuple[str, ...], list[str]],
                baseline: dict[tuple[str, ...], int]) -> list[str]:
    """Differences between measured stock duplicate groups and the baseline."""
    measured = {k: len(v) for k, v in stock.items()}
    drift = [f"group {list(k)}: baseline {baseline.get(k)}, now {measured.get(k)}"
             for k in sorted(measured.keys() | baseline.keys())
             if measured.get(k) != baseline.get(k)]
    total_now, total_base = sum(measured.values()), sum(baseline.values())
    if total_now != total_base:
        drift.append(f"total: baseline {total_base}, now {total_now}")
    return drift


def jackson_problems(jackson_jars: dict[str, str]) -> list[str]:
    """Check {jar file: version} of real Jackson jars against the alignment rule.

    Rule (D2.1): every Jackson jar has exactly jackson-databind's version,
    except jackson-annotations, which is versioned major.minor only since 2.20
    and must equal databind's major.minor.
    """
    databind = [v for j, v in jackson_jars.items() if artifact_name(j) == "jackson-databind"]
    if len(databind) != 1:
        return [f"expected exactly one jackson-databind jar, found {len(databind)}"]
    want = databind[0]
    want_minor = ".".join(want.split(".")[:2])
    problems = []
    for jar, version in sorted(jackson_jars.items()):
        expected = want_minor if artifact_name(jar) == "jackson-annotations" else want
        if version != expected:
            problems.append(f"{jar}: version {version}, expected {expected}")
    return problems


def read_commits(table: Path) -> list[tuple[int, list[dict[str, Any]]]]:
    """All JSON commits of a Delta table as (version, actions), oldest first."""
    out = []
    for f in sorted((table / "_delta_log").iterdir()):
        if _COMMIT_RE.match(f.name):
            actions = [json.loads(line) for line in f.read_text().splitlines() if line.strip()]
            out.append((int(f.name[:20]), actions))
    return out


def _dv_key(action: dict[str, Any]) -> str:
    dv = action.get("deletionVector") or {}
    return f"{dv.get('storageType', '')}{dv.get('pathOrInlineDv', '')}{dv.get('offset', '')}"


def reconcile(commits: list[tuple[int, list[dict[str, Any]]]]) -> dict[str, Any]:
    """Replay add/remove actions; sum live numRecords minus deletion vectors.

    A file is identified by (path, deletion vector), because Delta re-adds the
    same path with a new deletion vector when rows are deleted from it.
    """
    live: dict[tuple[str, str], dict[str, Any]] = {}
    removes = 0
    for _, actions in commits:
        for action in actions:
            if "add" in action:
                add = action["add"]
                live[(add["path"], _dv_key(add))] = add
            elif "remove" in action:
                rem = action["remove"]
                removes += 1
                live.pop((rem["path"], _dv_key(rem)), None)
    records = 0
    missing_stats = 0
    dv_files = 0
    for add in live.values():
        stats = json.loads(add["stats"]) if add.get("stats") else {}
        if "numRecords" not in stats:
            missing_stats += 1
            continue
        dv = add.get("deletionVector")
        dv_files += 1 if dv else 0
        records += int(stats["numRecords"]) - int((dv or {}).get("cardinality", 0))
    return {"live_files": len(live), "removes": removes, "records": records,
            "files_without_numRecords": missing_stats, "files_with_deletion_vector": dv_files}


def txn_actions(commits: list[tuple[int, list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    """The `txn` (SetTransaction) actions in commit order, with their version."""
    return [{"delta_version": v, "appId": a["txn"]["appId"], "version": a["txn"]["version"]}
            for v, actions in commits for a in actions if "txn" in a]


def scan_log(lines: Iterable[str], max_examples: int = 20) -> dict[str, Any]:
    """C13 scan: linkage/class-loading errors, plus jackson and skip-epoch lines."""
    errors: list[str] = []
    jackson: list[str] = []
    skip_epoch: list[str] = []
    total = 0
    error_kinds: Counter[str] = Counter()
    for line in lines:
        total += 1
        line = line.rstrip("\n")
        if line.startswith(CLASS_LIST_MARK):
            continue
        m = LOG_ERROR_RE.search(line)
        if m:
            error_kinds[m.group(1)] += 1
            if len(errors) < max_examples:
                errors.append(line[:300])
        if "jackson" in line.lower() and len(jackson) < max_examples:
            jackson.append(line[:300])
        if SKIP_EPOCH_RE.search(line):
            skip_epoch.append(line[:300])
    return {"lines": total, "error_hits": sum(error_kinds.values()),
            "error_kinds": dict(error_kinds), "error_examples": errors,
            "jackson_lines": jackson, "skip_epoch_lines": skip_epoch}


# --- child: the Spark checks -----------------------------------------------------
class Ctx:
    """Shared state of the child run: Spark session, work dir, listener data."""

    def __init__(self, spark: Any, work: Path) -> None:
        self.spark = spark
        self.work = work
        # query name -> {"kind": "sink"|"foreachbatch", "table", "checkpoint", "fb_versions"}
        self.registry: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.fb_calls: list[dict[str, Any]] = []

    def path(self, name: str) -> Path:
        return self.work / name


def _jar_of(jvm: Any, cls: str) -> str | None:
    src = jvm.java.lang.Class.forName(cls).getProtectionDomain().getCodeSource()
    return str(src.getLocation()).rsplit("/", 1)[-1] if src is not None else None


def _spark_jars_dir() -> Path:
    import pyspark

    return Path(pyspark.__file__).parent / "jars"


def _last_metrics(spark: Any, table: Path) -> tuple[int, dict[str, str]]:
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, str(table)).history(1).collect()[0]
    return int(row["version"]), dict(row["operationMetrics"] or {})


def _count(spark: Any, table: Path) -> tuple[int, int]:
    df = spark.read.format("delta").load(str(table))
    return df.count(), df.select("id").distinct().count()


def _write_json_file(folder: Path, name: str, ids: Iterable[int]) -> None:
    """Write one JSON-lines input file atomically (temp name, then rename)."""
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / f".{name}.tmp"
    tmp.write_text("".join(json.dumps({"id": i, "v": f"v{i}"}) + "\n" for i in ids))
    tmp.rename(folder / name)


def _delete_last_commit(checkpoint: Path) -> int:
    """Delete the checkpoint's last commits/N (and its .crc). Returns N."""
    commits = checkpoint / "commits"
    batch = max(int(f.name) for f in commits.iterdir() if f.name.isdigit())
    (commits / str(batch)).unlink()
    crc = commits / f".{batch}.crc"
    if crc.exists():
        crc.unlink()
    return batch


def _wait_for_events(ctx: Ctx, query: Any, timeout_s: float = 20.0) -> list[int]:
    """Wait until the listener saw a progress event for every batch of this run."""
    batches = sorted({int(p.batchId if hasattr(p, "batchId") else p["batchId"])
                      for p in query.recentProgress})
    run_id = str(query.runId)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        seen = {e["batch_id"] for e in ctx.events if e["run_id"] == run_id}
        if set(batches) <= seen:
            return batches
        time.sleep(0.2)
    raise TimeoutError(f"listener missed events for run {run_id}: batches {batches}")


def make_listener(ctx: Ctx) -> Any:
    """C12 listener: at each progress event, check commits/N and the Delta version."""
    from pyspark.sql.streaming import StreamingQueryListener

    class OrderListener(StreamingQueryListener):
        def onQueryStarted(self, event: Any) -> None:
            pass

        def onQueryIdle(self, event: Any) -> None:
            pass

        def onQueryTerminated(self, event: Any) -> None:
            pass

        def onQueryProgress(self, event: Any) -> None:
            p = event.progress
            info = ctx.registry.get(p.name)
            rec: dict[str, Any] = {"query": p.name, "run_id": str(p.runId),
                                   "batch_id": int(p.batchId), "rows": int(p.numInputRows)}
            try:
                if info is None:
                    rec.update(commit_exists=False, delta_ok=False, why="unknown query")
                else:
                    rec["commit_exists"] = (info["checkpoint"] / "commits" /
                                            str(p.batchId)).exists()
                    if info["kind"] == "sink":
                        txns = txn_actions(read_commits(info["table"]))
                        rec["delta_ok"] = any(t["appId"] == str(p.id) and
                                              t["version"] == p.batchId for t in txns)
                    else:
                        v = info["fb_versions"].get(int(p.batchId))
                        rec["delta_version"] = v
                        rec["delta_ok"] = v is not None and (
                            info["table"] / "_delta_log" / f"{v:020d}.json").exists()
                    if p.sources:
                        rec["start_offset"] = p.sources[0].startOffset
                        rec["end_offset"] = p.sources[0].endOffset
            except Exception as exc:  # never break the listener thread
                rec.update(commit_exists=False, delta_ok=False, why=repr(exc))
            ctx.events.append(rec)

    return OrderListener()


def c1_versions(ctx: Ctx) -> Check:
    import importlib.metadata as md

    spark, jvm = ctx.spark, ctx.spark._jvm
    try:
        scala = str(jvm.scala.util.Properties.versionNumberString())
    except Exception:
        scala = jar_version(next(j for j in os.listdir(_spark_jars_dir())
                                 if j.startswith("scala-library-")))
    java = str(jvm.java.lang.System.getProperty("java.version"))
    jars = sorted(os.listdir(_spark_jars_dir()))
    d = {
        "spark": spark.version, "scala": scala, "java": java,
        "pip_pyspark": md.version("pyspark"), "pip_delta_spark": md.version("delta-spark"),
        "delta_jars": [j for j in jars if j.startswith("delta-")],
        "DeltaLog_loaded_from": _jar_of(jvm, "org.apache.spark.sql.delta.DeltaLog"),
    }
    ok = (d["spark"] == EXPECTED_SPARK and scala == EXPECTED_SCALA
          and int(java.split(".")[0]) == EXPECTED_JAVA_MAJOR
          and d["pip_pyspark"] == EXPECTED_SPARK and d["pip_delta_spark"] == EXPECTED_DELTA
          and d["DeltaLog_loaded_from"] == EXPECTED_DELTA_JAR
          and [j for j in jars if j.startswith("delta-spark")] == [EXPECTED_DELTA_JAR])
    return Check("C1", "G", ok, f"spark {d['spark']}, scala {scala}, java {java}, "
                 f"delta pip {d['pip_delta_spark']}, DeltaLog from {d['DeltaLog_loaded_from']}", d)


def c2_classpath(ctx: Ctx) -> Check:
    jars_dir = _spark_jars_dir()
    jars = sorted(j for j in os.listdir(jars_dir) if j.endswith(".jar"))
    lists = {p.name: p.read_text() for p in sorted(LIST_DIR.glob("jars-*.txt"))}
    entries = parse_entries(lists.values())
    added_coords = {str(e["file"]): str(e["coord"]) for e in entries if e["action"] == "add"}
    added = set(added_coords)

    owners: dict[str, list[str]] = defaultdict(list)
    jackson: dict[str, str] = {}
    relocated_jackson: dict[str, list[str]] = defaultdict(list)
    own_coords: dict[str, tuple[str, str]] = {}
    for jar in jars:
        props: list[dict[str, str]] = []
        with zipfile.ZipFile(jars_dir / jar) as z:
            real_jackson = False
            for n in z.namelist():
                if n.endswith(".class") and not n.startswith("META-INF/") \
                        and "module-info" not in n:
                    owners[n].append(jar)
                    real_jackson = real_jackson or n.startswith("com/fasterxml/jackson/")
                elif n.startswith("META-INF/maven/") and n.endswith("pom.properties"):
                    pp = dict(line.split("=", 1) for line in
                              z.read(n).decode(errors="replace").splitlines()
                              if "=" in line and not line.startswith("#"))
                    props.append(pp)
                    if n.startswith("META-INF/maven/com.fasterxml.jackson"):
                        relocated_jackson[jar].append(
                            f"{pp.get('artifactId')}:{pp.get('version')}")
            if real_jackson:
                jackson[jar] = jar_version(jar)
        coord = own_coordinate(jar, props)
        if coord:
            own_coords[jar] = coord
    for jar in jackson:  # embedded metadata of a real Jackson jar is not "relocated"
        relocated_jackson.pop(jar, None)
    for jar, c in added_coords.items():  # added jars: exact coordinate from Ivy
        own_coords[jar] = (c.split(":")[0], c.split(":")[1])

    # Name rule: fail only if an ADDED jar shares its file-name artifact or its
    # group:artifact with another jar; stock pairs are compared with the baseline.
    twice = same_artifact_twice(jars)
    twice_added = {a: js for a, js in twice.items() if added.intersection(js)}
    twice_stock = {a: tuple(js) for a, js in twice.items() if a not in twice_added}
    for jar in sorted(added):
        same = sorted(j for j, c in own_coords.items() if j != jar and c == own_coords[jar])
        if same:
            twice_added[f"{own_coords[jar][0]}:{own_coords[jar][1]}"] = [jar, *same]
    name_drift = [f"{a}: baseline {list(STOCK_SAME_NAME_BASELINE.get(a, ()))}, "
                  f"now {list(twice_stock.get(a, ()))}"
                  for a in sorted(twice_stock.keys() | STOCK_SAME_NAME_BASELINE.keys())
                  if twice_stock.get(a) != STOCK_SAME_NAME_BASELINE.get(a)]

    misparsed, affected, name_only = audit_decisions(entries, own_coords, jars)
    involving_added, stock = classify_duplicates(owners, added)
    drift = stock_drift(stock, STOCK_DUP_BASELINE)
    jackson_bad = jackson_problems(jackson)
    forbidden = [j for j in jars if any(p.match(j) for p in FORBIDDEN_JAR_PATTERNS)]
    missing_added = sorted(a for a in added if a not in jars)
    lock_ok = LOCK_MARKER.is_file()

    # Every duplicate goes to the log: added-jar ones per class, stock per class too.
    for cls, js in sorted(involving_added.items()):
        print(f"{CLASS_LIST_MARK}DUPLICATE (added jar) {cls} in {js}")
    for group, classes in sorted(stock.items()):
        for cls in sorted(classes):
            print(f"{CLASS_LIST_MARK}stock duplicate {cls} in {list(group)}")
    for line in drift + name_drift:
        log.warning("C2 stock baseline drift: %s", line)
    for line in misparsed:
        log.warning("C2 file-name rule misparsed (fetch_jars.sh limit): %s", line)
    for line in affected:
        log.error("C2 add/skip DECISION AFFECTED: %s", line)

    ok = not (twice_added or involving_added or jackson_bad or forbidden or missing_added
              or affected) and lock_ok
    d = {
        "jars_total": len(jars),
        "added_jars": added_coords,
        "add_skip_lists": {k: v.splitlines() for k, v in lists.items()},
        "same_name_involving_added": twice_added,
        "same_name_stock": {a: list(js) for a, js in twice_stock.items()},
        "add_skip_decisions_checked": len(entries),
        "add_skip_misparsed_names": misparsed,
        "add_skip_decisions_affected": affected,
        "skips_confirmed_by_name_only": name_only,
        "ignored_classes": sorted(IGNORED_DUP_CLASSES),
        "duplicates_involving_added_jars": involving_added,
        "stock_duplicates": {"total": sum(len(v) for v in stock.values()),
                             "groups": [{"jars": list(k), "classes": len(v),
                                         "example": sorted(v)[0]}
                                        for k, v in sorted(stock.items())]},
        "stock_baseline_drift_WARNING": drift + name_drift,
        "jackson_jars": jackson, "jackson_problems": jackson_bad,
        "jackson_relocated_copies_not_checked": dict(relocated_jackson),
        "forbidden_jars_present": forbidden,
        "added_but_missing": missing_added,
        "lock_verified_marker": LOCK_MARKER.read_text().strip() if lock_ok else None,
    }
    summary = (f"{len(jars)} jars, {len(added)} added; same-name with added {len(twice_added)}"
               f", stock {len(twice_stock)}; add/skip names misparsed {len(misparsed)} of "
               f"{len(entries)}, decisions affected {len(affected)}; added-jar dup classes "
               f"{len(involving_added)}; stock dup "
               f"classes {d['stock_duplicates']['total']}; baseline drift "
               f"{len(drift + name_drift)}; jackson problems {len(jackson_bad)}; forbidden "
               f"{len(forbidden)}; lock verified {lock_ok}")
    return Check("C2", "G", ok, summary, d)


def c3_create(ctx: Ctx) -> Check:
    spark, t = ctx.spark, ctx.path("c3_table")
    spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, v STRING") \
        .coalesce(1).write.format("delta").save(str(t))
    n, _ = _count(spark, t)
    first = t / "_delta_log" / f"{0:020d}.json"
    engine = next((a["commitInfo"].get("engineInfo") for a in read_commits(t)[0][1]
                   if "commitInfo" in a), None) if first.exists() else None
    ok = n == 3 and first.exists()
    return Check("C3", "G", ok, f"rows {n}, {first.name} exists {first.exists()}, "
                 f"engineInfo {engine}", {"rows": n, "engineInfo": engine})


def c4_merge(ctx: Ctx) -> Check:
    spark, t = ctx.spark, ctx.path("c3_table")
    spark.createDataFrame([(3, "c-new"), (4, "d"), (5, "e")], "id INT, v STRING") \
        .createOrReplaceTempView("c4_src")
    sql = (f"MERGE INTO delta.`{t}` t USING c4_src s ON t.id = s.id "
           "WHEN NOT MATCHED THEN INSERT *")
    spark.sql(sql)
    n1, d1 = _count(spark, t)
    v1, m1 = _last_metrics(spark, t)
    spark.sql(sql)
    n2, d2 = _count(spark, t)
    v2, m2 = _last_metrics(spark, t)
    kept = spark.read.format("delta").load(str(t)).where("id = 3").collect()[0]["v"]
    ok = n1 == 5 and n2 == 5 and d2 == 5 and kept == "c"
    # An insert-only MERGE that inserts nothing writes no commit, so history(1)
    # still shows MERGE 1: report "no commit" rather than MERGE 1's metrics.
    m2_report: dict[str, str] | str = m2 if v2 != v1 else "no commit (no new Delta version)"
    return Check("C4", "G", ok, f"after MERGE 1: {n1} rows (v{v1}), after MERGE 2: {n2} rows "
                 f"(v{v2}, new commit {v2 != v1}); id 3 kept '{kept}'",
                 {"metric_keys": sorted(m1), "merge1_metrics": m1, "merge2_metrics": m2_report,
                  "merge2_new_version": v2 != v1})


def c5_pruning(ctx: Ctx) -> Check:
    spark, t = ctx.spark, ctx.path("c5_partitioned")
    rows = [(d * 10 + i, f"2026-01-0{d}") for d in range(1, 6) for i in range(4)]
    spark.createDataFrame(rows, "id INT, d STRING").selectExpr("id", "DATE(d) AS event_date") \
        .repartition("event_date").write.format("delta").partitionBy("event_date").save(str(t))
    total_files = reconcile(read_commits(t))["live_files"]
    spark.createDataFrame([(30, "2026-01-03"), (99, "2026-01-03")], "id INT, d STRING") \
        .selectExpr("id", "DATE(d) AS event_date").createOrReplaceTempView("c5_src")
    spark.sql(f"MERGE INTO delta.`{t}` t USING c5_src s ON t.id = s.id AND t.event_date "
              "BETWEEN DATE'2026-01-03' AND DATE'2026-01-03' WHEN NOT MATCHED THEN INSERT *")
    _, m = _last_metrics(spark, t)
    file_metrics = {k: v for k, v in m.items() if "File" in k or "Partition" in k}
    return Check("C5", "R", None, f"target files before MERGE {total_files}; "
                 f"skipping metrics {file_metrics}",
                 {"target_files_before": total_files, "metrics": m})


def c6_checkpoint(ctx: Ctx) -> Check:
    spark, t = ctx.spark, ctx.path("c6_commits")
    for i in range(12):
        spark.range(i, i + 1).selectExpr("CAST(id AS INT) AS id").coalesce(1) \
            .write.format("delta").mode("append").save(str(t))
    log_dir = t / "_delta_log"
    cps = sorted(f.name for f in log_dir.iterdir() if ".checkpoint" in f.name)
    last = (log_dir / "_last_checkpoint")
    try:
        ctx.spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
        cache_cleared = True
    except Exception:
        cache_cleared = False
    n, _ = _count(spark, t)
    ok = f"{10:020d}.checkpoint.parquet" in cps and n == 12
    return Check("C6", "G", ok, f"checkpoints {cps}; rows after fresh read {n}",
                 {"checkpoints": cps, "rows": n, "cache_cleared": cache_cleared,
                  "_last_checkpoint": last.read_text().strip() if last.exists() else None})


def c7_numrecords(ctx: Ctx) -> Check:
    spark, t = ctx.spark, ctx.path("c7_stats")
    for start in (0, 5):
        spark.range(start, start + 5).selectExpr("CAST(id AS INT) AS id").coalesce(1) \
            .write.format("delta").mode("append").save(str(t))
    spark.sql(f"DELETE FROM delta.`{t}` WHERE id % 3 = 0")
    results = {}
    ok = True
    for name in ("c7_stats", "c3_table", "c6_commits", "c5_partitioned"):
        rec = reconcile(read_commits(ctx.path(name)))
        rec["spark_count"], _ = _count(spark, ctx.path(name))
        rec["equal"] = rec["records"] == rec["spark_count"] and \
            rec["files_without_numRecords"] == 0
        ok = ok and rec["equal"]
        results[name] = rec
    c7 = results["c7_stats"]
    return Check("C7", "G", ok, f"c7: live sum {c7['records']} == count {c7['spark_count']} "
                 f"(removes {c7['removes']}, DV files {c7['files_with_deletion_vector']}); "
                 f"all tables equal {ok}", results)


def _run_sink(ctx: Ctx, inp: Path, out: Path, cp: Path) -> tuple[str, list[int]]:
    q = (ctx.spark.readStream.schema("id INT, v STRING").option("maxFilesPerTrigger", 1)
         .json(str(inp)).writeStream.format("delta").queryName("c8_sink")
         .option("checkpointLocation", str(cp)).trigger(availableNow=True).start(str(out)))
    q.awaitTermination()
    return str(q.id), _wait_for_events(ctx, q)


def c8_sink(ctx: Ctx) -> Check:
    inp, out, cp = ctx.path("c8_in"), ctx.path("c8_sink"), ctx.path("c8_cp")
    ctx.registry["c8_sink"] = {"kind": "sink", "table": out, "checkpoint": cp,
                               "fb_versions": {}}
    for k in range(3):
        _write_json_file(inp, f"part{k}.json", range(k * 10, k * 10 + 10))
    qid1, b1 = _run_sink(ctx, inp, out, cp)
    for k in range(3, 5):
        _write_json_file(inp, f"part{k}.json", range(k * 10, k * 10 + 10))
    qid2, b2 = _run_sink(ctx, inp, out, cp)
    n, distinct = _count(ctx.spark, out)
    ids = {r["id"] for r in ctx.spark.read.format("delta").load(str(out)).collect()}
    txns = txn_actions(read_commits(out))
    ok = (n == 50 and distinct == 50 and ids == set(range(50)) and qid1 == qid2
          and [t["version"] for t in txns] == b1 + b2
          and all(t["appId"] == qid1 for t in txns))
    return Check("C8", "G", ok, f"run1 batches {b1}, run2 batches {b2}; rows {n}, distinct "
                 f"{distinct}; query id stable {qid1 == qid2}; txn versions "
                 f"{[t['version'] for t in txns]}",
                 {"query_id": qid1, "txn_actions": txns})


def c9_sink_replay(ctx: Ctx) -> Check:
    inp, out, cp = ctx.path("c8_in"), ctx.path("c8_sink"), ctx.path("c8_cp")
    n_before, _ = _count(ctx.spark, out)
    v_before = max(v for v, _ in read_commits(out))
    deleted = _delete_last_commit(cp)
    _, rerun = _run_sink(ctx, inp, out, cp)
    n_after, distinct = _count(ctx.spark, out)
    v_after = max(v for v, _ in read_commits(out))
    restored = (cp / "commits" / str(deleted)).exists()
    ok = (deleted in rerun and n_after == n_before == distinct and v_after == v_before
          and restored)
    return Check("C9", "G", ok, f"deleted commits/{deleted}; re-run batches {rerun}; rows "
                 f"{n_before} -> {n_after}; Delta version {v_before} -> {v_after}; "
                 f"commits/{deleted} rewritten {restored}",
                 {"deleted_batch": deleted, "rerun_batches": rerun,
                  "delta_version_before": v_before, "delta_version_after": v_after})


def _fb_merge(ctx: Ctx, target: Path) -> Callable[[Any, int], None]:
    """foreachBatch body: dedupe within the batch, then insert-only MERGE."""
    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def run(batch_df: Any, batch_id: int) -> None:
        w = Window.partitionBy("id").orderBy("v")
        dedup = batch_df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
        v_before, _ = _last_metrics(ctx.spark, target)
        (DeltaTable.forPath(ctx.spark, str(target)).alias("t")
         .merge(dedup.alias("s"), "t.id = s.id").whenNotMatchedInsertAll().execute())
        v_after, metrics = _last_metrics(ctx.spark, target)
        ctx.registry["c10_fb"]["fb_versions"][int(batch_id)] = v_after
        ctx.fb_calls.append({"batch_id": int(batch_id), "rows_in": batch_df.count(),
                             "version_before": v_before, "version_after": v_after,
                             "inserted": metrics.get("numTargetRowsInserted")
                             if v_after != v_before else "no new version"})

    return run


def _run_fb(ctx: Ctx, src: Path, tgt: Path, cp: Path) -> tuple[str, list[int]]:
    q = (ctx.spark.readStream.format("delta").option("maxFilesPerTrigger", 1).load(str(src))
         .writeStream.queryName("c10_fb").foreachBatch(_fb_merge(ctx, tgt))
         .option("checkpointLocation", str(cp)).trigger(availableNow=True).start())
    q.awaitTermination()
    return str(q.id), _wait_for_events(ctx, q)


def _append_src(ctx: Ctx, src: Path, ids: list[int]) -> None:
    ctx.spark.createDataFrame([(i, f"v{i}") for i in ids], "id INT, v STRING").coalesce(1) \
        .write.format("delta").mode("append").save(str(src))


def c10_foreachbatch(ctx: Ctx) -> Check:
    src, tgt, cp = ctx.path("c10_src"), ctx.path("c10_tgt"), ctx.path("c10_cp")
    ctx.registry["c10_fb"] = {"kind": "foreachbatch", "table": tgt, "checkpoint": cp,
                              "fb_versions": {}}
    _append_src(ctx, src, [*range(10), 5])           # duplicate inside one commit
    _append_src(ctx, src, list(range(8, 15)))        # 8, 9 repeat across commits
    _append_src(ctx, src, [*range(14, 18), 16])
    ctx.spark.createDataFrame([], "id INT, v STRING").write.format("delta").save(str(tgt))
    _, b1 = _run_fb(ctx, src, tgt, cp)
    _append_src(ctx, src, list(range(15, 23)))        # overlaps the earlier ids
    _, b2 = _run_fb(ctx, src, tgt, cp)
    src_ids = {r["id"] for r in ctx.spark.read.format("delta").load(str(src)).collect()}
    n, distinct = _count(ctx.spark, tgt)
    tgt_ids = {r["id"] for r in ctx.spark.read.format("delta").load(str(tgt)).collect()}
    src_rows, _ = _count(ctx.spark, src)
    offsets = [{k: e.get(k) for k in ("batch_id", "start_offset", "end_offset")}
               for e in ctx.events if e["query"] == "c10_fb"]
    ok = tgt_ids == src_ids and n == distinct == len(src_ids)
    return Check("C10", "G", ok, f"batches {b1} + {b2}; source rows {src_rows}, distinct "
                 f"{len(src_ids)}; target rows {n}, distinct {distinct}",
                 {"source_offsets": offsets, "fb_calls": list(ctx.fb_calls)})


def c11_fb_replay(ctx: Ctx) -> Check:
    src, tgt, cp = ctx.path("c10_src"), ctx.path("c10_tgt"), ctx.path("c10_cp")
    n_before, _ = _count(ctx.spark, tgt)
    calls_before = len(ctx.fb_calls)
    deleted = _delete_last_commit(cp)
    _, rerun = _run_fb(ctx, src, tgt, cp)
    calls = ctx.fb_calls[calls_before:]
    n_after, distinct = _count(ctx.spark, tgt)
    src_distinct = ctx.spark.read.format("delta").load(str(src)).select("id").distinct().count()
    replay = [c for c in calls if c["batch_id"] == deleted]
    inserted_zero = bool(replay) and all(c["inserted"] in ("0", "no new version")
                                         for c in replay)
    ok = deleted in rerun and inserted_zero and n_after == n_before == distinct == src_distinct
    return Check("C11", "G", ok, f"deleted commits/{deleted}; re-run batches {rerun}; replay "
                 f"calls {replay}; target rows {n_before} -> {n_after}",
                 {"deleted_batch": deleted, "replay_calls": replay})


def c12_order(ctx: Ctx) -> Check:
    bad = [e for e in ctx.events if not (e.get("commit_exists") and e.get("delta_ok"))]
    queries = Counter(e["query"] for e in ctx.events)
    ok = not bad and queries.get("c8_sink", 0) > 0 and queries.get("c10_fb", 0) > 0
    return Check("C12", "G", ok, f"{len(ctx.events)} progress events {dict(queries)}; "
                 f"violations {len(bad)}",
                 {"violations": bad, "events": [
                     {k: e.get(k) for k in ("query", "batch_id", "rows", "commit_exists",
                                            "delta_ok")} for e in ctx.events]})


def c14_jackson(ctx: Ctx) -> Check:
    jvm, gw = ctx.spark._jvm, ctx.spark.sparkContext._gateway
    # R part first, before the probe loads anything: what did C3-C12 load?
    loaded: dict[str, Any] = {}
    try:
        cls_loader_cls = jvm.java.lang.Class.forName("java.lang.ClassLoader")
        params = gw.new_array(jvm.java.lang.Class, 1)
        params[0] = jvm.java.lang.Class.forName("java.lang.String")
        find = cls_loader_cls.getDeclaredMethod("findLoadedClass", params)
        find.setAccessible(True)
        loader = jvm.java.lang.Class.forName("org.apache.spark.sql.delta.DeltaLog") \
            .getClassLoader()
        for name in ("com.fasterxml.jackson.datatype.jdk8.Jdk8Module",
                     "io.delta.kernel.internal.metadatadomain.JsonMetadataDomain",
                     "io.delta.kernel.internal.metrics.MetricsReportSerializer"):
            arg = gw.new_array(jvm.java.lang.Object, 1)
            arg[0] = name
            loaded[name] = find.invoke(loader, arg) is not None
    except Exception as exc:
        loaded["error"] = repr(exc)

    mapper = jvm.com.fasterxml.jackson.databind.ObjectMapper()
    module = jvm.com.fasterxml.jackson.datatype.jdk8.Jdk8Module()
    mapper.registerModule(module)
    optional = jvm.java.util.Optional
    wrote = (str(mapper.writeValueAsString(optional.of(42))),
             str(mapper.writeValueAsString(optional.empty())))
    args = gw.new_array(jvm.java.lang.Class, 1)
    args[0] = jvm.java.lang.Class.forName("java.lang.Integer")
    typ = mapper.getTypeFactory().constructParametricType(
        jvm.java.lang.Class.forName("java.util.Optional"), args)
    read = (str(mapper.readValue("7", typ).toString()),
            str(mapper.readValue("null", typ).toString()))
    d = {"databind": str(mapper.version().toString()), "jdk8_module": str(module.version()
                                                                         .toString()),
         "jdk8_module_jar": _jar_of(jvm, "com.fasterxml.jackson.datatype.jdk8.Jdk8Module"),
         "write": wrote, "read": read, "loaded_during_C3_C12": loaded}
    ok = (wrote == ("42", "null") and read == ("Optional[7]", "Optional.empty")
          and d["databind"] == d["jdk8_module"] == "2.21.2")
    return Check("C14", "G", ok, f"databind {d['databind']}, jdk8 {d['jdk8_module']}; write "
                 f"{wrote}, read {read}; loaded during C3-C12: {loaded}", d)


CHILD_CHECKS: list[tuple[str, str, Callable[[Ctx], Check]]] = [
    ("C1", "G", c1_versions), ("C2", "G", c2_classpath), ("C3", "G", c3_create),
    ("C4", "G", c4_merge), ("C5", "R", c5_pruning), ("C6", "G", c6_checkpoint),
    ("C7", "G", c7_numrecords), ("C8", "G", c8_sink), ("C9", "G", c9_sink_replay),
    ("C10", "G", c10_foreachbatch), ("C11", "G", c11_fb_replay), ("C12", "G", c12_order),
    ("C14", "G", c14_jackson),
]


def build_spark() -> Any:
    """Local Spark with Delta's SQL extension and catalog (path tables only)."""
    from pyspark.sql import SparkSession

    return (SparkSession.builder.master("local[2]").appName("delta-compat-check")
            .config("spark.driver.memory", os.environ.get("DELTA_COMPAT_DRIVER_MEMORY", "1g"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def run_child(work: Path, report_path: Path) -> int:
    """Run C1-C12 and C14; write their results as JSON. Logs go to stdout/stderr."""
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = build_spark()  # JVM root logger stays at Spark's default (WARN here)
    try:  # Delta at INFO, so evidence such as "Skipping already complete epoch" is logged
        jvm = spark._jvm
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            "org.apache.spark.sql.delta", jvm.org.apache.logging.log4j.Level.toLevel("INFO"))
    except Exception:
        log.warning("could not raise the Delta log level", exc_info=True)
    ctx = Ctx(spark, work)
    spark.streams.addListener(make_listener(ctx))
    results: list[dict[str, Any]] = []
    for cid, kind, fn in CHILD_CHECKS:
        log.info("=== %s start", cid)
        try:
            check = fn(ctx)
        except Exception as exc:
            log.exception("%s raised", cid)
            check = Check(cid, kind, False, f"exception: {type(exc).__name__}: {exc}"[:500],
                          {"traceback": traceback.format_exc()[-4000:]})
        log.info("=== %s %s: %s", cid, {True: "PASS", False: "FAIL", None: "RECORDED"}
                 [check.ok], check.summary)
        results.append(asdict(check))
    report_path.write_text(json.dumps(results, default=str))
    spark.stop()
    return 0


# --- parent ------------------------------------------------------------------
def overall(checks: list[dict[str, Any]], expected_ids: Iterable[str]) -> tuple[str, list[str]]:
    """PASS if every expected check exists, G checks passed and nothing raised."""
    by_id = {c["id"]: c for c in checks}
    failed = [cid for cid in expected_ids if cid not in by_id or by_id[cid]["ok"] is False]
    return ("PASS" if not failed else "FAIL"), failed


def run_parent(args: argparse.Namespace) -> int:
    started = time.monotonic()
    work = Path(tempfile.mkdtemp(prefix="delta-compat-", dir=args.tmp_dir))
    log_path, report_path = work / "run.log", work / "child.json"
    with log_path.open("w") as logf:
        proc = subprocess.run([sys.executable, "-m", "streaming.delta_compat_check",
                               "--child", "--work", str(work), "--report", str(report_path)],
                              stdout=logf, stderr=subprocess.STDOUT, timeout=args.timeout,
                              check=False)
    checks: list[dict[str, Any]] = (json.loads(report_path.read_text())
                                    if report_path.exists() else [])
    with log_path.open(errors="replace") as f:
        scan = scan_log(f)
    c13_ok = scan["error_hits"] == 0 and proc.returncode == 0 and bool(checks)
    checks.insert(min(12, len(checks)), asdict(Check(
        "C13", "G", c13_ok, f"{scan['lines']} log lines; linkage/class-loading hits "
        f"{scan['error_hits']} {scan['error_kinds']}; child exit {proc.returncode}", scan)))
    expected = [cid for cid, _, _ in CHILD_CHECKS] + ["C13"]
    result, failed = overall(checks, expected)
    report = {"result": result, "failed": failed,
              "elapsed_s": round(time.monotonic() - started, 1),
              "checks": {c["id"]: c for c in checks}}
    if not checks:
        with log_path.open(errors="replace") as f:
            report["log_tail"] = f.readlines()[-60:]
    if args.log_file:
        shutil.copyfile(log_path, args.log_file)
        report["log_file"] = str(args.log_file)
    if not args.keep:
        shutil.rmtree(work, ignore_errors=True)
    else:
        report["work_dir"] = str(work)
    print(json.dumps(report, indent=1, default=str))
    return 0 if result == "PASS" else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Delta compatibility gate (C1-C14)")
    p.add_argument("--log-file", type=Path, help="copy the full run log here")
    p.add_argument("--keep", action="store_true", help="keep the temp work folder")
    p.add_argument("--tmp-dir", default="/tmp", help="parent of the temp work folder")
    p.add_argument("--timeout", type=int, default=1200, help="child timeout in seconds")
    p.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--work", type=Path, help=argparse.SUPPRESS)
    p.add_argument("--report", type=Path, help=argparse.SUPPRESS)
    args = p.parse_args(argv)
    if args.child:
        return run_child(args.work, args.report)
    return run_parent(args)


if __name__ == "__main__":
    sys.exit(main())
