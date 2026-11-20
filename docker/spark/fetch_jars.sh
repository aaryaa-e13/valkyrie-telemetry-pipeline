#!/bin/bash
# Resolve a Maven package (and its dependencies) at image BUILD time and add the
# jars to Spark's classpath, so the container needs no internet at runtime.
#
# Usage: fetch_jars.sh <maven-coordinate> <list-name> [<exclude-packages>]
#   e.g. fetch_jars.sh org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4 kafka
#        fetch_jars.sh io.delta:delta-spark_4.1_2.13:4.4.1 delta \
#            commons-logging:commons-logging,org.roaringbitmap:shims
#   <exclude-packages> is passed to spark-submit --exclude-packages
#   (comma-separated groupId:artifactId); Ivy then drops those modules from the
#   whole dependency tree.
#
# Skip rule: a jar whose artifact name already exists in Spark's jars folder
# (shipped by Spark, or added by an earlier run of this script) is not copied.
# Limitation (found in Phase 2 slice 1, docs/WHAT_BROKE.md): skipping a jar does
# NOT skip its own dependencies. Those must be excluded explicitly, and
# streaming/delta_compat_check.py (C2) fails if an added jar duplicates a class.
#
# The add/skip decisions are printed and also written to
# /opt/valkyrie/jars-<list-name>.txt. Each add and skip line carries the exact
# Maven coordinate (group:artifact:version) from Ivy's cache; jars_lock.py uses
# the add lines, delta_compat_check C2 checks both.
#
# Integrity: Ivy does NOT verify .sha1/.md5 checksums here (tested in slice 1,
# docs/DECISIONS.md D2.1). Every added jar is instead checked against the
# committed SHA-256 lock (docker/spark/jars.sha256) after all fetches.

set -euo pipefail

COORD="${1:?maven coordinate required}"
LIST_NAME="${2:?list name required (e.g. kafka, delta)}"
EXCLUDES="${3:-}"
LIST_DIR="/opt/valkyrie"
LIST_FILE="${LIST_DIR}/jars-${LIST_NAME}.txt"
SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
IVY_DIR="$(mktemp -d)"
NOOP="$(mktemp --suffix=.py)"
echo 'pass' > "$NOOP"

EXCLUDE_ARGS=()
if [[ -n "$EXCLUDES" ]]; then
  EXCLUDE_ARGS=(--exclude-packages "$EXCLUDES")
fi

mkdir -p "$LIST_DIR"
{
  echo "# coordinate: ${COORD}"
  echo "# excluded:   ${EXCLUDES:-none}"
} > "$LIST_FILE"

echo "resolving ${COORD} into ${IVY_DIR} (excluded: ${EXCLUDES:-none})"
"$SPARK_HOME/bin/spark-submit" --master 'local[1]' \
  --conf "spark.jars.ivy=${IVY_DIR}" --packages "$COORD" "${EXCLUDE_ARGS[@]}" \
  "$NOOP" > /dev/null

echo "resolved jars:"
for jar in "$IVY_DIR"/jars/*.jar; do
  name="$(basename "$jar")"            # ivy naming: <groupId>_<artifactId>-<version>.jar
  group="${name%%_*}"
  file="${name#*_}"                    # <artifactId>-<version>.jar
  # Exact coordinate from Ivy's cache layout: cache/<group>/<module>/<type>/<file>,
  # where <type> is "jars", or "bundles" for OSGi-packaged jars (e.g. Jackson).
  mapfile -t cached < <(compgen -G "$IVY_DIR/cache/${group}/*/jars/${file}" || true;
                        compgen -G "$IVY_DIR/cache/${group}/*/bundles/${file}" || true)
  if [[ ${#cached[@]} -ne 1 ]]; then
    echo "ERROR: expected 1 cache entry for ${name}, found ${#cached[@]}" >&2
    exit 1
  fi
  module="$(basename "$(dirname "$(dirname "${cached[0]}")")")"
  version="${file#"${module}"-}"
  coord="${group}:${module}:${version%.jar}"
  # Skip rule works on FILE NAMES (artifact = name before "-<digit>"). Known limit:
  # names like hive-shims-0.23-2.3.10.jar parse wrongly; delta_compat_check C2
  # compares every decision below with the real coordinate.
  artifact="$(echo "$file" | sed -E 's/-[0-9][^-]*(-[^-]+)?\.jar$//')"
  if compgen -G "$SPARK_HOME/jars/${artifact}-[0-9]*.jar" > /dev/null; then
    shipped="$(basename "$(ls "$SPARK_HOME"/jars/"${artifact}"-[0-9]*.jar | head -1)")"
    line="  skip  $file ${coord} (Spark already ships ${shipped})"
  else
    cp "$jar" "$SPARK_HOME/jars/$file"
    line="  add   $file ${coord}"
  fi
  echo "$line"
  echo "$line" >> "$LIST_FILE"
done

rm -rf "$IVY_DIR" "$NOOP"
