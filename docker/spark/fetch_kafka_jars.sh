#!/bin/bash
# Resolve the Spark Kafka connector (and its dependencies) at image BUILD time
# and add them to Spark's classpath, so the container needs no internet at runtime.
#
# Usage: fetch_kafka_jars.sh <maven-coordinate>
#   e.g. fetch_kafka_jars.sh org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4
#
# Jars whose artifact already ships with Spark (e.g. lz4-java, snappy-java) are
# skipped, so we never have two versions of one library on the classpath.

set -euo pipefail

COORD="${1:?maven coordinate required}"
SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
IVY_DIR="$(mktemp -d)"
NOOP="$(mktemp --suffix=.py)"
echo 'pass' > "$NOOP"

echo "resolving ${COORD} into ${IVY_DIR}"
"$SPARK_HOME/bin/spark-submit" --master 'local[1]' \
  --conf "spark.jars.ivy=${IVY_DIR}" --packages "$COORD" "$NOOP" > /dev/null

echo "resolved jars:"
for jar in "$IVY_DIR"/jars/*.jar; do
  name="$(basename "$jar")"            # ivy naming: <groupId>_<artifactId>-<version>.jar
  file="${name#*_}"                    # <artifactId>-<version>.jar
  artifact="$(echo "$file" | sed -E 's/-[0-9][^-]*(-[^-]+)?\.jar$//')"
  if compgen -G "$SPARK_HOME/jars/${artifact}-[0-9]*.jar" > /dev/null; then
    echo "  skip  $file (Spark already ships $(basename "$(ls "$SPARK_HOME"/jars/"${artifact}"-[0-9]*.jar | head -1)"))"
  else
    cp "$jar" "$SPARK_HOME/jars/$file"
    echo "  add   $file"
  fi
done

rm -rf "$IVY_DIR" "$NOOP"
