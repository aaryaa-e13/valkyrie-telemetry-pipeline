#!/bin/sh
# Create VALKYRIE Kafka topics idempotently and verify their configuration.
#
# Runs inside the `kafka-init` container (apache/kafka image).
# Settings come from environment variables set in docker-compose.yml.
# Exits non-zero if the topic exists but with the wrong partition count,
# because silently continuing would break ordering/parallelism assumptions.

set -eu

BIN=/opt/kafka/bin
: "${BOOTSTRAP_SERVERS:?BOOTSTRAP_SERVERS is required}"
: "${TOPIC_NAME:?TOPIC_NAME is required}"
: "${TOPIC_PARTITIONS:?TOPIC_PARTITIONS is required}"
: "${TOPIC_REPLICATION_FACTOR:?TOPIC_REPLICATION_FACTOR is required}"
: "${TOPIC_RETENTION_MS:?TOPIC_RETENTION_MS is required}"

log() {
  # Minimal structured log line: one JSON object per line.
  printf '{"ts":"%s","level":"%s","component":"kafka-init","msg":"%s"}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2"
}

log INFO "creating topic ${TOPIC_NAME} (partitions=${TOPIC_PARTITIONS}, rf=${TOPIC_REPLICATION_FACTOR}, retention.ms=${TOPIC_RETENTION_MS})"

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create --if-not-exists \
  --topic "$TOPIC_NAME" \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor "$TOPIC_REPLICATION_FACTOR" \
  --config retention.ms="$TOPIC_RETENTION_MS" \
  --config min.insync.replicas=1

# Verify: --if-not-exists does NOT fix an existing topic with different settings.
DESCRIBE="$("$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" --describe --topic "$TOPIC_NAME")"
echo "$DESCRIBE"

ACTUAL_PARTITIONS="$(echo "$DESCRIBE" | sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p' | head -n 1)"
if [ "$ACTUAL_PARTITIONS" != "$TOPIC_PARTITIONS" ]; then
  log ERROR "topic ${TOPIC_NAME} has ${ACTUAL_PARTITIONS:-unknown} partitions, expected ${TOPIC_PARTITIONS}"
  exit 1
fi

log INFO "topic ${TOPIC_NAME} ready with ${ACTUAL_PARTITIONS} partitions"
