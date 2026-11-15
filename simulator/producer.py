"""Kafka producer wrapper with delivery tracking.

Key settings and why:
    acks=all              The broker confirms only after the write is on all in-sync
                          replicas (with RF=1 that is just the leader).
    enable.idempotence    The broker discards duplicates caused by the producer's own
                          internal retries (per partition, per producer session). It
                          does NOT stop duplicates from re-running the simulator.
    partitioner=murmur2_random
                          Same key->partition hashing as the Java client. librdkafka's
                          default (consistent_random, CRC32) would put the same key on a
                          different partition than a Java producer would.

Sending is asynchronous: `produce()` only queues the message. The broker's
answer arrives later via the delivery callback, which runs inside
`poll()`/`flush()`.
"""

from __future__ import annotations

import functools
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from simulator.events import GameEvent

log = logging.getLogger(__name__)


@dataclass
class PartitionOffsets:
    """Offset range of this run's acknowledged messages in one partition."""

    min: int
    max: int
    count: int = 1

    def add(self, offset: int) -> None:
        self.min = min(self.min, offset)
        self.max = max(self.max, offset)
        self.count += 1


@dataclass
class DeliveryTracker:
    """Records the broker's verdict for every message (fed by delivery callbacks)."""

    delivered_ids: list[str] = field(default_factory=list)
    failed_ids: list[str] = field(default_factory=list)
    partitions_by_match: dict[str, set[int]] = field(default_factory=lambda: defaultdict(set))
    # partition -> offset range of acknowledged messages. Lets tests know exactly
    # which offsets a consumer (Spark) must have processed to have seen this run.
    offsets_by_partition: dict[int, PartitionOffsets] = field(default_factory=dict)

    def _record_offset(self, partition: int, offset: int) -> None:
        current = self.offsets_by_partition.get(partition)
        if current is None:
            self.offsets_by_partition[partition] = PartitionOffsets(offset, offset)
        else:
            current.add(offset)

    def on_delivery(
        self, event_id: str, match_id: str, err: KafkaError | None, msg: Message | None
    ) -> None:
        """Delivery callback. Called once per message by librdkafka."""
        if err is not None:
            self.failed_ids.append(event_id)
            log.error(
                "delivery failed",
                extra={"fields": {
                    "event_id": event_id, "match_id": match_id,
                    "error": str(err), "retriable": err.retriable(),
                }},
            )
            return
        assert msg is not None
        self.delivered_ids.append(event_id)
        self.partitions_by_match[match_id].add(msg.partition())
        self._record_offset(msg.partition(), msg.offset())
        log.debug(
            "delivered",
            extra={"fields": {
                "event_id": event_id, "match_id": match_id,
                "partition": msg.partition(), "offset": msg.offset(),
            }},
        )


class EventProducer:
    """Sends GameEvents to one Kafka topic, keyed by match_id."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str,
        extra_config: dict[str, Any] | None = None,
    ) -> None:
        conf: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "partitioner": "murmur2_random",
            "linger.ms": 20,               # wait up to 20 ms to batch messages
            "compression.type": "lz4",
            "delivery.timeout.ms": 120_000,  # give up on a message after 2 min
            "logger": logging.getLogger("librdkafka"),
        }
        conf.update(extra_config or {})
        self.topic = topic
        self.tracker = DeliveryTracker()
        self._producer = Producer(conf)

    def check_topic(self, timeout_s: float = 10.0) -> int:
        """Fail fast if the broker is unreachable or the topic is missing.

        Returns:
            The topic's partition count.

        Raises:
            RuntimeError: Broker unreachable or topic missing.
        """
        try:
            metadata = self._producer.list_topics(topic=self.topic, timeout=timeout_s)
        except KafkaException as exc:
            raise RuntimeError(f"cannot reach Kafka: {exc}") from exc
        topic_md = metadata.topics.get(self.topic)
        if topic_md is None or topic_md.error is not None:
            reason = topic_md.error if topic_md is not None else "not found"
            raise RuntimeError(f"topic {self.topic!r} unavailable: {reason}")
        return len(topic_md.partitions)

    def send(self, event: GameEvent) -> None:
        """Queue one event. Blocks briefly (serving callbacks) if the local queue is full."""
        callback = functools.partial(self.tracker.on_delivery, event.event_id, event.match_id)
        while True:
            try:
                self._producer.produce(
                    self.topic, key=event.key, value=event.to_json_bytes(), on_delivery=callback
                )
                break
            except BufferError:
                # Local queue full: let librdkafka send some messages, then retry.
                self._producer.poll(0.5)
        self._producer.poll(0)  # serve delivery callbacks that are ready

    def poll(self, timeout_s: float) -> None:
        """Wait up to `timeout_s` while serving delivery callbacks.

        Used by realtime pacing instead of `time.sleep`, so broker answers keep
        being processed while the simulator waits for the next event's send time.
        It may return early (when a callback was served); callers re-check the clock.
        """
        self._producer.poll(max(0.0, timeout_s))

    def flush(self, timeout_s: float) -> int:
        """Wait for outstanding messages. Returns how many are still undelivered."""
        return self._producer.flush(timeout_s)
