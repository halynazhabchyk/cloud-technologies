"""Follower side of replication: consume the shard's Kafka topic and apply it."""

import logging
import threading

import httpx
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from app.replication.kafka_log import PARTITION, to_entry, topic_name
from app.replication.log import LogEntry
from app.replication.node import ReplicaNode

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
RETRY_SECONDS = 1.0


class KafkaFollowerReplicator:

    def __init__(self, node: ReplicaNode, consumer, ack_client: httpx.Client | None):
        self.node = node
        self.consumer = consumer
        self.ack_client = ack_client
        self.partition = TopicPartition(topic_name(node.settings), PARTITION)
        self._acked = 0
        # Next Kafka offset to read = last applied log offset (1-based) .
        self.consumer.assign(
            [TopicPartition(self.partition.topic, PARTITION, node.applied_offset)]
        )

    def sync_once(self, timeout: float = 1.0) -> int:
        first = self.consumer.poll(timeout)
        messages = [] if first is None else [first]
        if first is not None:
            messages += self.consumer.consume(num_messages=BATCH_SIZE - 1, timeout=0)

        entries: list[LogEntry] = []
        for message in messages:
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            entries.append(to_entry(message))

        _, high = self.consumer.get_watermark_offsets(self.partition, cached=True)
        applied = self.node.apply_replicated(entries, max(high, 0))
        self._acknowledge()
        return applied

    def _acknowledge(self) -> None:
        offset = self.node.applied_offset
        if self.ack_client is None or offset <= self._acked:
            return
        response = self.ack_client.post(
            "/internal/ack",
            json={"replicaId": self.node.settings.replica_id, "appliedOffset": offset},
        )
        response.raise_for_status()
        self._acked = offset

    def run(self, stop: threading.Event) -> None:
        logger.info(
            "Follower %s consumes %s from log offset %s",
            self.node.settings.replica_id,
            self.partition.topic,
            self.node.applied_offset + 1,
        )
        while not stop.is_set():
            try:
                self.sync_once(self.node.settings.poll_wait_seconds)
            except (KafkaException, httpx.HTTPError, ValueError, KeyError) as error:
                logger.warning("Replication from Kafka failed: %s", error)
                stop.wait(RETRY_SECONDS)


def _kafka_replicator(node: ReplicaNode) -> tuple[KafkaFollowerReplicator, list]:
    settings = node.settings
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.replica_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "fetch.wait.max.ms": 50,
        }
    )
    ack_client = httpx.Client(base_url=settings.leader_url, timeout=5)
    return KafkaFollowerReplicator(node, consumer, ack_client), [consumer, ack_client]


def start_follower(node: ReplicaNode) -> tuple[threading.Thread, threading.Event]:
    """Start the background replication thread for a follower replica."""
    replicator, resources = _kafka_replicator(node)
    stop = threading.Event()

    def run() -> None:
        try:
            replicator.run(stop)
        finally:
            for resource in resources:
                resource.close()

    thread = threading.Thread(target=run, name="follower-replicator", daemon=True)
    thread.start()
    return thread, stop
