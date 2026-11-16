import json
import logging
import time

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic

from app.core.config import ReplicaSettings
from app.replication.log import LogEntry

logger = logging.getLogger(__name__)

PARTITION = 0


class LogWriteError(RuntimeError):
    """Kafka did not confirm a produced message."""


def topic_name(settings: ReplicaSettings) -> str:
    return f"{settings.kafka_topic_prefix}{settings.shard_id}"


def to_entry(message: Message) -> LogEntry:
    offset, value = message.offset(), message.value()
    if offset is None or value is None:
        raise ValueError("A Kafka log message has no offset or no value")
    return {"offset": offset + 1, **json.loads(value)}


def ensure_topic(bootstrap: str, topic: str, timeout: float = 60) -> None:
    """Create the shard topic (1 partition, kept forever) if it is missing."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout
    while True:
        try:
            if topic in admin.list_topics(timeout=5).topics:
                return
            new_topic = NewTopic(
                topic,
                num_partitions=1,
                replication_factor=1,
                # The log is the source of truth for recovery: never delete it.
                config={"retention.ms": "-1", "retention.bytes": "-1"},
            )
            admin.create_topics([new_topic])[topic].result(timeout=10)
            logger.info("Created Kafka topic %s", topic)
            return
        except KafkaException as error:
            if error.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                return
            if time.monotonic() > deadline:
                raise
            logger.warning("Kafka is not ready yet: %s", error)
            time.sleep(2)


class KafkaLeaderLog:
    """Leader side: produce every write to the shard topic and read it back."""

    def __init__(self, bootstrap: str, topic: str):
        self.bootstrap = bootstrap
        self.topic = topic
        ensure_topic(bootstrap, topic)
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 0,
            }
        )

    def append(self, operation: LogEntry) -> LogEntry:
        """Produce one operation and wait until Kafka stores it."""
        result: dict = {}

        def delivered(error, message) -> None:
            result["error"] = error
            result["message"] = message

        self._producer.produce(
            self.topic,
            value=json.dumps(operation).encode("utf-8"),
            key=str(operation.get("partitionKey", "")).encode("utf-8"),
            partition=PARTITION,
            on_delivery=delivered,
        )
        self._producer.flush(10)
        if result.get("error") is not None or "message" not in result:
            raise LogWriteError(f"Kafka did not store the write: {result.get('error')}")
        return to_entry(result["message"])

    def read_all(self, timeout: float = 30) -> list[LogEntry]:
        """Read the whole topic from the beginning (leader recovery)."""
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"{self.topic}-leader-recovery",
                "enable.auto.commit": False,
            }
        )
        try:
            partition = TopicPartition(self.topic, PARTITION)
            deadline = time.monotonic() + timeout
            while True:
                try:
                    # A freshly created topic needs a moment to show up in metadata.
                    _, high = consumer.get_watermark_offsets(partition, timeout=5)
                    break
                except KafkaException:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(1)
            consumer.assign([TopicPartition(self.topic, PARTITION, 0)])
            entries: list[LogEntry] = []
            while len(entries) < high:
                if time.monotonic() > deadline:
                    raise LogWriteError("Timed out reading the Kafka log")
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                entries.append(to_entry(message))
            return entries
        finally:
            consumer.close()
