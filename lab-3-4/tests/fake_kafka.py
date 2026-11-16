import json
import time

from app.replication.kafka_log import to_entry
from app.replication.log import LogEntry

EMPTY_POLL_SLEEP_SECONDS = 0.01


class FakeMessage:
    def __init__(self, kafka_offset: int, operation: dict):
        self._offset = kafka_offset
        self._value = json.dumps(operation).encode()

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value

    def error(self) -> None:
        return None


class FakeTopic:
    """Ordered message list; Kafka offsets start at 0 like in a real topic."""

    def __init__(self, operations: list[dict] | None = None):
        self.messages: list[FakeMessage] = []
        for operation in operations or []:
            self.append(operation)

    def append(self, operation: LogEntry) -> LogEntry:
        message = FakeMessage(len(self.messages), operation)
        self.messages.append(message)
        return to_entry(message)

    def read_all(self) -> list[LogEntry]:
        return [to_entry(message) for message in self.messages]

    def consumer(self) -> "FakeConsumer":
        return FakeConsumer(self)


class FakeConsumer:
    """Reads a ``FakeTopic`` from the position set by ``assign()``."""

    def __init__(self, topic: FakeTopic):
        self.topic = topic
        self.position = 0

    def assign(self, partitions) -> None:
        self.position = partitions[0].offset

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        batch = self.topic.messages[self.position : self.position + num_messages]
        self.position += len(batch)
        return batch

    def poll(self, timeout: float) -> FakeMessage | None:
        batch = self.consume(1, timeout)
        if not batch:
            # A real poll blocks until the timeout; do not spin the CPU.
            time.sleep(min(timeout, EMPTY_POLL_SLEEP_SECONDS))
            return None
        return batch[0]

    def get_watermark_offsets(
        self, _partition, cached: bool = False
    ) -> tuple[int, int]:
        return 0, len(self.topic.messages)
