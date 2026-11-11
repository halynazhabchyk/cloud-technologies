"""One replica of a shard: its record store plus the replication log."""

import logging
import threading
from typing import Any

from app.core.config import ReplicaSettings
from app.replication.kafka_log import KafkaLeaderLog, topic_name
from app.replication.log import (
    LeaderLog,
    LogEntry,
    LogGapError,
    OffsetStore,
    ReplicationLog,
)
from app.storage import InMemoryShardStorage, RecordKey, RecordValue

logger = logging.getLogger(__name__)

PUT = "put"
DELETE = "delete"


class NotLeaderError(RuntimeError):
    """A write reached a follower; only the leader accepts writes."""


def _kafka_leader_log(settings: ReplicaSettings) -> KafkaLeaderLog:
    if not settings.kafka_bootstrap:
        raise ValueError("The leader needs KAFKA_BOOTSTRAP_SERVERS")
    return KafkaLeaderLog(settings.kafka_bootstrap, topic_name(settings))


class ReplicaNode:
    """Applies log entries to the in-memory store in strict offset order."""

    def __init__(self, settings: ReplicaSettings, leader_log: LeaderLog | None = None):
        """``leader_log`` replaces Kafka on the leader (tests pass a fake)."""
        self.settings = settings
        self.storage = InMemoryShardStorage()
        self.applied_offset = 0
        self.leader_offset = 0  # follower: last offset the leader reported
        self.lock = threading.RLock()

        data_dir = settings.data_dir
        self.offsets = OffsetStore(data_dir / "offset" if data_dir else None)
        self.leader_log: LeaderLog | None = None
        if settings.is_leader:
            self.leader_log = leader_log or _kafka_leader_log(settings)
            self.log = ReplicationLog()  # in-memory mirror of the Kafka topic
        else:
            self.log = ReplicationLog(
                data_dir / "log.jsonl" if data_dir else None,
                keep_up_to=self.offsets.load(),
            )
        self._recover()

    @property
    def is_leader(self) -> bool:
        return self.settings.is_leader

    def _recover(self) -> None:
        """Rebuild the in-memory store by replaying the log."""
        if self.leader_log is not None:
            for entry in self.leader_log.read_all():
                self.log.append_replicated(entry)
        for entry in self.log.entries():
            self._apply(entry)
        if not self.is_leader:
            self.offsets.save(self.applied_offset)
        if self.applied_offset:
            logger.info(
                "Replica %s recovered up to offset %s",
                self.settings.replica_id,
                self.applied_offset,
            )

    def _apply(self, entry: LogEntry) -> None:
        offset = entry["offset"]
        if offset <= self.applied_offset:
            return  # already applied: re-delivery is idempotent
        if offset != self.applied_offset + 1:
            raise LogGapError(
                f"Expected offset {self.applied_offset + 1}, got {offset}"
            )

        key = RecordKey(entry["partitionKey"], entry.get("sortKey"))
        if entry["op"] == PUT:
            self.storage.put(entry["table"], key, entry["value"], version=offset)
        elif entry["op"] == DELETE:
            self.storage.delete(entry["table"], key)
        else:
            raise ValueError(f"Unknown log operation {entry['op']!r}")
        self.applied_offset = offset

    # --- leader -----------------------------------------------------------

    def _write(self, operation: dict[str, Any]) -> LogEntry:
        if self.leader_log is None:
            raise NotLeaderError("This replica is a read-only follower")
        # Kafka assigns the offset; the write is durable once produced.
        entry = self.leader_log.append(operation)
        self.log.append_replicated(entry)
        self._apply(entry)
        return entry

    def put(self, table: str, key: RecordKey, value: RecordValue) -> tuple[bool, int]:
        with self.lock:
            created = not self.storage.exists(table, key)
            entry = self._write(
                {
                    "op": PUT,
                    "table": table,
                    "partitionKey": key.partition_key,
                    "sortKey": key.sort_key,
                    "value": value,
                }
            )
            return created, entry["offset"]

    def delete(self, table: str, key: RecordKey) -> int | None:
        """Delete a record; return the log offset, or None when it is missing."""
        with self.lock:
            if not self.is_leader:
                raise NotLeaderError("This replica is a read-only follower")
            if not self.storage.exists(table, key):
                return None
            entry = self._write(
                {
                    "op": DELETE,
                    "table": table,
                    "partitionKey": key.partition_key,
                    "sortKey": key.sort_key,
                }
            )
            return entry["offset"]

    def wait_for_write_quorum(self, offset: int, acks: int) -> bool:
        """Wait until ``acks`` replicas (the leader included) hold ``offset``."""
        return self.log.wait_for_acks(
            offset,
            followers=acks - 1,
            timeout=self.settings.ack_timeout_seconds,
        )

    # --- follower ---------------------------------------------------------

    def apply_replicated(self, entries: list[LogEntry], leader_offset: int) -> int:
        """Store and apply entries fetched from the leader; return how many."""
        applied = 0
        with self.lock:
            for entry in entries:
                if entry["offset"] <= self.applied_offset:
                    continue
                self.log.append_replicated(entry)
                self._apply(entry)
                applied += 1
            if applied:
                self.offsets.save(self.applied_offset)
            self.leader_offset = max(leader_offset, self.applied_offset)
        return applied

    def status(self) -> dict[str, Any]:
        with self.lock:
            status: dict[str, Any] = {
                "shardId": self.settings.shard_id,
                "replicaId": self.settings.replica_id,
                "role": self.settings.role,
                "appliedOffset": self.applied_offset,
            }
            if self.is_leader:
                status["followers"] = {
                    replica_id: {
                        "ackedOffset": acked,
                        "lag": self.applied_offset - acked,
                    }
                    for replica_id, acked in self.log.follower_progress().items()
                }
            else:
                status["leaderOffset"] = self.leader_offset
                status["lag"] = self.leader_offset - self.applied_offset
            return status
