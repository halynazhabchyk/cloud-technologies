import logging
import threading
import time
from dataclasses import dataclass
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
from app.storage import InMemoryShardStorage, RecordKey, RecordValue, Version
from app.storage.versioning import is_newer
from common.clock import now_micros
from common.trace import get_trace_id

logger = logging.getLogger(__name__)

PUT = "put"
DELETE = "delete"


class NotLeaderError(RuntimeError):
    """A write reached a follower; only the leader accepts writes."""


class WriteConflictError(RuntimeError):
    """Last-Write-Wins rejected a write: the stored version is newer."""

    def __init__(self, stored: Version):
        super().__init__(
            f"A newer write already exists (timestamp {stored.timestamp}, "
            f"version {stored.offset})"
        )
        self.stored = stored


@dataclass(frozen=True)
class WriteResult:
    created: bool
    version: Version


def _kafka_leader_log(settings: ReplicaSettings) -> KafkaLeaderLog:
    if not settings.kafka_bootstrap:
        raise ValueError("The leader needs KAFKA_BOOTSTRAP_SERVERS")
    return KafkaLeaderLog(settings.kafka_bootstrap, topic_name(settings))


def _entry_version(entry: LogEntry) -> Version:
    return Version(entry.get("timestamp", 0), entry["offset"])


def _log_operation(
    op: str,
    table: str,
    key: RecordKey,
    timestamp: int,
    value: RecordValue | None = None,
) -> dict[str, Any]:
    """A write as it goes to the replication log (Kafka adds the offset)."""
    operation: dict[str, Any] = {
        "op": op,
        "table": table,
        "partitionKey": key.partition_key,
        "sortKey": key.sort_key,
        "timestamp": timestamp,
    }
    if op == PUT:
        operation["value"] = value
    return operation


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
            self._apply(entry, log_entry=False)
        if not self.is_leader:
            self.offsets.save(self.applied_offset)
        if self.applied_offset:
            logger.info(
                "Replica %s recovered up to offset %s",
                self.settings.replica_id,
                self.applied_offset,
            )

    def _apply(self, entry: LogEntry, log_entry: bool = True) -> bool:
        """Apply one entry in offset order; return whether LWW kept it."""
        offset = entry["offset"]
        if offset <= self.applied_offset:
            return False  # already applied: re-delivery is idempotent
        if offset != self.applied_offset + 1:
            raise LogGapError(
                f"Expected offset {self.applied_offset + 1}, got {offset}"
            )
        if entry["op"] not in (PUT, DELETE):
            raise ValueError(f"Unknown log operation {entry['op']!r}")

        key = RecordKey(entry["partitionKey"], entry.get("sortKey"))
        value = entry["value"] if entry["op"] == PUT else None  # None = tombstone
        applied = self.storage.write(entry["table"], key, value, _entry_version(entry))
        self.applied_offset = offset
        if log_entry:
            self._log_applied(entry, applied)
        return applied

    def _log_applied(self, entry: LogEntry, applied: bool) -> None:
        logger.info(
            "Applied %s %s/%s",
            entry["op"],
            entry["table"],
            entry["partitionKey"],
            extra={
                "trace_id": entry.get("traceId"),
                "offset": entry["offset"],
                "op": entry["op"],
                "applied": applied,
            },
        )

    # --- leader -----------------------------------------------------------

    def _require_leader(self) -> None:
        if not self.is_leader:
            raise NotLeaderError("This replica is a read-only follower")

    def _check_last_write_wins(
        self, table: str, key: RecordKey, timestamp: int
    ) -> None:
        # DEFENSE: лідер відкидає старіший запис (409) ще до запису в Kafka.
        stored = self.storage.current(table, key)
        candidate = Version(timestamp, self.applied_offset + 1)
        if stored is not None and not is_newer(candidate, stored.version):
            raise WriteConflictError(stored.version)

    def _write(self, operation: dict[str, Any]) -> Version:
        if self.leader_log is None:
            raise NotLeaderError("This replica is a read-only follower")
        operation["traceId"] = get_trace_id()
        # Leader clock, read back by the same leader: replication lag in seconds.
        operation["producedAt"] = time.time()
        # Kafka assigns the offset; the write is durable once produced.
        entry = self.leader_log.append(operation)
        self.log.append_replicated(entry)
        self._apply(entry)
        return _entry_version(entry)

    def put(
        self,
        table: str,
        key: RecordKey,
        value: RecordValue,
        timestamp: int | None = None,
    ) -> WriteResult:
        """Write a record; ``timestamp`` comes from the coordinator (LWW)."""
        timestamp = timestamp or now_micros()
        with self.lock:
            self._require_leader()
            self._check_last_write_wins(table, key, timestamp)
            created = not self.storage.exists(table, key)
            version = self._write(_log_operation(PUT, table, key, timestamp, value))
            return WriteResult(created, version)

    def delete(
        self, table: str, key: RecordKey, timestamp: int | None = None
    ) -> WriteResult | None:
        """Delete a record (leave a tombstone); None when it is missing."""
        timestamp = timestamp or now_micros()
        with self.lock:
            self._require_leader()
            if not self.storage.exists(table, key):
                return None
            self._check_last_write_wins(table, key, timestamp)
            version = self._write(_log_operation(DELETE, table, key, timestamp))
            return WriteResult(created=False, version=version)

    def wait_for_write_quorum(self, offset: int, acks: int) -> bool:
        """Wait until ``acks`` replicas (the leader included) hold ``offset``."""
        return self.log.wait_for_acks(
            offset,
            followers=acks - 1,
            timeout=self.settings.ack_timeout_seconds,
        )

    def follower_lag(self, acked_offset: int) -> tuple[int, float]:
        # DEFENSE: лаг = вік найстарішого непідтвердженого запису за годинником лідера.
        lag_entries = self.applied_offset - acked_offset
        if lag_entries <= 0:
            return 0, 0.0
        oldest_unacked = self.log.entry(acked_offset + 1)
        produced_at = oldest_unacked.get("producedAt")
        if produced_at is None:
            return lag_entries, 0.0
        return lag_entries, max(0.0, time.time() - produced_at)

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
                "keys": self.storage.live_count(),
            }
            if self.is_leader:
                status["followers"] = self._followers_status()
            else:
                status["leaderOffset"] = self.leader_offset
                status["lag"] = self.leader_offset - self.applied_offset
            return status

    def _followers_status(self) -> dict[str, dict[str, Any]]:
        followers = {}
        for replica_id, acked in self.log.follower_progress().items():
            lag_entries, lag_seconds = self.follower_lag(acked)
            followers[replica_id] = {
                "ackedOffset": acked,
                "lag": lag_entries,
                "lagSeconds": round(lag_seconds, 3),
            }
        return followers
