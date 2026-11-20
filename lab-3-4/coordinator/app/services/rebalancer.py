import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from common.trace import new_trace_id, use_trace_id
from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.services.errors import ShardUnavailableError
from coordinator.app.services.metrics import REBALANCE_KEYS_MOVED
from coordinator.app.services.shard_client import ShardClient
from coordinator.app.services.shard_responses import error_detail
from coordinator.app.sharding.consistent_hashing import ConsistentHashRing
from coordinator.app.sharding.shard_registry import ShardRegistry
from coordinator.app.storage.storage import RecordKey

logger = logging.getLogger(__name__)

IDLE, RUNNING, DONE, FAILED = "idle", "running", "done", "failed"
COPY_OK = (200, 201, 409)
DELETE_OK = (200, 404, 409)
MAX_REPORTED_ERRORS = 10


@dataclass
class RebalanceStatus:
    state: str = IDLE
    new_shard: str | None = None
    trace_id: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    scanned: int = 0
    moved: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "newShard": self.new_shard,
            "traceId": self.trace_id,
            "startedAt": self.started_at,
            "finishedAt": self.finished_at,
            "scanned": self.scanned,
            "moved": self.moved,
            "errors": self.errors,
        }


class Rebalancer:
    def __init__(
        self,
        registry: ShardRegistry,
        shards: ShardClient,
        settings: CoordinatorSettings,
    ):
        self.registry = registry
        self.shards = shards
        self.settings = settings
        self.status = RebalanceStatus()
        self._previous_ring: ConsistentHashRing | None = None
        self._lock = threading.Lock()

    def schedule(self, new_shard: str) -> None:
        """Rebalance in a background thread after a short delay."""

        def run_later() -> None:
            time.sleep(self.settings.rebalance_delay_seconds)
            self.run(new_shard)

        threading.Thread(target=run_later, name="rebalancer", daemon=True).start()

    def run(self, new_shard: str) -> RebalanceStatus:
        """Move every record whose owner differs from the shard it is on."""
        with self._lock:  # one rebalancing at a time
            self.status = RebalanceStatus(
                RUNNING, new_shard, new_trace_id(), started_at=time.time()
            )
            self._previous_ring = self.registry.ring_without(new_shard)
            with use_trace_id(self.status.trace_id):
                logger.info("Rebalancing started: %s joined the ring", new_shard)
                # Only the old shards hold records; the new one starts empty.
                for shard_id in self.registry.routable_shard_ids():
                    if shard_id != new_shard:
                        self._rebalance_shard(shard_id)
                self._finish()
            return self.status

    def _rebalance_shard(self, shard_id: str) -> None:
        try:
            records = self.shards.list_all_records(shard_id)
        except ShardUnavailableError as error:
            self._error(f"{shard_id}: cannot list records: {error}")
            return
        moved_before = self.status.moved
        for record in records:
            self.status.scanned += 1
            owner = self.registry.owner(record["partitionKey"])
            if owner != shard_id:
                self._move(record, shard_id, owner)
        logger.info(
            "Moved %s of %s records from %s",
            self.status.moved - moved_before,
            len(records),
            shard_id,
            extra={"shard": shard_id},
        )

    def _move(self, record: dict[str, Any], source: str, target: str) -> None:
        # DEFENSE: копія має оригінальний timestamp – новіший запис не затре.
        table = record["tableName"]
        key = RecordKey(record["partitionKey"], record.get("sortKey"))
        timestamp = record.get("timestamp")
        try:
            copied = self.shards.put_record(
                table, key, record["value"], timestamp=timestamp, shard_id=target
            ).response
            if copied.status_code not in COPY_OK:
                raise ShardUnavailableError(error_detail(copied))
            deleted = self.shards.delete_record(
                table, key, timestamp=timestamp, shard_id=source
            ).response
            if deleted.status_code not in DELETE_OK:
                raise ShardUnavailableError(error_detail(deleted))
        except ShardUnavailableError as error:
            self._error(f"{table}/{key.partition_key}: {error}")
            return
        self.status.moved += 1
        REBALANCE_KEYS_MOVED.inc()

    def _error(self, message: str) -> None:
        logger.warning("Rebalancing error: %s", message)
        if len(self.status.errors) < MAX_REPORTED_ERRORS:
            self.status.errors.append(message)

    def _finish(self) -> None:
        self.status.state = FAILED if self.status.errors else DONE
        self.status.finished_at = time.time()
        self._previous_ring = None
        logger.info(
            "Rebalancing %s: scanned %s, moved %s",
            self.status.state,
            self.status.scanned,
            self.status.moved,
        )

    def fallback_shard(self, partition_key: str) -> str | None:
        previous_ring = self._previous_ring
        if previous_ring is None or previous_ring.node_count == 0:
            return None
        previous_owner = previous_ring.get_node(partition_key)
        if previous_owner == self.registry.owner(partition_key):
            return None
        return previous_owner
