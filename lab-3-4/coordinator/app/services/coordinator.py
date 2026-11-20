"""Coordinator: the single entry point for clients.

It validates requests against the table definitions and hands records to a
record backend: local storage (single node) or the shards (cluster).
"""

import logging
from typing import Any

from common.clock import now_micros
from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.services.errors import (
    InvalidRequestError,
    ShardUnavailableError,
)
from coordinator.app.services.local_records import LocalRecords
from coordinator.app.services.rebalancer import Rebalancer
from coordinator.app.services.record_backend import RecordBackend
from coordinator.app.services.shard_client import ShardClient
from coordinator.app.services.sharded_records import ShardedRecords
from coordinator.app.sharding.shard_registry import LEADER, MIN_SHARDS, ShardRegistry
from coordinator.app.storage.storage import RecordKey, RecordValue, Storage

__all__ = ["Coordinator", "InvalidRequestError", "ShardUnavailableError"]

logger = logging.getLogger(__name__)


def write_timestamp(client_timestamp: int | None) -> int:
    # DEFENSE: мітку LWW ставить координатор; X-Write-Timestamp імітує інший годинник.
    return client_timestamp or now_micros()


class Coordinator:
    def __init__(
        self,
        storage: Storage,
        shard_registry: ShardRegistry | None = None,
        settings: CoordinatorSettings | None = None,
    ):
        self.storage = storage
        self.settings = settings or CoordinatorSettings()
        self.shard_registry = shard_registry or ShardRegistry()
        self.shards = ShardClient(self.shard_registry, self.settings)
        self.rebalancer = Rebalancer(self.shard_registry, self.shards, self.settings)
        self.records: RecordBackend = (
            ShardedRecords(self.shards, self.rebalancer)
            if self.settings.sharding_enabled
            else LocalRecords(storage)
        )

    @property
    def sharding_enabled(self) -> bool:
        return self.settings.sharding_enabled

    def replication_summary(self) -> dict[str, Any]:
        settings = self.settings
        return {
            "replicationFactor": settings.replication_factor,
            "writeQuorum": settings.write_quorum,
            "readQuorum": settings.read_quorum,
            # R + W > N: every read quorum shares a replica with the write quorum.
            "strongConsistency": settings.read_quorum + settings.write_quorum
            > settings.replication_factor,
        }

    # --- shards -------------------------------------------------------------

    def register_replica(
        self, shard_id: str, base_url: str, replica_id: str | None, role: str
    ) -> bool:
        """Register a replica; a new leader on a working ring starts rebalancing."""
        shards_before = self.shard_registry.shard_count()
        created = self.shard_registry.register(shard_id, base_url, replica_id, role)
        new_shard_joined = created and role == LEADER
        ring_was_working = shards_before >= MIN_SHARDS
        if new_shard_joined and ring_was_working and self.settings.rebalance_enabled:
            logger.info("Shard %s joined the ring, scheduling rebalancing", shard_id)
            self.rebalancer.schedule(shard_id)
        return created

    # --- tables -------------------------------------------------------------

    def create_table(
        self,
        table_name: str,
        partition_key: str,
        sort_key: str | None = None,
    ) -> dict[str, Any]:
        _require_not_blank(table_name, "Table name")
        _require_not_blank(partition_key, "Partition key")
        if sort_key is not None:
            _require_not_blank(sort_key, "Sort key")
        return self.storage.create_table(table_name, partition_key, sort_key)

    def list_tables(self) -> list[dict[str, Any]]:
        return self.storage.list_tables()

    def get_table(self, table_name: str) -> dict[str, Any]:
        return self.storage.get_table(table_name)

    # --- records ------------------------------------------------------------

    def create_record(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue,
        client_timestamp: int | None = None,
    ) -> dict[str, Any]:
        """Write a record; return created, shardId, version and timestamp."""
        self._validate_record_key(table_name, key)
        return self.records.put(
            table_name, key, value, write_timestamp(client_timestamp)
        )

    def delete_record(
        self, table_name: str, key: RecordKey, client_timestamp: int | None = None
    ) -> bool:
        """Delete a record; return False when it does not exist."""
        self._validate_record_key(table_name, key)
        return self.records.delete(table_name, key, write_timestamp(client_timestamp))

    def read_record(self, table_name: str, key: RecordKey) -> dict[str, Any] | None:
        """Read one record by its full key; return None when it is missing."""
        self._validate_record_key(table_name, key)
        return self.records.read(table_name, key)

    def read_records_by_partition(
        self, table_name: str, partition_key: str
    ) -> dict[str, Any]:
        """Read every record of one partition."""
        self.storage.get_table(table_name)
        _require_not_blank(partition_key, "Partition key value")
        return self.records.read_partition(table_name, partition_key)

    def record_exists(self, table_name: str, key: RecordKey) -> bool:
        self._validate_record_key(table_name, key)
        return self.records.exists(table_name, key)

    def _validate_record_key(self, table_name: str, key: RecordKey) -> None:
        """The key must match the table: a sort key exactly when the table has one."""
        table_has_sort_key = (
            self.storage.get_table(table_name).get("sortKey") is not None
        )

        _require_not_blank(key.partition_key, "Partition key value")
        if table_has_sort_key and key.sort_key is None:
            raise InvalidRequestError("Sort key value is required")
        if not table_has_sort_key and key.sort_key is not None:
            raise InvalidRequestError("This table does not use a sort key")
        if key.sort_key is not None:
            _require_not_blank(key.sort_key, "Sort key value")


def _require_not_blank(text: str, name: str) -> None:
    if not text.strip():
        raise InvalidRequestError(f"{name} cannot be empty")
