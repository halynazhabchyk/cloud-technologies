import logging
from typing import Any

from common.clock import now_micros
from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.services.errors import InvalidRequestError, WriteConflictError
from coordinator.app.services.rebalancer import Rebalancer
from coordinator.app.services.shard_client import (
    ShardClient,
    ShardUnavailableError,
    error_detail,
    read_json_object,
    read_records,
)
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

    # --- shards -----------------------------------------------------------

    def register_replica(
        self, shard_id: str, base_url: str, replica_id: str | None, role: str
    ) -> bool:
        """Register a replica; a new leader on a working ring starts rebalancing."""
        routable_before = self.shard_registry.shard_count()
        created = self.shard_registry.register(shard_id, base_url, replica_id, role)
        joined_ring = created and role == LEADER
        if (
            joined_ring
            and routable_before >= MIN_SHARDS
            and self.settings.rebalance_enabled
        ):
            logger.info("Shard %s joined the ring, scheduling rebalancing", shard_id)
            self.rebalancer.schedule(shard_id)
        return created

    # --- tables -----------------------------------------------------------

    def create_table(
        self,
        table_name: str,
        partition_key: str,
        sort_key: str | None = None,
    ) -> dict[str, Any]:
        if not table_name.strip():
            raise InvalidRequestError("Table name cannot be empty")
        if not partition_key.strip():
            raise InvalidRequestError("Partition key cannot be empty")
        if sort_key is not None and not sort_key.strip():
            raise InvalidRequestError("Sort key cannot be empty")

        return self.storage.create_table(
            table_name=table_name,
            partition_key=partition_key,
            sort_key=sort_key,
        )

    def list_tables(self) -> list[dict[str, Any]]:
        return self.storage.list_tables()

    def get_table(self, table_name: str) -> dict[str, Any]:
        return self.storage.get_table(table_name)

    def _validate_record_key(self, table_name: str, key: RecordKey) -> None:
        table = self.storage.get_table(table_name)
        sort_key_name = table.get("sortKey")

        if not key.partition_key.strip():
            raise InvalidRequestError("Partition key value cannot be empty")
        if sort_key_name is not None and key.sort_key is None:
            raise InvalidRequestError("Sort key value is required")
        if sort_key_name is None and key.sort_key is not None:
            raise InvalidRequestError("This table does not use a sort key")
        if key.sort_key is not None and not key.sort_key.strip():
            raise InvalidRequestError("Sort key value cannot be empty")

    # --- records: writes --------------------------------------------------

    def create_record(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue,
        client_timestamp: int | None = None,
    ) -> dict[str, Any]:
        """Write a record; return created, shardId, version and timestamp."""
        self._validate_record_key(table_name, key)

        if not self.sharding_enabled:
            created = self.storage.put_record(
                table_name=table_name, key=key, value=value
            )
            return {"created": created, "shardId": None, "version": None}

        shard_response = self.shards.write(
            "PUT", table_name, key, value, timestamp=write_timestamp(client_timestamp)
        )
        response = shard_response.response
        if response.status_code == 409:
            raise WriteConflictError(error_detail(response))
        if response.status_code not in (200, 201):
            raise ShardUnavailableError()

        payload = read_json_object(response)
        if not isinstance(payload.get("created"), bool):
            raise ShardUnavailableError()
        return {
            "created": payload["created"],
            "shardId": shard_response.shard_id,
            "version": payload.get("version"),
            "timestamp": payload.get("timestamp"),
        }

    def delete_record(
        self, table_name: str, key: RecordKey, client_timestamp: int | None = None
    ) -> bool:
        self._validate_record_key(table_name, key)

        if not self.sharding_enabled:
            return self.storage.delete_record(table_name=table_name, key=key)

        response = self.shards.write(
            "DELETE", table_name, key, timestamp=write_timestamp(client_timestamp)
        ).response
        if response.status_code == 404:
            return False
        if response.status_code == 409:
            raise WriteConflictError(error_detail(response))
        if (
            response.status_code == 200
            and read_json_object(response).get("deleted") is True
        ):
            return True
        raise ShardUnavailableError()

    # --- records: reads ---------------------------------------------------

    def read_record(self, table_name: str, key: RecordKey) -> dict[str, Any] | None:
        """Read one record by its full key; return None when it is missing."""
        self._validate_record_key(table_name, key)

        if not self.sharding_enabled:
            value = self.storage.get_record(table_name=table_name, key=key)
            if value is None:
                return None
            return {
                "shardId": None,
                "replicaId": None,
                "sortKey": key.sort_key,
                "value": value,
                "version": None,
            }

        record = self._read_record_from(table_name, key, shard_id=None)
        fallback = self.rebalancer.fallback_shard(key.partition_key)
        if record is None and fallback is not None:
            record = self._read_record_from(table_name, key, shard_id=fallback)
        return record

    def _read_record_from(
        self, table_name: str, key: RecordKey, shard_id: str | None
    ) -> dict[str, Any] | None:
        shard_response = self.shards.read(
            "GET", table_name, key, resource="record", shard_id=shard_id
        )
        response = shard_response.response
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise ShardUnavailableError()

        payload = read_json_object(response)
        if not isinstance(payload.get("value"), dict):
            raise ShardUnavailableError()
        return {
            "shardId": shard_response.shard_id,
            "replicaId": shard_response.replica_id,
            "sortKey": key.sort_key,
            "value": payload["value"],
            "version": payload.get("version"),
            "timestamp": payload.get("timestamp"),
        }

    def read_records_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> dict[str, Any]:
        self.storage.get_table(table_name)
        if not partition_key.strip():
            raise InvalidRequestError("Partition key value cannot be empty")

        if not self.sharding_enabled:
            return {
                "shardId": None,
                "replicaId": None,
                "records": self.storage.list_records_by_partition(
                    table_name=table_name,
                    partition_key=partition_key,
                ),
            }

        partition = self._read_partition_from(table_name, partition_key, None)
        fallback = self.rebalancer.fallback_shard(partition_key)
        if not partition["records"] and fallback is not None:
            partition = self._read_partition_from(table_name, partition_key, fallback)
        return partition

    def _read_partition_from(
        self, table_name: str, partition_key: str, shard_id: str | None
    ) -> dict[str, Any]:
        shard_response = self.shards.read(
            "GET",
            table_name,
            RecordKey(partition_key=partition_key),
            resource="records",
            shard_id=shard_id,
        )
        if shard_response.response.status_code != 200:
            raise ShardUnavailableError()
        return {
            "shardId": shard_response.shard_id,
            "replicaId": shard_response.replica_id,
            "records": read_records(shard_response.response),
        }

    def record_exists(self, table_name: str, key: RecordKey) -> bool:
        self._validate_record_key(table_name, key)

        if not self.sharding_enabled:
            return self.storage.record_exists(table_name=table_name, key=key)

        if self._exists_on(table_name, key, shard_id=None):
            return True
        fallback = self.rebalancer.fallback_shard(key.partition_key)
        return fallback is not None and self._exists_on(table_name, key, fallback)

    def _exists_on(self, table_name: str, key: RecordKey, shard_id: str | None) -> bool:
        response = self.shards.read(
            "HEAD", table_name, key, resource="records", shard_id=shard_id
        ).response
        if response.status_code == 404:
            return False
        if response.status_code != 200:
            raise ShardUnavailableError()
        return True
