from typing import Any

from coordinator.app.services.errors import ShardUnavailableError, WriteConflictError
from coordinator.app.services.rebalancer import Rebalancer
from coordinator.app.services.shard_client import ShardClient
from coordinator.app.services.shard_responses import (
    error_detail,
    read_json_object,
    read_records,
)
from coordinator.app.storage.storage import RecordKey, RecordValue

CURRENT_OWNER = None  # shard_id=None: route by the current hash ring


class ShardedRecords:
    """Cluster mode: records live on the shards; this class turns replica
    responses into coordinator results and errors.
    """

    def __init__(self, shards: ShardClient, rebalancer: Rebalancer):
        self.shards = shards
        self.rebalancer = rebalancer

    # --- writes -------------------------------------------------------------

    def put(
        self, table_name: str, key: RecordKey, value: RecordValue, timestamp: int
    ) -> dict[str, Any]:
        shard_response = self.shards.put_record(
            table_name, key, value, timestamp=timestamp
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

    def delete(self, table_name: str, key: RecordKey, timestamp: int) -> bool:
        response = self.shards.delete_record(
            table_name, key, timestamp=timestamp
        ).response
        if response.status_code == 404:
            return False
        if response.status_code == 409:
            raise WriteConflictError(error_detail(response))
        if response.status_code != 200:
            raise ShardUnavailableError()
        if read_json_object(response).get("deleted") is not True:
            raise ShardUnavailableError()
        return True

    # --- reads --------------------------------------------------------------

    def read(self, table_name: str, key: RecordKey) -> dict[str, Any] | None:
        for shard_id in self._shards_to_ask(key.partition_key):
            record = self._read_from(shard_id, table_name, key)
            if record is not None:
                return record
        return None

    def read_partition(self, table_name: str, partition_key: str) -> dict[str, Any]:
        partition: dict[str, Any] = {}
        for shard_id in self._shards_to_ask(partition_key):
            partition = self._read_partition_from(shard_id, table_name, partition_key)
            if partition["records"]:
                break
        return partition

    def exists(self, table_name: str, key: RecordKey) -> bool:
        return any(
            self._exists_on(shard_id, table_name, key)
            for shard_id in self._shards_to_ask(key.partition_key)
        )

    def _shards_to_ask(self, partition_key: str) -> list[str | None]:
        """The current owner first; while rebalancing, then the previous owner."""
        # DEFENSE: під час ребалансування ключ може ще лежати на старому шарді,
        # тому читання, яке не знайшло його в нового власника, питає старого.
        previous_owner = self.rebalancer.fallback_shard(partition_key)
        if previous_owner is None:
            return [CURRENT_OWNER]
        return [CURRENT_OWNER, previous_owner]

    def _read_from(
        self, shard_id: str | None, table_name: str, key: RecordKey
    ) -> dict[str, Any] | None:
        shard_response = self.shards.get_record(table_name, key, shard_id=shard_id)
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

    def _read_partition_from(
        self, shard_id: str | None, table_name: str, partition_key: str
    ) -> dict[str, Any]:
        shard_response = self.shards.get_partition(
            table_name, partition_key, shard_id=shard_id
        )
        if shard_response.response.status_code != 200:
            raise ShardUnavailableError()
        return {
            "shardId": shard_response.shard_id,
            "replicaId": shard_response.replica_id,
            "records": read_records(shard_response.response),
        }

    def _exists_on(self, shard_id: str | None, table_name: str, key: RecordKey) -> bool:
        response = self.shards.head_record(table_name, key, shard_id=shard_id).response
        if response.status_code == 404:
            return False
        if response.status_code != 200:
            raise ShardUnavailableError()
        return True
