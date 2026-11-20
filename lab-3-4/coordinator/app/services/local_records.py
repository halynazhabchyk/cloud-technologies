from typing import Any

from coordinator.app.storage.storage import RecordKey, RecordValue, Storage


class LocalRecords:
    """Single-node mode: records stay in the coordinator's own storage.

    There are no shards and no replication, so ``shardId``, ``replicaId`` and
    ``version`` are always None and the write timestamp is not used.
    """

    def __init__(self, storage: Storage):
        self.storage = storage

    def put(
        self, table_name: str, key: RecordKey, value: RecordValue, timestamp: int
    ) -> dict[str, Any]:
        created = self.storage.put_record(table_name, key, value)
        return {"created": created, "shardId": None, "version": None}

    def delete(self, table_name: str, key: RecordKey, timestamp: int) -> bool:
        return self.storage.delete_record(table_name, key)

    def read(self, table_name: str, key: RecordKey) -> dict[str, Any] | None:
        value = self.storage.get_record(table_name, key)
        if value is None:
            return None
        return {
            "shardId": None,
            "replicaId": None,
            "sortKey": key.sort_key,
            "value": value,
            "version": None,
        }

    def read_partition(self, table_name: str, partition_key: str) -> dict[str, Any]:
        records = self.storage.list_records_by_partition(table_name, partition_key)
        return {"shardId": None, "replicaId": None, "records": records}

    def exists(self, table_name: str, key: RecordKey) -> bool:
        return self.storage.record_exists(table_name, key)
