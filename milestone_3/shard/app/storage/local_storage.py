from copy import deepcopy
from typing import Any

from app.storage.models import RecordKey, RecordValue
from app.storage.storage import ShardStorage


class InMemoryShardStorage(ShardStorage):
    """In-memory record store owned by one shard process."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, RecordKey], RecordValue] = {}

    def put(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue,
    ) -> bool:
        storage_key = (table_name, key)
        created = storage_key not in self._records
        self._records[storage_key] = deepcopy(value)
        return created

    def get(
        self,
        table_name: str,
        key: RecordKey,
    ) -> RecordValue | None:
        value = self._records.get((table_name, key))
        return deepcopy(value) if value is not None else None

    def list_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> list[dict[str, Any]]:
        records = [
            {"sortKey": key.sort_key, "value": deepcopy(value)}
            for (record_table, key), value in self._records.items()
            if record_table == table_name and key.partition_key == partition_key
        ]
        return sorted(records, key=lambda record: record["sortKey"] or "")

    def list_all(self) -> list[dict[str, Any]]:
        records = [
            {
                "tableName": table_name,
                "partitionKey": key.partition_key,
                "sortKey": key.sort_key,
                "value": deepcopy(value),
            }
            for (table_name, key), value in self._records.items()
        ]
        return sorted(
            records,
            key=lambda record: (
                record["tableName"],
                record["partitionKey"],
                record["sortKey"] or "",
            ),
        )

    def exists(self, table_name: str, key: RecordKey) -> bool:
        return (table_name, key) in self._records

    def delete(self, table_name: str, key: RecordKey) -> bool:
        storage_key = (table_name, key)
        if storage_key not in self._records:
            return False

        del self._records[storage_key]
        return True
