from copy import deepcopy
from typing import Any

from app.storage.models import RecordKey, RecordValue, StoredRecord, Version
from app.storage.storage import ShardStorage
from app.storage.versioning import is_newer


def _public_record(record: StoredRecord) -> dict[str, Any]:
    return {
        "value": deepcopy(record.value),
        "version": record.version.offset,
        "timestamp": record.version.timestamp,
    }


class InMemoryShardStorage(ShardStorage):

    def __init__(self) -> None:
        self._records: dict[tuple[str, RecordKey], StoredRecord] = {}

    def write(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue | None,
        version: Version,
    ) -> bool:
        storage_key = (table_name, key)
        stored = self._records.get(storage_key)
        if not is_newer(version, stored.version if stored else None):
            return False
        self._records[storage_key] = StoredRecord(deepcopy(value), version)
        return True

    def current(self, table_name: str, key: RecordKey) -> StoredRecord | None:
        return self._records.get((table_name, key))

    def get(self, table_name: str, key: RecordKey) -> StoredRecord | None:
        record = self._records.get((table_name, key))
        if record is None or record.is_tombstone:
            return None
        return StoredRecord(deepcopy(record.value), record.version)

    def _live_items(self):
        return (
            (table_name, key, record)
            for (table_name, key), record in self._records.items()
            if not record.is_tombstone
        )

    def list_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> list[dict[str, Any]]:
        records = [
            {"sortKey": key.sort_key, **_public_record(record)}
            for record_table, key, record in self._live_items()
            if record_table == table_name and key.partition_key == partition_key
        ]
        return sorted(records, key=lambda record: record["sortKey"] or "")

    def list_all(self) -> list[dict[str, Any]]:
        records = [
            {
                "tableName": table_name,
                "partitionKey": key.partition_key,
                "sortKey": key.sort_key,
                **_public_record(record),
            }
            for table_name, key, record in self._live_items()
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
        return self.get(table_name, key) is not None

    def live_count(self) -> int:
        return sum(1 for _ in self._live_items())
