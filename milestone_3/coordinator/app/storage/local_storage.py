from copy import deepcopy
from typing import Any

from coordinator.app.storage.storage import (
    RecordKey,
    RecordValue,
    Storage,
    TableAlreadyExistsError,
)


class LocalStorage(Storage):
    """In-memory implementation used when coordinator sharding is disabled."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, Any]] = {}
        self._records: dict[str, dict[RecordKey, RecordValue]] = {}

    def create_table(
        self,
        table_name: str,
        partition_key: str,
        sort_key: str | None = None,
    ) -> dict[str, Any]:
        if table_name in self._tables:
            raise TableAlreadyExistsError("Table already exists")

        table = {
            "tableName": table_name,
            "partitionKey": partition_key,
            "sortKey": sort_key,
        }

        self._tables[table_name] = table
        self._records[table_name] = {}

        return deepcopy(table)

    def list_tables(self) -> list[dict[str, Any]]:
        return deepcopy(list(self._tables.values()))

    def get_table(self, table_name: str) -> dict[str, Any]:
        if table_name not in self._tables:
            raise KeyError(f"Table '{table_name}' does not exist")

        return deepcopy(self._tables[table_name])

    def put_record(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue,
    ) -> bool:
        self.get_table(table_name)

        table_records = self._records[table_name]
        created = key not in table_records
        table_records[key] = deepcopy(value)

        return created

    def list_records_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> list[dict[str, Any]]:
        self.get_table(table_name)

        records = [
            {"sortKey": key.sort_key, "value": deepcopy(value)}
            for key, value in self._records[table_name].items()
            if key.partition_key == partition_key
        ]
        return sorted(records, key=lambda record: record["sortKey"] or "")

    def delete_record(self, table_name: str, key: RecordKey) -> bool:
        self.get_table(table_name)

        table_records = self._records[table_name]

        if key not in table_records:
            return False

        del table_records[key]

        return True

    def record_exists(self, table_name: str, key: RecordKey) -> bool:
        self.get_table(table_name)

        return key in self._records[table_name]
