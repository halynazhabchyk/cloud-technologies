from copy import deepcopy
from pathlib import Path
from typing import Any

from coordinator.app.storage.storage import (
    RecordKey,
    RecordValue,
    Storage,
    TableAlreadyExistsError,
)
from coordinator.app.storage.table_catalog_file import (
    load_table_definitions,
    save_table_definitions,
)


class LocalStorage(Storage):
    """Table definitions plus in-memory records for the single-node mode.

    With ``tables_file`` the table definitions survive a coordinator restart;
    without it (tests, single-node demo) everything stays in memory.
    """

    def __init__(self, tables_file: Path | None = None) -> None:
        self._tables_file = tables_file
        self._tables: dict[str, dict[str, Any]] = (
            load_table_definitions(tables_file) if tables_file else {}
        )
        self._records: dict[str, dict[RecordKey, RecordValue]] = {
            table_name: {} for table_name in self._tables
        }

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
        if self._tables_file:
            save_table_definitions(self._tables_file, self._tables)

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

    def get_record(self, table_name: str, key: RecordKey) -> RecordValue | None:
        self.get_table(table_name)

        value = self._records[table_name].get(key)
        return deepcopy(value) if value is not None else None

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
