from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RecordKey:
    partition_key: str
    sort_key: str | None = None


RecordValue = dict[str, Any]


class TableAlreadyExistsError(ValueError):
    """Raised when a table creation request conflicts with an existing table."""


class Storage(ABC):
    """Storage contract used by the coordinator's local mode."""

    @abstractmethod
    def create_table(
        self,
        table_name: str,
        partition_key: str,
        sort_key: str | None = None,
    ) -> dict[str, Any]:
        """Create a table and return its public definition."""
        ...

    @abstractmethod
    def list_tables(self) -> list[dict[str, Any]]:
        """Return all table definitions."""
        ...

    @abstractmethod
    def get_table(self, table_name: str) -> dict[str, Any]:
        """Return one table definition or raise KeyError."""
        ...

    @abstractmethod
    def put_record(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue,
    ) -> bool:
        """Create or replace a record; return whether it was created."""
        ...

    @abstractmethod
    def list_records_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> list[dict[str, Any]]:
        """Return all records for one partition."""
        ...

    @abstractmethod
    def delete_record(self, table_name: str, key: RecordKey) -> bool:
        """Delete a record and report whether it existed."""
        ...

    @abstractmethod
    def record_exists(self, table_name: str, key: RecordKey) -> bool:
        """Check whether a record exists."""
        ...
