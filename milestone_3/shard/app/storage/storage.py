from abc import ABC, abstractmethod

from app.storage.models import RecordKey, RecordValue


class ShardStorage(ABC):
    """Storage contract implemented by a shard-local record store."""

    @abstractmethod
    def put(self, table_name: str, key: RecordKey, value: RecordValue) -> bool:
        """Create or replace a record and report whether it was created."""
        ...

    @abstractmethod
    def get(self, table_name: str, key: RecordKey) -> RecordValue | None:
        """Return a record value or None when it does not exist."""
        ...

    @abstractmethod
    def list_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> list[dict[str, object]]:
        """Return all records for a partition."""
        ...

    @abstractmethod
    def list_all(self) -> list[dict[str, object]]:
        """Return all records stored by the shard."""
        ...

    @abstractmethod
    def exists(self, table_name: str, key: RecordKey) -> bool:
        """Check whether a record exists."""
        ...

    @abstractmethod
    def delete(self, table_name: str, key: RecordKey) -> bool:
        """Delete a record and report whether it existed."""
        ...
