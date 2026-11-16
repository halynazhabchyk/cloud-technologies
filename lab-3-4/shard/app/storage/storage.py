from abc import ABC, abstractmethod

from app.storage.models import RecordKey, RecordValue, StoredRecord, Version


class ShardStorage(ABC):
    """Storage contract implemented by a shard-local record store."""

    @abstractmethod
    def write(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue | None,
        version: Version,
    ) -> bool:
        """Store a value (``None`` = delete) if ``version`` wins; True if stored."""
        ...

    @abstractmethod
    def current(self, table_name: str, key: RecordKey) -> StoredRecord | None:
        """Return the stored record, tombstones included."""
        ...

    @abstractmethod
    def get(self, table_name: str, key: RecordKey) -> StoredRecord | None:
        """Return a live record or None when it does not exist or was deleted."""
        ...

    @abstractmethod
    def list_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> list[dict[str, object]]:
        """Return all live records for a partition."""
        ...

    @abstractmethod
    def list_all(self) -> list[dict[str, object]]:
        """Return all live records stored by the shard."""
        ...

    @abstractmethod
    def exists(self, table_name: str, key: RecordKey) -> bool:
        """Check whether a live record exists."""
        ...

    @abstractmethod
    def live_count(self) -> int:
        """Number of live (not deleted) records."""
        ...
