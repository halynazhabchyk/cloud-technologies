"""Interface of the place where the coordinator keeps records.

Two implementations: ``LocalRecords`` (single node, records in memory) and
``ShardedRecords`` (records on the shards). The coordinator picks one at start
and does not branch on the mode in every method.
"""

from typing import Any, Protocol

from coordinator.app.storage.storage import RecordKey, RecordValue


class RecordBackend(Protocol):
    def put(
        self, table_name: str, key: RecordKey, value: RecordValue, timestamp: int
    ) -> dict[str, Any]:
        """Create or replace a record; return created, shardId and version."""
        ...

    def delete(self, table_name: str, key: RecordKey, timestamp: int) -> bool:
        """Delete a record; return False when it does not exist."""
        ...

    def read(self, table_name: str, key: RecordKey) -> dict[str, Any] | None:
        """Read one record by its full key; None when it does not exist."""
        ...

    def read_partition(self, table_name: str, partition_key: str) -> dict[str, Any]:
        """Read every record of one partition."""
        ...

    def exists(self, table_name: str, key: RecordKey) -> bool:
        """Check that a record exists."""
        ...
