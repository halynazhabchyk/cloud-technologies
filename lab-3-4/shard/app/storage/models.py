from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RecordKey:
    partition_key: str
    sort_key: str | None = None


RecordValue = dict[str, Any]


@dataclass(frozen=True)
class Version:

    timestamp: int
    offset: int


@dataclass(frozen=True)
class StoredRecord:
    """A record as kept by a replica; ``value is None`` marks a tombstone."""

    value: RecordValue | None
    version: Version

    @property
    def is_tombstone(self) -> bool:
        return self.value is None
