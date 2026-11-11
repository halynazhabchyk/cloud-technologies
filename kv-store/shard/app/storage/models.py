from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RecordKey:
    partition_key: str
    sort_key: str | None = None


RecordValue = dict[str, Any]
