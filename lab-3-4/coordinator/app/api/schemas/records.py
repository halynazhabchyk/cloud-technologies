from typing import Any

from pydantic import BaseModel, Field


class Record(BaseModel):
    value: dict[str, Any]


class RecordWriteResponse(Record):
    shard_id: str | None = Field(alias="shardId")
    # Offset of the write in the shard's replication log (null in single-node).
    version: int | None = None
    # Last-Write-Wins timestamp in microseconds (null in single-node).
    timestamp: int | None = None


class PartitionRecord(BaseModel):
    sort_key: str | None = Field(default=None, alias="sortKey")
    value: dict[str, Any]
    version: int | None = None
    timestamp: int | None = None


class RecordReadResponse(PartitionRecord):
    shard_id: str | None = Field(alias="shardId")
    replica_id: str | None = Field(default=None, alias="replicaId")


class PartitionReadResponse(BaseModel):
    shard_id: str | None = Field(alias="shardId")
    replica_id: str | None = Field(default=None, alias="replicaId")
    records: list[PartitionRecord]
