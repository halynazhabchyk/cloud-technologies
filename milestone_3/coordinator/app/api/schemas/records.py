from typing import Any

from pydantic import BaseModel, Field


class Record(BaseModel):
    value: dict[str, Any]


class RecordWriteResponse(Record):
    shard_id: str | None = Field(alias="shardId")


class PartitionRecord(BaseModel):
    sort_key: str | None = Field(default=None, alias="sortKey")
    value: dict[str, Any]


class PartitionReadResponse(BaseModel):
    shard_id: str | None = Field(alias="shardId")
    records: list[PartitionRecord]
