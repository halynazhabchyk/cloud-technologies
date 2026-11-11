from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, Field


class ShardRegistration(BaseModel):
    shard_id: str = Field(alias="shardId", min_length=1, max_length=128)
    base_url: AnyHttpUrl = Field(alias="baseUrl")
    # Defaults keep the milestone 2 payload valid: one leader per shard.
    replica_id: str | None = Field(
        default=None, alias="replicaId", min_length=1, max_length=128
    )
    role: Literal["leader", "follower"] = "leader"
