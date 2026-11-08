from pydantic import AnyHttpUrl, BaseModel, Field


class ShardRegistration(BaseModel):
    shard_id: str = Field(alias="shardId", min_length=1, max_length=128)
    base_url: AnyHttpUrl = Field(alias="baseUrl")
