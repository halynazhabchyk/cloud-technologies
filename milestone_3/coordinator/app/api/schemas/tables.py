from pydantic import BaseModel, ConfigDict, Field


class TableDefinition(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    table_name: str = Field(alias="tableName", min_length=1, max_length=128)
    partition_key: str = Field(alias="partitionKey", min_length=1, max_length=128)
    sort_key: str | None = Field(
        default=None,
        alias="sortKey",
        min_length=1,
        max_length=128,
    )
