from typing import Annotated

from fastapi import Path, Query

TableName = Annotated[
    str,
    Path(alias="tableName", min_length=1, max_length=128),
]
PartitionKey = Annotated[
    str,
    Path(alias="partitionKey", min_length=1, max_length=512),
]
SortKey = Annotated[
    str | None,
    Query(alias="sortKey", min_length=1, max_length=512),
]
