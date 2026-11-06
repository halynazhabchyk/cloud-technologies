from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Response,
    status,
)

from app.api.dependencies import get_storage
from app.api.parameters import PartitionKey, SortKey, TableName
from app.api.schemas.records import RecordBody
from app.storage import InMemoryShardStorage, RecordKey

router = APIRouter(tags=["Records"])


def build_key(partition_key: str, sort_key: str | None) -> RecordKey:
    return RecordKey(partition_key=partition_key, sort_key=sort_key)


@router.get("/internal/records")
def list_all_records(storage: InMemoryShardStorage = Depends(get_storage)):
    return storage.list_all()


@router.put("/internal/tables/{tableName}/records")
def put_record(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    body: RecordBody = Body(...),
    storage: InMemoryShardStorage = Depends(get_storage),
):
    created = storage.put(
        table_name,
        build_key(partition_key, sort_key),
        body.value,
    )
    return {"created": created}


@router.get("/internal/tables/{tableName}/records")
def list_partition_records(
    table_name: TableName,
    partition_key: PartitionKey,
    storage: InMemoryShardStorage = Depends(get_storage),
):
    return storage.list_by_partition(table_name, partition_key)


@router.head("/internal/tables/{tableName}/records")
def record_exists(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey,
    storage: InMemoryShardStorage = Depends(get_storage),
):
    if not storage.exists(
        table_name,
        build_key(partition_key, sort_key),
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Record not found",
        )
    return Response(status_code=status.HTTP_200_OK)


@router.delete("/internal/tables/{tableName}/records")
def delete_record(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey,
    storage: InMemoryShardStorage = Depends(get_storage),
):
    deleted = storage.delete(
        table_name,
        build_key(partition_key, sort_key),
    )

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Record not found",
        )

    return {"deleted": True}
