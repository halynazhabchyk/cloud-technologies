"""Record CRUD. Domain errors become HTTP codes in api/errors.py:
InvalidRequestError -> 400, ShardUnavailableError -> 503."""

from fastapi import APIRouter, Body, Depends, HTTPException, Response, status

from coordinator.app.api.dependencies import get_coordinator
from coordinator.app.api.parameters import PartitionKey, SortKey, TableName
from coordinator.app.api.schemas.records import (
    PartitionReadResponse,
    Record,
    RecordReadResponse,
    RecordWriteResponse,
)
from coordinator.app.services.coordinator import Coordinator
from coordinator.app.storage.storage import RecordKey

router = APIRouter(
    prefix="/tables/{tableName}/items/{partitionKey}",
    tags=["Records"],
)

RECORD_EXAMPLE = {
    "user": {
        "summary": "Приклад запису користувача",
        "value": {"name": "Alex", "email": "alex@example.com"},
    },
}


def existing_table(
    table_name: TableName,
    coordinator: Coordinator = Depends(get_coordinator),
) -> str:
    """Dependency: 404 unless the table is registered."""
    try:
        coordinator.get_table(table_name)
    except KeyError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' does not exist",
        ) from error
    return table_name


def record_not_found() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail="Record not found"
    )


@router.put(
    "",
    response_model=RecordWriteResponse,
    status_code=status.HTTP_201_CREATED,
    responses={status.HTTP_200_OK: {"model": RecordWriteResponse}},
)
def create_or_update_record(
    response: Response,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    record: Record = Body(openapi_examples=RECORD_EXAMPLE),
    table_name: str = Depends(existing_table),
    coordinator: Coordinator = Depends(get_coordinator),
):
    created, shard_id, version = coordinator.create_record(
        table_name, RecordKey(partition_key, sort_key), record.value
    )
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return {"value": record.value, "shardId": shard_id, "version": version}


@router.get("", response_model=RecordReadResponse | PartitionReadResponse)
def read_records(
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    table_name: str = Depends(existing_table),
    coordinator: Coordinator = Depends(get_coordinator),
):
    """With sortKey, read one record by its full key; otherwise the partition."""
    if sort_key is None:
        return coordinator.read_records_by_partition(table_name, partition_key)

    record = coordinator.read_record(table_name, RecordKey(partition_key, sort_key))
    if record is None:
        raise record_not_found()
    return record


@router.head("")
def record_exists(
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    table_name: str = Depends(existing_table),
    coordinator: Coordinator = Depends(get_coordinator),
):
    if not coordinator.record_exists(table_name, RecordKey(partition_key, sort_key)):
        raise record_not_found()
    return Response(status_code=status.HTTP_200_OK)


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
def delete_record(
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    table_name: str = Depends(existing_table),
    coordinator: Coordinator = Depends(get_coordinator),
):
    if not coordinator.delete_record(table_name, RecordKey(partition_key, sort_key)):
        raise record_not_found()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
