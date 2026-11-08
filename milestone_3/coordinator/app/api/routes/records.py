from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Response,
    status,
)

from coordinator.app.api.dependencies import get_coordinator
from coordinator.app.api.parameters import PartitionKey, SortKey, TableName
from coordinator.app.api.schemas.records import (
    PartitionReadResponse,
    Record,
    RecordWriteResponse,
)
from coordinator.app.services.coordinator import Coordinator, ShardUnavailableError
from coordinator.app.storage.storage import RecordKey

router = APIRouter(
    prefix="/tables/{tableName}/items/{partitionKey}",
    tags=["Records"],
)


def build_key(
    partition_key: str,
    sort_key: str | None,
) -> RecordKey:
    return RecordKey(
        partition_key=partition_key,
        sort_key=sort_key,
    )


def ensure_table_exists(
    coordinator: Coordinator,
    table_name: str,
) -> None:
    try:
        coordinator.get_table(table_name)
    except KeyError as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' does not exist",
        ) from error


def handle_shard_error(_error: ShardUnavailableError) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="No available shard can handle the request",
    )


@router.put(
    "",
    response_model=RecordWriteResponse,
    status_code=status.HTTP_201_CREATED,
    responses={status.HTTP_200_OK: {"model": RecordWriteResponse}},
)
def create_or_update_record(
    response: Response,
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    record: Record = Body(
        openapi_examples={
            "user": {
                "summary": "Приклад запису користувача",
                "value": {
                    "name": "Alex",
                    "email": "alex@example.com",
                },
            },
        },
    ),
    coordinator: Coordinator = Depends(get_coordinator),
):
    ensure_table_exists(coordinator, table_name)

    key = build_key(partition_key, sort_key)

    try:
        created, shard_id = coordinator.create_record(table_name, key, record.value)
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error
    except ShardUnavailableError as error:
        raise handle_shard_error(error) from error

    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return {"value": record.value, "shardId": shard_id}


@router.get("", response_model=PartitionReadResponse)
def list_partition_records(
    table_name: TableName,
    partition_key: PartitionKey,
    coordinator: Coordinator = Depends(get_coordinator),
):
    ensure_table_exists(coordinator, table_name)

    try:
        return coordinator.read_records_by_partition(table_name, partition_key)
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error
    except ShardUnavailableError as error:
        raise handle_shard_error(error) from error


@router.head("")
def record_exists(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    coordinator: Coordinator = Depends(get_coordinator),
):
    ensure_table_exists(coordinator, table_name)

    key = build_key(partition_key, sort_key)

    try:
        exists = coordinator.record_exists(table_name, key)
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error
    except ShardUnavailableError as error:
        raise handle_shard_error(error) from error

    if not exists:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Record not found",
        )

    return Response(status_code=status.HTTP_200_OK)


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
def delete_record(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    coordinator: Coordinator = Depends(get_coordinator),
):
    ensure_table_exists(coordinator, table_name)

    key = build_key(partition_key, sort_key)

    try:
        deleted = coordinator.delete_record(table_name, key)
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error
    except ShardUnavailableError as error:
        raise handle_shard_error(error) from error

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Record not found",
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)
