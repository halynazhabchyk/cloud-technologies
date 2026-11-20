from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    Response,
    status,
)

from app.api.dependencies import get_node
from app.api.parameters import PartitionKey, SortKey, TableName
from app.api.schemas.records import RecordBody
from app.replication.node import (
    NotLeaderError,
    ReplicaNode,
    WriteConflictError,
    WriteResult,
)
from app.storage import RecordKey
from common.replica_headers import APPLIED_OFFSET_HEADER, REPLICA_ID_HEADER

router = APIRouter(tags=["Records"])

Acks = Query(
    default=1,
    ge=1,
    le=16,
    description="Replicas (leader included) that must hold the write before 2xx",
)
Timestamp = Query(
    default=None,
    ge=1,
    description="Write time in microseconds set by the coordinator (Last-Write-Wins)",
)


def build_key(partition_key: str, sort_key: str | None) -> RecordKey:
    return RecordKey(partition_key=partition_key, sort_key=sort_key)


def replica_headers(node: ReplicaNode) -> dict[str, str]:
    return {
        APPLIED_OFFSET_HEADER: str(node.applied_offset),
        REPLICA_ID_HEADER: node.settings.replica_id,
    }


def not_found(node: ReplicaNode) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Record not found",
        headers=replica_headers(node),
    )


def rejected_write(error: NotLeaderError | WriteConflictError) -> HTTPException:
    """Both a write to a follower and a write that lost LWW are conflicts."""
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error))


def ensure_write_quorum(node: ReplicaNode, offset: int, acks: int) -> None:
    if not node.wait_for_write_quorum(offset, acks):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Write quorum not reached: {acks} replicas did not confirm",
        )


def write_response(result: WriteResult, **fields: object) -> dict[str, object]:
    return {
        **fields,
        "version": result.version.offset,
        "timestamp": result.version.timestamp,
    }


@router.get("/internal/records")
def list_all_records(response: Response, node: ReplicaNode = Depends(get_node)):
    with node.lock:
        response.headers.update(replica_headers(node))
        return node.storage.list_all()


@router.put("/internal/tables/{tableName}/records")
def put_record(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    acks: int = Acks,
    timestamp: int | None = Timestamp,
    body: RecordBody = Body(...),
    node: ReplicaNode = Depends(get_node),
):
    key = build_key(partition_key, sort_key)
    try:
        result = node.put(table_name, key, body.value, timestamp)
    except (NotLeaderError, WriteConflictError) as error:
        raise rejected_write(error) from error

    ensure_write_quorum(node, result.version.offset, acks)
    return write_response(result, created=result.created)


@router.get("/internal/tables/{tableName}/records")
def list_partition_records(
    response: Response,
    table_name: TableName,
    partition_key: PartitionKey,
    node: ReplicaNode = Depends(get_node),
):
    with node.lock:
        response.headers.update(replica_headers(node))
        return node.storage.list_by_partition(table_name, partition_key)


@router.get("/internal/tables/{tableName}/record")
def get_record(
    response: Response,
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    node: ReplicaNode = Depends(get_node),
):
    key = build_key(partition_key, sort_key)
    with node.lock:
        record = node.storage.get(table_name, key)
        if record is None:
            raise not_found(node)
        response.headers.update(replica_headers(node))
        return {
            "sortKey": sort_key,
            "value": record.value,
            "version": record.version.offset,
            "timestamp": record.version.timestamp,
        }


@router.head("/internal/tables/{tableName}/records")
def record_exists(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    node: ReplicaNode = Depends(get_node),
):
    with node.lock:
        if not node.storage.exists(table_name, build_key(partition_key, sort_key)):
            raise not_found(node)
        return Response(status_code=status.HTTP_200_OK, headers=replica_headers(node))


@router.delete("/internal/tables/{tableName}/records")
def delete_record(
    table_name: TableName,
    partition_key: PartitionKey,
    sort_key: SortKey = None,
    acks: int = Acks,
    timestamp: int | None = Timestamp,
    node: ReplicaNode = Depends(get_node),
):
    try:
        result = node.delete(table_name, build_key(partition_key, sort_key), timestamp)
    except (NotLeaderError, WriteConflictError) as error:
        raise rejected_write(error) from error

    if result is None:
        raise not_found(node)

    ensure_write_quorum(node, result.version.offset, acks)
    return write_response(result, deleted=True)
