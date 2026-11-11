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
from app.replication.node import NotLeaderError, ReplicaNode
from app.storage import RecordKey

router = APIRouter(tags=["Records"])

# Every read response says which replica answered and how far it has applied
# the log; the coordinator uses the offset to pick the freshest quorum answer.
APPLIED_OFFSET_HEADER = "X-Applied-Offset"
REPLICA_ID_HEADER = "X-Replica-Id"

Acks = Query(
    default=1,
    ge=1,
    le=16,
    description="Replicas (leader included) that must hold the write before 2xx",
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


def not_leader(error: NotLeaderError) -> HTTPException:
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error))


def ensure_write_quorum(node: ReplicaNode, offset: int, acks: int) -> None:
    if not node.wait_for_write_quorum(offset, acks):
        # The entry stays in the log and will still replicate; the client is
        # only told that it was not confirmed by enough replicas in time.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Write quorum not reached: {acks} replicas did not confirm",
        )


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
    body: RecordBody = Body(...),
    node: ReplicaNode = Depends(get_node),
):
    try:
        created, offset = node.put(
            table_name,
            build_key(partition_key, sort_key),
            body.value,
        )
    except NotLeaderError as error:
        raise not_leader(error) from error

    ensure_write_quorum(node, offset, acks)
    return {"created": created, "version": offset}


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
        value = node.storage.get(table_name, key)
        if value is None:
            raise not_found(node)
        response.headers.update(replica_headers(node))
        return {
            "sortKey": sort_key,
            "value": value,
            "version": node.storage.get_version(table_name, key),
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
    node: ReplicaNode = Depends(get_node),
):
    try:
        offset = node.delete(table_name, build_key(partition_key, sort_key))
    except NotLeaderError as error:
        raise not_leader(error) from error

    if offset is None:
        raise not_found(node)

    ensure_write_quorum(node, offset, acks)
    return {"deleted": True, "version": offset}
