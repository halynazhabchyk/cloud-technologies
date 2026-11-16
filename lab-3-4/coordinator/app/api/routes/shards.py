from fastapi import APIRouter, Depends, HTTPException, status

from coordinator.app.api.dependencies import get_coordinator
from coordinator.app.api.schemas.shards import ShardRegistration
from coordinator.app.services.coordinator import Coordinator

router = APIRouter(prefix="/shards", tags=["Shards"])


@router.post("", status_code=status.HTTP_201_CREATED)
def register_shard(
    registration: ShardRegistration,
    coordinator: Coordinator = Depends(get_coordinator),
):
    try:
        created = coordinator.register_replica(
            registration.shard_id,
            str(registration.base_url),
            replica_id=registration.replica_id,
            role=registration.role,
        )
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error

    if not created:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A replica with this identifier already exists",
        )

    return {
        "shardId": registration.shard_id,
        "replicaId": registration.replica_id or registration.shard_id,
        "role": registration.role,
        "baseUrl": str(registration.base_url).rstrip("/"),
    }


@router.get("")
def list_shards(
    coordinator: Coordinator = Depends(get_coordinator),
):
    return coordinator.shard_registry.list_shards()
