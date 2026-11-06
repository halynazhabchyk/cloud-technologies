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
        created = coordinator.shard_registry.register(
            registration.shard_id,
            str(registration.base_url),
        )
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error

    if not created:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A shard with this identifier already exists",
        )

    return {
        "shardId": registration.shard_id,
        "baseUrl": str(registration.base_url).rstrip("/"),
    }


@router.get("")
def list_shards(
    coordinator: Coordinator = Depends(get_coordinator),
):
    return coordinator.shard_registry.list_shards()
