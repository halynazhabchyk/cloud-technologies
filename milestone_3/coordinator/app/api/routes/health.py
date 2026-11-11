from fastapi import APIRouter, Depends

from coordinator.app.api.dependencies import get_coordinator
from coordinator.app.services.coordinator import Coordinator

router = APIRouter(tags=["Health"])


@router.get("/")
def root():
    return {"message": "Coordinator is running"}


@router.get("/health")
def health(coordinator: Coordinator = Depends(get_coordinator)):
    registry = coordinator.shard_registry
    return {
        "status": "ok",
        "shardingEnabled": coordinator.sharding_enabled,
        "registeredShards": registry.shard_count(),
        "registeredReplicas": registry.replica_count(),
        **coordinator.replication_summary(),
    }
