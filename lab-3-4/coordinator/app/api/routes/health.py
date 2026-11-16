from fastapi import APIRouter, Depends

from common.build_info import APP_VERSION
from coordinator.app.api.dependencies import get_coordinator
from coordinator.app.services.coordinator import Coordinator
from coordinator.app.sharding.shard_registry import MIN_SHARDS

router = APIRouter(tags=["Health"])


@router.get("/")
def root():
    return {"message": "Coordinator is running"}


@router.get("/health")
def health(coordinator: Coordinator = Depends(get_coordinator)):
    registry = coordinator.shard_registry
    replicas = registry.replica_count()
    healthy = registry.healthy_replica_count()
    ready = not coordinator.sharding_enabled or registry.shard_count() >= MIN_SHARDS
    return {
        "status": "ok" if healthy == replicas else "degraded",
        "ready": ready,
        "version": APP_VERSION,
        "shardingEnabled": coordinator.sharding_enabled,
        "registeredShards": registry.shard_count(),
        "registeredReplicas": replicas,
        "healthyReplicas": healthy,
        **coordinator.replication_summary(),
    }
