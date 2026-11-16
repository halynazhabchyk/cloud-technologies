from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, status

from coordinator.app.api.dependencies import get_coordinator
from coordinator.app.services.coordinator import Coordinator

router = APIRouter(tags=["Cluster"])


@router.get("/internal/prometheus-targets")
def prometheus_targets(coordinator: Coordinator = Depends(get_coordinator)):
    return [
        {
            "targets": [urlparse(replica.base_url).netloc],
            "labels": {
                "shard": shard_id,
                "replica": replica.replica_id,
                "role": replica.role,
            },
        }
        for shard_id, replica in coordinator.shard_registry.replicas()
    ]


@router.get("/rebalance")
def rebalance_status(coordinator: Coordinator = Depends(get_coordinator)):
    """Progress of the last background rebalancing."""
    return coordinator.rebalancer.status.as_dict()


@router.post("/rebalance/{shardId}", status_code=status.HTTP_202_ACCEPTED)
def start_rebalance(shardId: str, coordinator: Coordinator = Depends(get_coordinator)):
    """Repeat rebalancing for a shard, e.g. after a run that ended with errors."""
    if shardId not in coordinator.shard_registry.routable_shard_ids():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Shard '{shardId}' is not on the ring",
        )
    coordinator.rebalancer.schedule(shardId)
    return {"scheduled": shardId}
