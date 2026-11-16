from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.api.dependencies import get_node
from app.replication.node import ReplicaNode

router = APIRouter(tags=["Replication"])


class Ack(BaseModel):
    replica_id: str = Field(alias="replicaId", min_length=1, max_length=128)
    applied_offset: int = Field(alias="appliedOffset", ge=0)


@router.post("/internal/ack")
def acknowledge(ack: Ack, node: ReplicaNode = Depends(get_node)):
    if not node.is_leader:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only the leader collects acknowledgements",
        )
    node.log.record_ack(ack.replica_id, ack.applied_offset)
    return {"replicaId": ack.replica_id, "ackedOffset": ack.applied_offset}
