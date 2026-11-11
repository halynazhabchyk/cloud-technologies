from fastapi import Request

from app.replication.node import ReplicaNode
from app.storage import InMemoryShardStorage


def get_node(request: Request) -> ReplicaNode:
    return request.app.state.node


def get_storage(request: Request) -> InMemoryShardStorage:
    return request.app.state.node.storage
