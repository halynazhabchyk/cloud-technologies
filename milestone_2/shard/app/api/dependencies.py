from fastapi import Request

from app.storage import InMemoryShardStorage


def get_storage(request: Request) -> InMemoryShardStorage:
    return request.app.state.storage
