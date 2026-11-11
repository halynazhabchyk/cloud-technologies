import asyncio
import logging
from collections.abc import AsyncGenerator

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import ReplicaSettings, load_settings
from app.registration import register_replica
from app.replication.follower import start_follower
from app.replication.log import LeaderLog
from app.replication.node import ReplicaNode

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
# Followers send an ack to the leader after every batch; per-request logs are noise.
logging.getLogger("httpx").setLevel(logging.WARNING)


def create_app(
    settings: ReplicaSettings | None = None,
    leader_log: LeaderLog | None = None,
) -> FastAPI:
    """Build one replica; ``leader_log`` replaces Kafka in tests."""
    settings = settings or load_settings()
    node = ReplicaNode(settings, leader_log)

    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        follower = None if settings.is_leader else start_follower(node)
        registration_task = asyncio.create_task(register_replica(settings))
        try:
            yield
        finally:
            registration_task.cancel()
            await asyncio.gather(registration_task, return_exceptions=True)
            if follower is not None:
                follower[1].set()

    app = FastAPI(
        title=f"Key-Value Shard: {settings.shard_id} / {settings.replica_id}",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.node = node
    app.include_router(router)

    @app.get("/health")
    def health():
        return {"status": "ok", **node.status()}

    return app
