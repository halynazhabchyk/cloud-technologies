import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import ReplicaSettings, load_settings
from app.metrics import update_replica_metrics
from app.registration import register_replica
from app.replication.follower import start_follower
from app.replication.log import LeaderLog
from app.replication.node import ReplicaNode
from common.build_info import APP_VERSION, build_info
from common.errors import install_error_format
from common.http_observability import install_observability
from common.json_logging import configure_logging

SERVICE_NAME = "shard"


def create_app(
    settings: ReplicaSettings | None = None,
    leader_log: LeaderLog | None = None,
) -> FastAPI:
    """Build one replica; ``leader_log`` replaces Kafka in tests."""
    settings = settings or load_settings()
    configure_logging(SERVICE_NAME, replica_id=settings.replica_id)
    node = ReplicaNode(settings, leader_log)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
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
        version=APP_VERSION,
        lifespan=lifespan,
    )
    app.state.node = node
    install_observability(
        app, SERVICE_NAME, before_scrape=lambda: update_replica_metrics(node)
    )
    install_error_format(app)
    app.include_router(router)

    @app.get("/health", tags=["Health"])
    def health():
        """Liveness plus the replica's role, offset, key count and lag."""
        return {
            "status": "ok",
            "version": build_info(SERVICE_NAME)["version"],
            **node.status(),
        }

    return app
