from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from common.build_info import APP_VERSION
from common.errors import install_error_format
from common.http_observability import install_observability
from common.json_logging import configure_logging
from coordinator.app.api.errors import register_error_handlers
from coordinator.app.api.routes import cluster, health, records, shards, tables
from coordinator.app.core.config import CoordinatorSettings, load_settings
from coordinator.app.services.coordinator import Coordinator
from coordinator.app.services.health_checker import ReplicaHealthChecker
from coordinator.app.services.metrics import update_cluster_metrics
from coordinator.app.sharding.shard_registry import ShardRegistry
from coordinator.app.storage.local_storage import LocalStorage

SERVICE_NAME = "coordinator"


def create_app(settings: CoordinatorSettings | None = None) -> FastAPI:
    settings = settings or load_settings()
    configure_logging(SERVICE_NAME)
    storage = LocalStorage(settings.tables_file)
    coordinator = Coordinator(storage, ShardRegistry(), settings)
    health_checker = ReplicaHealthChecker(coordinator.shard_registry, settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if settings.sharding_enabled:
            health_checker.start()
        try:
            yield
        finally:
            health_checker.stop()

    app = FastAPI(
        title="Distributed Key-Value Store",
        description="Key-value store coordinator: sharding, replication, monitoring",
        version=APP_VERSION,
        lifespan=lifespan,
    )
    app.state.coordinator = coordinator

    install_observability(
        app,
        SERVICE_NAME,
        before_scrape=lambda: update_cluster_metrics(coordinator.shard_registry),
    )
    install_error_format(app)
    register_error_handlers(app)
    app.include_router(health.router)
    app.include_router(tables.router)
    app.include_router(records.router)
    if settings.sharding_enabled:
        app.include_router(shards.router)
        app.include_router(cluster.router)

    return app


app = create_app()
