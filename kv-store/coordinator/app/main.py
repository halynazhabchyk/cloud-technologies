from fastapi import FastAPI

from coordinator.app.api.errors import register_error_handlers
from coordinator.app.api.routes import health, records, shards, tables
from coordinator.app.core.config import CoordinatorSettings, load_settings
from coordinator.app.services.coordinator import Coordinator
from coordinator.app.sharding.shard_registry import ShardRegistry
from coordinator.app.storage.local_storage import LocalStorage


def create_app(settings: CoordinatorSettings | None = None) -> FastAPI:
    settings = settings or load_settings()
    app = FastAPI(
        title="Distributed Key-Value Store",
        description="Key-value store coordinator: sharding + replication",
        version="1.0.0",
    )
    app.state.coordinator = Coordinator(LocalStorage(), ShardRegistry(), settings)

    register_error_handlers(app)
    app.include_router(health.router)
    app.include_router(tables.router)
    app.include_router(records.router)
    if settings.sharding_enabled:
        app.include_router(shards.router)

    return app


app = create_app()
