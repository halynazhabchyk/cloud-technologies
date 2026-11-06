from fastapi import FastAPI

from coordinator.app.api.routes import records, shards, tables
from coordinator.app.core.config import REQUEST_TIMEOUT, SHARDING_ENABLED
from coordinator.app.services.coordinator import Coordinator
from coordinator.app.sharding.shard_registry import ShardRegistry
from coordinator.app.storage.local_storage import LocalStorage


def create_app() -> FastAPI:
    app = FastAPI(
        title="Distributed Key-Value Store",
        description="Coordinator API for the Milestone 2 key-value store",
        version="1.0.0",
    )
    app.state.coordinator = Coordinator(
        LocalStorage(),
        shard_registry=ShardRegistry(),
        sharding_enabled=SHARDING_ENABLED,
        request_timeout=REQUEST_TIMEOUT,
    )

    app.include_router(tables.router)
    app.include_router(records.router)
    if SHARDING_ENABLED:
        app.include_router(shards.router)

    @app.get("/", tags=["Health"])
    def health_check():
        return {"message": "Coordinator is running"}

    return app


app = create_app()
