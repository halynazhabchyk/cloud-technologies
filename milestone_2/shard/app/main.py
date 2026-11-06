import asyncio
import logging
from collections.abc import AsyncGenerator

import httpx
from fastapi import FastAPI

from app.api.routes import router
from app.core.config import (
    COORDINATOR_URL,
    REGISTRATION_RETRY_SECONDS,
    SHARD_ID,
    SHARD_URL,
)
from app.storage import InMemoryShardStorage

logger = logging.getLogger(__name__)


async def register_shard() -> None:
    payload = {
        "shardId": SHARD_ID,
        "baseUrl": SHARD_URL,
    }

    while True:
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                response = await client.post(
                    f"{COORDINATOR_URL}/shards",
                    json=payload,
                )
                if response.status_code == 409:
                    shards_response = await client.get(f"{COORDINATOR_URL}/shards")
                    shards_response.raise_for_status()
                    registered = next(
                        (
                            shard
                            for shard in shards_response.json()
                            if shard.get("shardId") == SHARD_ID
                        ),
                        None,
                    )
                    registered_url = (
                        registered.get("baseUrl", "").rstrip("/")
                        if registered
                        else None
                    )
                    if registered_url == SHARD_URL.rstrip("/"):
                        logger.info("Shard %s is already registered", SHARD_ID)
                        return
                response.raise_for_status()

            logger.info("Registered shard %s", SHARD_ID)
            return

        except httpx.HTTPError as exc:
            logger.warning("Shard registration failed: %s", exc)
            await asyncio.sleep(REGISTRATION_RETRY_SECONDS)


def create_app() -> FastAPI:
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        registration_task = asyncio.create_task(register_shard())
        try:
            yield
        finally:
            registration_task.cancel()
            await asyncio.gather(registration_task, return_exceptions=True)

    app = FastAPI(
        title=f"Key-Value Shard: {SHARD_ID}",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.storage = InMemoryShardStorage()
    app.include_router(router)

    @app.get("/health")
    def health():
        return {"status": "ok", "shardId": SHARD_ID}

    return app


app = create_app()
