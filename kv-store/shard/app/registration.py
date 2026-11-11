"""Self-registration of a replica in the coordinator's shard registry."""

import asyncio
import logging

import httpx

from app.core.config import REGISTRATION_RETRY_SECONDS, ReplicaSettings

logger = logging.getLogger(__name__)


async def is_registered(client: httpx.AsyncClient, settings: ReplicaSettings) -> bool:
    """Check whether the coordinator knows this replica under its own URL."""
    shards_response = await client.get(f"{settings.coordinator_url}/shards")
    shards_response.raise_for_status()
    shard = next(
        (s for s in shards_response.json() if s.get("shardId") == settings.shard_id),
        None,
    )
    replica = next(
        (
            r
            for r in (shard or {}).get("replicas", [])
            if r.get("replicaId") == settings.replica_id
        ),
        None,
    )
    registered_url = replica.get("baseUrl", "").rstrip("/") if replica else None
    return registered_url == settings.base_url.rstrip("/")


async def register_once(client: httpx.AsyncClient, settings: ReplicaSettings) -> None:
    payload = {
        "shardId": settings.shard_id,
        "replicaId": settings.replica_id,
        "role": settings.role,
        "baseUrl": settings.base_url,
    }
    response = await client.post(f"{settings.coordinator_url}/shards", json=payload)
    if response.status_code == 409 and await is_registered(client, settings):
        logger.info("Replica %s is already registered", settings.replica_id)
        return
    response.raise_for_status()
    logger.info("Registered replica %s (%s)", settings.replica_id, settings.role)


async def register_replica(settings: ReplicaSettings) -> None:
    registered = False
    while True:
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                if not registered or not await is_registered(client, settings):
                    if registered:
                        logger.warning(
                            "Coordinator lost replica %s, re-registering",
                            settings.replica_id,
                        )
                    await register_once(client, settings)
                    registered = True
            await asyncio.sleep(settings.registration_check_seconds)

        except httpx.HTTPError as exc:
            logger.warning("Replica registration failed: %s", exc)
            await asyncio.sleep(REGISTRATION_RETRY_SECONDS)
