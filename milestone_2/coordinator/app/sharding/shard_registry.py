from threading import RLock

from coordinator.app.sharding.consistent_hashing import (
    DEFAULT_VIRTUAL_NODES_PER_SHARD,
    ConsistentHashRing,
)

MIN_SHARDS = 2


class NoAvailableShardsError(RuntimeError):
    pass


class ShardRegistry:
    """Tracks shard endpoints and delegates placement to the hash ring."""

    def __init__(
        self,
        virtual_nodes_per_shard: int = DEFAULT_VIRTUAL_NODES_PER_SHARD,
    ):
        self._shards: dict[str, str] = {}
        self._ring = ConsistentHashRing(virtual_nodes_per_shard)
        self._lock = RLock()

    def register(self, shard_id: str, base_url: str) -> bool:
        normalized_url = base_url.rstrip("/")
        with self._lock:
            current_url = self._shards.get(shard_id)
            if current_url is not None:
                if current_url != normalized_url:
                    raise ValueError(
                        "Shard identifier is already registered with another URL"
                    )
                return False

            self._shards[shard_id] = normalized_url
            self._ring.add_node(shard_id)
            return True

    def list_shards(self) -> list[dict[str, str]]:
        with self._lock:
            return [
                {"shardId": shard_id, "baseUrl": self._shards[shard_id]}
                for shard_id in sorted(self._shards)
            ]

    def select_shard(self, partition_key: str) -> dict[str, str]:
        with self._lock:
            if len(self._shards) < MIN_SHARDS:
                raise NoAvailableShardsError(
                    f"At least {MIN_SHARDS} shards must be registered"
                )

            shard_id = self._ring.get_node(partition_key)
            return {"shardId": shard_id, "baseUrl": self._shards[shard_id]}
