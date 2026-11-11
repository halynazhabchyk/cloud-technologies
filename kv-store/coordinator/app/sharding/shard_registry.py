import time
from dataclasses import dataclass
from itertools import count
from threading import RLock

from coordinator.app.sharding.consistent_hashing import (
    DEFAULT_VIRTUAL_NODES_PER_SHARD,
    ConsistentHashRing,
)

MIN_SHARDS = 2
LEADER = "leader"
FOLLOWER = "follower"


class NoAvailableShardsError(RuntimeError):
    pass


@dataclass
class Replica:
    replica_id: str
    base_url: str
    role: str
    # Monotonic time until which the replica is skipped after a failed request.
    unavailable_until: float = 0.0

    def is_available(self, now: float) -> bool:
        return now >= self.unavailable_until

    def as_dict(self, now: float) -> dict[str, object]:
        return {
            "replicaId": self.replica_id,
            "baseUrl": self.base_url,
            "role": self.role,
            "available": self.is_available(now),
        }


class ShardRegistry:
    """Tracks each shard's replica set and delegates placement to the hash ring.

    The ring maps a partition key to a shard; inside the shard writes go to
    the leader and reads are spread over all available replicas.
    """

    def __init__(
        self,
        virtual_nodes_per_shard: int = DEFAULT_VIRTUAL_NODES_PER_SHARD,
    ):
        self._shards: dict[str, dict[str, Replica]] = {}
        self._read_turns: dict[str, count] = {}
        self._ring = ConsistentHashRing(virtual_nodes_per_shard)
        self._lock = RLock()

    def register(
        self,
        shard_id: str,
        base_url: str,
        replica_id: str | None = None,
        role: str = LEADER,
    ) -> bool:
        """Register a replica; return False if it is already registered."""
        replica_id = replica_id or shard_id
        normalized_url = base_url.rstrip("/")
        if role not in (LEADER, FOLLOWER):
            raise ValueError(f"Unknown replica role '{role}'")

        with self._lock:
            replicas = self._shards.get(shard_id, {})
            current = replicas.get(replica_id)
            if current is not None:
                if current.base_url != normalized_url or current.role != role:
                    raise ValueError(
                        "Replica identifier is already registered with another "
                        "URL or role"
                    )
                current.unavailable_until = 0.0  # it announced itself: alive
                return False

            if role == LEADER and self._leader(replicas) is not None:
                raise ValueError(f"Shard '{shard_id}' already has a leader")

            replicas[replica_id] = Replica(replica_id, normalized_url, role)
            self._shards[shard_id] = replicas
            self._read_turns.setdefault(shard_id, count())
            if role == LEADER:
                # Only shards that can accept writes take part in routing.
                self._ring.add_node(shard_id)
            return True

    @staticmethod
    def _leader(replicas: dict[str, Replica]) -> Replica | None:
        return next((r for r in replicas.values() if r.role == LEADER), None)

    def _routable_count(self) -> int:
        return sum(1 for r in self._shards.values() if self._leader(r) is not None)

    def list_shards(self) -> list[dict[str, object]]:
        now = time.monotonic()
        with self._lock:
            result = []
            for shard_id in sorted(self._shards):
                replicas = self._shards[shard_id]
                leader = self._leader(replicas)
                result.append(
                    {
                        "shardId": shard_id,
                        "baseUrl": leader.base_url if leader else None,
                        "replicas": [
                            replicas[replica_id].as_dict(now)
                            for replica_id in sorted(replicas)
                        ],
                    }
                )
            return result

    def shard_count(self) -> int:
        with self._lock:
            return self._routable_count()

    def replica_count(self) -> int:
        with self._lock:
            return sum(len(replicas) for replicas in self._shards.values())

    def _owner(self, partition_key: str) -> str:
        if self._routable_count() < MIN_SHARDS:
            raise NoAvailableShardsError(
                f"At least {MIN_SHARDS} shards must be registered"
            )
        return self._ring.get_node(partition_key)

    def select_shard(self, partition_key: str) -> dict[str, str]:
        """Shard owning the key and its leader URL (writes go to the leader)."""
        with self._lock:
            shard_id = self._owner(partition_key)
            leader = self._leader(self._shards[shard_id])
            assert leader is not None
            return {
                "shardId": shard_id,
                "baseUrl": leader.base_url,
                "replicaId": leader.replica_id,
            }

    def select_read_replicas(self, partition_key: str) -> tuple[str, list[Replica]]:
        """Shard owning the key and its replicas, leader first, then available."""
        now = time.monotonic()
        with self._lock:
            shard_id = self._owner(partition_key)
            replicas = sorted(
                self._shards[shard_id].values(), key=lambda r: r.replica_id
            )
            turn = next(self._read_turns[shard_id]) % len(replicas)
            rotated = replicas[turn:] + replicas[:turn]
            available = [r for r in rotated if r.is_available(now)]
            unavailable = [r for r in rotated if not r.is_available(now)]
            return shard_id, available + unavailable

    def mark_unavailable(self, shard_id: str, replica_id: str, seconds: float) -> None:
        with self._lock:
            replica = self._shards.get(shard_id, {}).get(replica_id)
            if replica is not None:
                replica.unavailable_until = time.monotonic() + seconds
