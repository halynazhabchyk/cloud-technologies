from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

import httpx
import pytest
from fake_kafka import FakeTopic
from fastapi.testclient import TestClient

from app.core.config import FOLLOWER, LEADER, ReplicaSettings
from app.main import create_app as create_shard_app
from app.replication.follower import KafkaFollowerReplicator
from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.main import create_app as create_coordinator_app

SHARD_IDS = ("shard-1", "shard-2")
SHARD_URLS = {shard_id: f"http://{shard_id}:8000" for shard_id in SHARD_IDS}
FOLLOWER_SUFFIXES = ("b", "c")


@dataclass
class Node:
    shard_id: str
    replica_id: str
    role: str
    base_url: str
    client: TestClient


@dataclass
class ShardedCluster:
    """Coordinator plus in-process replicas wired through a fake HTTP client."""

    coordinator: TestClient
    nodes: dict[str, Node] = field(default_factory=dict)
    topics: dict[str, FakeTopic] = field(default_factory=dict)
    offline: set[str] = field(default_factory=set)

    @property
    def shards(self) -> dict[str, TestClient]:
        """Leader of every shard, by shard id."""
        return {n.shard_id: n.client for n in self.nodes.values() if n.role == LEADER}

    def register_all(self) -> None:
        for node in self.nodes.values():
            response = self.coordinator.post(
                "/shards",
                json={
                    "shardId": node.shard_id,
                    "replicaId": node.replica_id,
                    "role": node.role,
                    "baseUrl": node.base_url,
                },
            )
            assert response.status_code == 201

    def shard_records(self, shard_id: str) -> list[dict]:
        return self.shards[shard_id].get("/internal/records").json()

    def replica_records(self, replica_id: str) -> list[dict]:
        return self.nodes[replica_id].client.get("/internal/records").json()

    def sync(self, replica_id: str) -> int:
        """Let one follower consume its shard topic once and ack the leader."""
        node = self.nodes[replica_id]
        replicator = KafkaFollowerReplicator(
            node.client.app.state.node,
            self.topics[node.shard_id].consumer(),
            ack_client=self.shards[node.shard_id],
        )
        return replicator.sync_once(timeout=0)

    def add_shard(self, shard_id: str) -> Node:
        """Start a new single-replica shard and register it (rebalancing demo)."""
        topic = FakeTopic()
        node = _node(shard_id, shard_id, LEADER, topic)
        self.nodes[shard_id] = node
        self.topics[shard_id] = topic
        response = self.coordinator.post(
            "/shards",
            json={"shardId": shard_id, "role": LEADER, "baseUrl": node.base_url},
        )
        assert response.status_code == 201
        return node

    def partition_key_for_each_shard(self) -> dict[str, str]:
        """Find one sample partition key routed to each registered shard."""
        registry = self.coordinator.app.state.coordinator.shard_registry
        keys: dict[str, str] = {}
        for index in range(1_000):
            key = f"partition-{index}"
            shard_id, _leader = registry.select_leader(key)
            keys.setdefault(shard_id, key)
            if len(keys) == len(SHARD_IDS):
                return keys
        raise AssertionError("Sample keys did not cover every shard")


def _node(
    shard_id: str, replica_id: str, role: str, topic: FakeTopic | None = None
) -> Node:
    base_url = f"http://{replica_id}:8000"
    settings = ReplicaSettings(
        shard_id=shard_id,
        replica_id=replica_id,
        role=role,
        base_url=base_url,
        leader_url=SHARD_URLS[shard_id] if role == FOLLOWER else None,
        ack_timeout_seconds=0.2,
    )
    return Node(
        shard_id,
        replica_id,
        role,
        base_url,
        TestClient(create_shard_app(settings, topic)),
    )


def _build_cluster(
    monkeypatch: pytest.MonkeyPatch,
    replicated: bool,
    write_quorum: int = 1,
    read_quorum: int = 1,
) -> ShardedCluster:
    settings = CoordinatorSettings(
        sharding_enabled=True,
        replication_factor=1 + len(FOLLOWER_SUFFIXES) if replicated else 1,
        write_quorum=write_quorum,
        read_quorum=read_quorum,
        # Tests run rebalancing synchronously instead of in a background thread.
        rebalance_enabled=False,
    )

    nodes: dict[str, Node] = {}
    topics = {shard_id: FakeTopic() for shard_id in SHARD_IDS}
    for shard_id in SHARD_IDS:
        nodes[shard_id] = _node(shard_id, shard_id, LEADER, topics[shard_id])
        if replicated:
            for suffix in FOLLOWER_SUFFIXES:
                replica_id = f"{shard_id}-{suffix}"
                nodes[replica_id] = _node(shard_id, replica_id, FOLLOWER)

    cluster = ShardedCluster(
        TestClient(create_coordinator_app(settings)), nodes, topics
    )

    class RoutingClient:
        """Stands in for httpx.Client inside the coordinator."""

        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> "RoutingClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def request(self, method: str, url: str, **kwargs) -> httpx.Response:
            for replica_id, node in cluster.nodes.items():
                if (
                    url.startswith(node.base_url + "/")
                    and replica_id not in cluster.offline
                ):
                    path = url[len(node.base_url) :]
                    return node.client.request(method, path, **kwargs)
            raise httpx.ConnectError(f"Replica is unreachable: {url}")

        def get(self, url: str, **kwargs) -> httpx.Response:
            return self.request("GET", url, **kwargs)

    monkeypatch.setattr(
        "coordinator.app.services.shard_client.httpx.Client", RoutingClient
    )
    return cluster


@pytest.fixture
def cluster(monkeypatch: pytest.MonkeyPatch) -> Iterator[ShardedCluster]:
    """Two shards, one replica each (the milestone 2 topology)."""
    yield _build_cluster(monkeypatch, replicated=False)


@pytest.fixture
def replicated_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., ShardedCluster]:
    """Factory: two shards with a leader and two followers each (N = 3)."""

    def build(write_quorum: int = 1, read_quorum: int = 1) -> ShardedCluster:
        cluster = _build_cluster(monkeypatch, True, write_quorum, read_quorum)
        cluster.register_all()
        return cluster

    return build
