import httpx
import pytest

from coordinator.app.services.coordinator import (
    Coordinator,
    ShardUnavailableError,
)
from coordinator.app.sharding.consistent_hashing import ConsistentHashRing
from coordinator.app.sharding.measure_resharding import measure
from coordinator.app.sharding.shard_registry import (
    NoAvailableShardsError,
    ShardRegistry,
)
from coordinator.app.storage.local_storage import LocalStorage
from coordinator.app.storage.storage import RecordKey


def test_consistent_hashing_is_stable_for_same_ring() -> None:
    ring = ConsistentHashRing()
    ring.add_node("shard-1")
    ring.add_node("shard-2")

    first = [ring.get_node(f"partition-{index}") for index in range(100)]
    second = [ring.get_node(f"partition-{index}") for index in range(100)]

    assert first == second


def test_resharding_reports_changed_assignments() -> None:
    before, after, moved = measure(10_000)

    assert before == {"shard-1": 5106, "shard-2": 4894, "shard-3": 0}
    assert after == {"shard-1": 3377, "shard-2": 2963, "shard-3": 3660}
    assert moved == 3660


def test_registry_requires_two_shards_and_routes_after_registration() -> None:
    registry = ShardRegistry()

    registry.register("shard-1", "http://shard-1:8000/")
    try:
        registry.select_shard("partition-1")
    except NoAvailableShardsError:
        pass
    else:
        raise AssertionError("A single shard must not accept routed requests")

    registry.register("shard-2", "http://shard-2:8000")
    selected = registry.select_shard("partition-1")

    assert selected["shardId"] in {"shard-1", "shard-2"}


def test_coordinator_rejects_malformed_shard_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeClient:
        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def request(self, *_args: object, **_kwargs: object) -> httpx.Response:
            return httpx.Response(200, json={"created": "yes"})

    registry = ShardRegistry()
    registry.register("shard-1", "http://shard-1:8000")
    registry.register("shard-2", "http://shard-2:8000")
    coordinator = Coordinator(
        LocalStorage(),
        shard_registry=registry,
        sharding_enabled=True,
    )
    coordinator.create_table("events", "userId")
    monkeypatch.setattr(
        "coordinator.app.services.coordinator.httpx.Client",
        lambda **_kwargs: FakeClient(),
    )

    with pytest.raises(ShardUnavailableError):
        coordinator.create_record(
            "events",
            key=RecordKey("user-1"),
            value={"name": "Alex"},
        )
