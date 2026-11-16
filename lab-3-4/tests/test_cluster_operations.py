from collections import Counter
from dataclasses import replace

from coordinator.app.services.health_checker import ReplicaHealthChecker
from coordinator.app.sharding.shard_registry import MIN_SHARDS

TABLE = {"tableName": "orders", "partitionKey": "customerId"}
KEY_COUNT = 60


def coordinator_service(cluster):
    return cluster.coordinator.app.state.coordinator


def fill(cluster) -> None:
    cluster.coordinator.post("/tables", json=TABLE)
    for index in range(KEY_COUNT):
        response = cluster.coordinator.put(
            f"/tables/orders/items/customer-{index}", json={"value": {"n": index}}
        )
        assert response.status_code == 201


def test_health_checker_marks_replica_down_and_up(replicated_cluster) -> None:
    cluster = replicated_cluster()
    service = coordinator_service(cluster)
    checker = ReplicaHealthChecker(service.shard_registry, service.settings)

    cluster.offline.add("shard-1-b")
    checker.check_once()
    health = cluster.coordinator.get("/health").json()
    metrics = cluster.coordinator.get("/metrics").text

    assert health["status"] == "degraded"
    assert health["healthyReplicas"] == health["registeredReplicas"] - 1
    assert 'kv_replica_up{replica="shard-1-b",role="follower",shard="shard-1"} 0.0' in (
        metrics
    )

    cluster.offline.clear()
    checker.check_once()
    assert cluster.coordinator.get("/health").json()["status"] == "ok"


def test_reads_skip_a_replica_found_down_by_health_check(replicated_cluster) -> None:
    cluster = replicated_cluster()
    service = coordinator_service(cluster)
    cluster.offline.add("shard-1-b")
    ReplicaHealthChecker(service.shard_registry, service.settings).check_once()
    cluster.offline.clear()  # back online, but the registry still says "down"

    _, replicas = service.shard_registry.select_read_replicas("", "shard-1")

    assert replicas[-1].replica_id == "shard-1-b"


def test_prometheus_targets_list_every_replica(replicated_cluster) -> None:
    cluster = replicated_cluster()

    targets = cluster.coordinator.get("/internal/prometheus-targets").json()

    assert len(targets) == 6
    assert {
        "targets": ["shard-1-b:8000"],
        "labels": {"shard": "shard-1", "replica": "shard-1-b", "role": "follower"},
    } in targets


def test_rebalancing_moves_keys_to_the_new_shard(cluster) -> None:
    cluster.register_all()
    fill(cluster)
    cluster.add_shard("shard-3")
    service = coordinator_service(cluster)

    status = service.rebalancer.run("shard-3")

    per_shard = Counter()
    for shard_id in ("shard-1", "shard-2", "shard-3"):
        for record in cluster.shard_records(shard_id):
            per_shard[shard_id] += 1
            assert service.shard_registry.owner(record["partitionKey"]) == shard_id
    assert status.state == "done"
    assert status.moved == per_shard["shard-3"] > 0
    assert sum(per_shard.values()) == KEY_COUNT  # nothing lost, nothing copied twice
    for index in (0, 30, 59):
        read = cluster.coordinator.get(f"/tables/orders/items/customer-{index}")
        assert read.json()["records"][0]["value"] == {"n": index}


def test_moved_record_keeps_its_timestamp(cluster) -> None:
    cluster.register_all()
    fill(cluster)
    before = {
        record["partitionKey"]: record["timestamp"]
        for shard_id in ("shard-1", "shard-2")
        for record in cluster.shard_records(shard_id)
    }
    cluster.add_shard("shard-3")

    coordinator_service(cluster).rebalancer.run("shard-3")

    for record in cluster.shard_records("shard-3"):
        assert record["timestamp"] == before[record["partitionKey"]]


def test_reads_fall_back_to_the_old_shard_during_migration(cluster) -> None:
    cluster.register_all()
    fill(cluster)
    cluster.add_shard("shard-3")
    service = coordinator_service(cluster)
    moved_key = next(
        f"customer-{index}"
        for index in range(KEY_COUNT)
        if service.shard_registry.owner(f"customer-{index}") == "shard-3"
    )
    url = f"/tables/orders/items/{moved_key}"

    assert cluster.coordinator.get(url).json()["records"] == []  # not moved yet
    # What run() sets while it is moving records:
    service.rebalancer._previous_ring = service.shard_registry.ring_without("shard-3")

    assert len(cluster.coordinator.get(url).json()["records"]) == 1


def test_new_shard_triggers_rebalancing_only_on_a_working_ring(cluster) -> None:
    service = coordinator_service(cluster)
    scheduled: list[str] = []
    service.rebalancer.schedule = scheduled.append
    service.settings = replace(service.settings, rebalance_enabled=True)

    cluster.register_all()  # the first MIN_SHARDS shards just form the ring
    cluster.add_shard("shard-3")

    assert service.shard_registry.shard_count() == MIN_SHARDS + 1
    assert scheduled == ["shard-3"]
