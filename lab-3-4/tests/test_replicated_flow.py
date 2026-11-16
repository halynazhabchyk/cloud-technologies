import pytest

from coordinator.app.core.config import CoordinatorSettings


def create_orders(cluster) -> str:
    cluster.coordinator.post(
        "/tables",
        json={
            "tableName": "orders",
            "partitionKey": "customerId",
            "sortKey": "orderId",
        },
    )
    response = cluster.coordinator.put(
        "/tables/orders/items/c1?sortKey=o1", json={"value": {"total": 10}}
    )
    assert response.status_code == 201
    return response.json()["shardId"]


def replicas_of(shard_id: str) -> set[str]:
    return {shard_id, f"{shard_id}-b", f"{shard_id}-c"}


def test_shards_list_replica_sets(replicated_cluster) -> None:
    cluster = replicated_cluster()

    shards = cluster.coordinator.get("/shards").json()
    health = cluster.coordinator.get("/health").json()

    assert [shard["shardId"] for shard in shards] == ["shard-1", "shard-2"]
    assert [r["role"] for r in shards[0]["replicas"]] == [
        "leader",
        "follower",
        "follower",
    ]
    assert health["registeredShards"] == 2
    assert health["registeredReplicas"] == 6


def test_writes_go_to_leader_and_replicate_to_followers(replicated_cluster) -> None:
    cluster = replicated_cluster()
    shard_id = create_orders(cluster)

    assert cluster.replica_records(f"{shard_id}-b") == []
    assert cluster.sync(f"{shard_id}-b") == 1

    assert cluster.replica_records(f"{shard_id}-b") == cluster.shard_records(shard_id)


def test_reads_are_spread_over_all_live_replicas(replicated_cluster) -> None:
    cluster = replicated_cluster()
    shard_id = create_orders(cluster)
    for follower in (f"{shard_id}-b", f"{shard_id}-c"):
        cluster.sync(follower)

    served_by = {
        cluster.coordinator.get("/tables/orders/items/c1?sortKey=o1").json()[
            "replicaId"
        ]
        for _ in range(3)
    }

    assert served_by == replicas_of(shard_id)


def test_single_replica_reads_can_be_stale(replicated_cluster) -> None:
    # W = 1, R = 1, N = 3: R + W <= N, so a read may miss the latest write.
    cluster = replicated_cluster(write_quorum=1, read_quorum=1)
    create_orders(cluster)  # followers have not pulled the log yet

    statuses = [
        cluster.coordinator.get("/tables/orders/items/c1?sortKey=o1").status_code
        for _ in range(3)
    ]

    assert sorted(statuses) == [200, 404, 404]


def test_quorum_read_returns_freshest_replica(replicated_cluster) -> None:
    cluster = replicated_cluster(read_quorum=2)
    shard_id = create_orders(cluster)
    cluster.sync(f"{shard_id}-b")

    for _ in range(6):  # every rotation of the replica order
        response = cluster.coordinator.get("/tables/orders/items/c1?sortKey=o1")
        assert response.status_code == 200
        assert response.json()["replicaId"] in {shard_id, f"{shard_id}-b"}
        assert (
            cluster.coordinator.head("/tables/orders/items/c1?sortKey=o1").status_code
            == 200
        )


def test_reads_fail_over_to_live_replicas(replicated_cluster) -> None:
    cluster = replicated_cluster()
    shard_id = create_orders(cluster)
    for follower in (f"{shard_id}-b", f"{shard_id}-c"):
        cluster.sync(follower)
    cluster.offline.add(f"{shard_id}-b")

    served_by = {
        cluster.coordinator.get("/tables/orders/items/c1?sortKey=o1").json()[
            "replicaId"
        ]
        for _ in range(6)
    }

    assert served_by == {shard_id, f"{shard_id}-c"}
    replicas = next(
        s["replicas"]
        for s in cluster.coordinator.get("/shards").json()
        if s["shardId"] == shard_id
    )
    assert {r["replicaId"]: r["available"] for r in replicas}[f"{shard_id}-b"] is False


def test_read_quorum_not_reached_returns_503(replicated_cluster) -> None:
    cluster = replicated_cluster(read_quorum=2)
    shard_id = create_orders(cluster)
    cluster.offline.update({f"{shard_id}-b", f"{shard_id}-c"})

    response = cluster.coordinator.get("/tables/orders/items/c1?sortKey=o1")

    assert response.status_code == 503
    assert "Read quorum not reached" in response.json()["error"]


def test_writes_fail_when_leader_is_down(replicated_cluster) -> None:
    cluster = replicated_cluster()
    shard_id = create_orders(cluster)
    cluster.offline.add(shard_id)

    response = cluster.coordinator.put(
        "/tables/orders/items/c1?sortKey=o2", json={"value": {"total": 20}}
    )

    assert response.status_code == 503


def test_second_leader_for_a_shard_is_rejected(replicated_cluster) -> None:
    cluster = replicated_cluster()

    response = cluster.coordinator.post(
        "/shards",
        json={
            "shardId": "shard-1",
            "replicaId": "shard-1-x",
            "role": "leader",
            "baseUrl": "http://shard-1-x:8000",
        },
    )

    assert response.status_code == 409


@pytest.mark.parametrize("write_quorum, read_quorum", [(0, 1), (4, 1), (1, 4)])
def test_quorums_must_fit_replication_factor(write_quorum, read_quorum) -> None:
    with pytest.raises(ValueError):
        CoordinatorSettings(
            replication_factor=3,
            write_quorum=write_quorum,
            read_quorum=read_quorum,
        )
