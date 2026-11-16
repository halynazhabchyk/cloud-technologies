def test_records_with_same_partition_key_share_a_shard(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post(
        "/tables",
        json={
            "tableName": "orders",
            "partitionKey": "customerId",
            "sortKey": "orderId",
        },
    )

    first = cluster.coordinator.put(
        "/tables/orders/items/c1?sortKey=order-1", json={"value": {"total": 10}}
    )
    second = cluster.coordinator.put(
        "/tables/orders/items/c1?sortKey=order-2", json={"value": {"total": 20}}
    )

    assert first.status_code == second.status_code == 201
    shard_id = first.json()["shardId"]
    assert second.json()["shardId"] == shard_id
    stored = cluster.shard_records(shard_id)
    assert [record["sortKey"] for record in stored] == ["order-1", "order-2"]


def test_different_partition_keys_use_both_shards(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post(
        "/tables", json={"tableName": "users", "partitionKey": "userId"}
    )

    keys = cluster.partition_key_for_each_shard()
    for shard_id, partition_key in keys.items():
        response = cluster.coordinator.put(
            f"/tables/users/items/{partition_key}", json={"value": {"id": 1}}
        )
        assert response.json()["shardId"] == shard_id

    assert all(cluster.shard_records(shard_id) for shard_id in keys)


def test_point_read_exists_and_delete_go_to_owning_shard(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post(
        "/tables",
        json={
            "tableName": "orders",
            "partitionKey": "customerId",
            "sortKey": "orderId",
        },
    )
    url = "/tables/orders/items/c1?sortKey=order-1"

    assert cluster.coordinator.head(url).status_code == 404
    assert cluster.coordinator.get(url).status_code == 404

    written = cluster.coordinator.put(url, json={"value": {"total": 10}})
    read = cluster.coordinator.get(url)

    assert read.status_code == 200
    assert read.json() == {
        "shardId": written.json()["shardId"],
        "replicaId": written.json()["shardId"],
        "sortKey": "order-1",
        "value": {"total": 10},
        "version": written.json()["version"],
        "timestamp": written.json()["timestamp"],
    }
    assert cluster.coordinator.head(url).status_code == 200

    partition = cluster.coordinator.get("/tables/orders/items/c1")
    assert partition.json()["records"] == [
        {
            "sortKey": "order-1",
            "value": {"total": 10},
            "version": 1,
            "timestamp": written.json()["timestamp"],
        }
    ]

    assert cluster.coordinator.delete(url).status_code == 204
    assert cluster.coordinator.get(url).status_code == 404
    assert cluster.coordinator.delete(url).status_code == 404


def test_tables_without_sort_key_support_exists_and_delete(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post(
        "/tables", json={"tableName": "users", "partitionKey": "userId"}
    )

    cluster.coordinator.put("/tables/users/items/u1", json={"value": {"n": 1}})

    assert cluster.coordinator.head("/tables/users/items/u1").status_code == 200
    assert cluster.coordinator.delete("/tables/users/items/u1").status_code == 204
    assert cluster.coordinator.head("/tables/users/items/u1").status_code == 404


def test_requests_fail_with_503_until_two_shards_register(cluster) -> None:
    cluster.coordinator.post(
        "/tables", json={"tableName": "users", "partitionKey": "userId"}
    )
    cluster.coordinator.post(
        "/shards", json={"shardId": "shard-1", "baseUrl": "http://shard-1:8000"}
    )

    response = cluster.coordinator.put(
        "/tables/users/items/u1", json={"value": {"n": 1}}
    )

    assert response.status_code == 503
    assert cluster.coordinator.get("/health").json()["registeredShards"] == 1


def test_unreachable_shard_returns_503(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post(
        "/tables", json={"tableName": "users", "partitionKey": "userId"}
    )
    keys = cluster.partition_key_for_each_shard()
    cluster.offline.add("shard-2")

    down = cluster.coordinator.put(
        f"/tables/users/items/{keys['shard-2']}", json={"value": {"n": 1}}
    )
    up = cluster.coordinator.put(
        f"/tables/users/items/{keys['shard-1']}", json={"value": {"n": 1}}
    )

    assert down.status_code == 503
    assert up.status_code == 201
