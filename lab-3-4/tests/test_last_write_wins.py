from app.storage import Version
from app.storage.versioning import is_newer

TABLE = {"tableName": "orders", "partitionKey": "customerId", "sortKey": "orderId"}
URL = "/tables/orders/items/c1?sortKey=o1"
STAMP = "X-Write-Timestamp"


def test_is_newer_compares_timestamp_then_offset() -> None:
    assert is_newer(Version(200, 1), None)
    assert is_newer(Version(200, 1), Version(100, 5))
    assert not is_newer(Version(100, 9), Version(200, 1))
    # Equal timestamps: the later log entry (bigger offset) wins.
    assert is_newer(Version(100, 2), Version(100, 1))


def test_older_write_loses_and_newer_value_stays(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post("/tables", json=TABLE)

    newer = cluster.coordinator.put(
        URL, json={"value": {"total": 2}}, headers={STAMP: "2000"}
    )
    older = cluster.coordinator.put(
        URL, json={"value": {"total": 1}}, headers={STAMP: "1000"}
    )

    assert newer.status_code == 201
    assert newer.json()["timestamp"] == 2000
    assert older.status_code == 409
    assert "newer write already exists" in older.json()["error"]
    assert cluster.coordinator.get(URL).json()["value"] == {"total": 2}


def test_tombstone_stops_an_older_write_from_resurrecting_a_key(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post("/tables", json=TABLE)
    cluster.coordinator.put(URL, json={"value": {"total": 1}}, headers={STAMP: "1000"})

    deleted = cluster.coordinator.delete(URL, headers={STAMP: "3000"})
    delayed = cluster.coordinator.put(
        URL, json={"value": {"total": 2}}, headers={STAMP: "2000"}
    )

    assert deleted.status_code == 204
    assert delayed.status_code == 409
    assert cluster.coordinator.get(URL).status_code == 404


def test_coordinator_stamps_writes_with_increasing_timestamps(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post("/tables", json=TABLE)

    first = cluster.coordinator.put(URL, json={"value": {"total": 1}}).json()
    second = cluster.coordinator.put(URL, json={"value": {"total": 2}}).json()

    assert second["timestamp"] > first["timestamp"] > 1_600_000_000_000_000


def test_followers_reach_the_same_lww_decision(replicated_cluster) -> None:
    cluster = replicated_cluster()
    cluster.coordinator.post("/tables", json=TABLE)
    shard_id = cluster.coordinator.put(
        URL, json={"value": {"total": 2}}, headers={STAMP: "2000"}
    ).json()["shardId"]
    cluster.coordinator.delete(URL, headers={STAMP: "3000"})

    for suffix in ("b", "c"):
        cluster.sync(f"{shard_id}-{suffix}")

    for replica_id in (shard_id, f"{shard_id}-b", f"{shard_id}-c"):
        assert cluster.replica_records(replica_id) == []
