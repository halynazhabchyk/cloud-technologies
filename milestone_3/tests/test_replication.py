"""Follower log copy, leader/follower replication, recovery and write quorum."""

import threading
from pathlib import Path

import httpx
from fake_kafka import FakeTopic
from fastapi.testclient import TestClient

from app.core.config import FOLLOWER, ReplicaSettings
from app.main import create_app
from app.replication.follower import KafkaFollowerReplicator
from app.replication.log import OffsetStore, ReplicationLog
from app.replication.node import ReplicaNode
from app.storage import RecordKey

LEADER_URL = "http://leader:8000"
RECORDS = "/internal/tables/orders/records"


def follower_settings(data_dir: Path | None = None) -> ReplicaSettings:
    return ReplicaSettings(
        shard_id="shard-1",
        replica_id="shard-1-b",
        role=FOLLOWER,
        base_url="http://shard-1-b:8000",
        leader_url=LEADER_URL,
        data_dir=data_dir,
    )


def leader_with(topic: FakeTopic, **settings) -> TestClient:
    return TestClient(create_app(ReplicaSettings(**settings), topic))


def put(
    client: TestClient, sort_key: str, value: dict, acks: int = 1
) -> httpx.Response:
    return client.put(
        RECORDS,
        params={"partitionKey": "c1", "sortKey": sort_key, "acks": acks},
        json={"value": value},
    )


# --- follower log copy ----------------------------------------------------


def test_log_copy_survives_restart_and_ignores_torn_line(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    log = ReplicationLog(path)
    log.append_replicated({"offset": 1, "op": "put", "value": 1})
    log.append_replicated({"offset": 2, "op": "put", "value": 2})

    with path.open("a", encoding="utf-8") as file:
        file.write('{"offset": 3, "op": "pu')  # crash in the middle of a write

    reopened = ReplicationLog(path)
    assert reopened.last_offset == 2
    assert reopened.entries()[-1]["value"] == 2


def test_log_drops_entries_after_follower_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    log = ReplicationLog(path)
    for offset in range(1, 4):
        log.append_replicated({"offset": offset, "op": "put"})

    reopened = ReplicationLog(path, keep_up_to=1)

    assert reopened.last_offset == 1
    assert ReplicationLog(path).last_offset == 1  # the file was rewritten


def test_log_waits_for_follower_acks() -> None:
    log = ReplicationLog()

    assert log.wait_for_acks(1, followers=1, timeout=0.01) is False

    threading.Timer(0.05, lambda: log.record_ack("f1", 1)).start()
    assert log.wait_for_acks(1, followers=1, timeout=2) is True


def test_offset_store_round_trip(tmp_path: Path) -> None:
    store = OffsetStore(tmp_path / "offset")

    assert store.load() == 0
    store.save(42)
    assert OffsetStore(tmp_path / "offset").load() == 42


# --- leader / follower ----------------------------------------------------


def test_follower_applies_leader_log_in_order() -> None:
    topic = FakeTopic()
    leader = leader_with(topic)
    follower = ReplicaNode(follower_settings())
    replicator = KafkaFollowerReplicator(follower, topic.consumer(), leader)

    put(leader, "o1", {"total": 10})
    put(leader, "o2", {"total": 20})
    put(leader, "o1", {"total": 11})
    leader.delete(RECORDS, params={"partitionKey": "c1", "sortKey": "o2"})

    assert replicator.sync_once(timeout=0) == 4
    assert follower.applied_offset == 4
    assert follower.storage.list_all() == leader.get("/internal/records").json()
    assert follower.storage.list_all()[0]["version"] == 3  # version = log offset

    # Nothing new to deliver; the follower has acknowledged offset 4.
    assert replicator.sync_once(timeout=0) == 0
    assert leader.get("/health").json()["followers"] == {
        "shard-1-b": {"ackedOffset": 4, "lag": 0}
    }


def test_follower_rejects_writes() -> None:
    follower = TestClient(create_app(follower_settings()))

    assert put(follower, "o1", {"total": 10}).status_code == 409


def test_leader_rebuilds_state_from_its_log() -> None:
    topic = FakeTopic()
    put(leader_with(topic), "o1", {"total": 10})

    restarted = leader_with(topic)  # same topic, fresh process

    assert restarted.get("/internal/records").json()[0]["value"] == {"total": 10}
    assert put(restarted, "o2", {"total": 20}).json()["version"] == 2


# --- write quorum ---------------------------------------------------------


def test_write_quorum_fails_without_enough_followers() -> None:
    leader = leader_with(FakeTopic(), ack_timeout_seconds=0.1)

    response = put(leader, "o1", {"total": 10}, acks=2)

    assert response.status_code == 503
    assert "Write quorum not reached" in response.json()["detail"]


def test_write_quorum_waits_for_follower_ack() -> None:
    topic = FakeTopic()
    leader = leader_with(topic, ack_timeout_seconds=5)
    follower = ReplicaNode(follower_settings())
    replicator = KafkaFollowerReplicator(follower, topic.consumer(), leader)
    stop = threading.Event()
    thread = threading.Thread(target=replicator.run, args=(stop,), daemon=True)
    thread.start()
    try:
        response = put(leader, "o1", {"total": 10}, acks=2)
    finally:
        stop.set()
        thread.join(timeout=5)

    assert response.status_code == 200
    assert follower.storage.get("orders", RecordKey("c1", "o1")) == {"total": 10}
