"""Kafka follower logic with a fake topic (no broker needed)."""

from pathlib import Path

from fake_kafka import FakeMessage, FakeTopic
from fastapi.testclient import TestClient

from app.core.config import FOLLOWER, ReplicaSettings
from app.main import create_app
from app.replication.follower import KafkaFollowerReplicator
from app.replication.kafka_log import to_entry
from app.replication.node import ReplicaNode


def put_op(sort_key: str, total: int) -> dict:
    return {
        "op": "put",
        "table": "orders",
        "partitionKey": "c1",
        "sortKey": sort_key,
        "value": {"total": total},
    }


def follower_settings(data_dir: Path | None = None) -> ReplicaSettings:
    return ReplicaSettings(
        shard_id="shard-1",
        replica_id="shard-1-b",
        role=FOLLOWER,
        leader_url="http://shard-1:8000",
        data_dir=data_dir,
        kafka_bootstrap="kafka:9092",
    )


def test_kafka_offset_becomes_one_based_log_offset() -> None:
    entry = to_entry(FakeMessage(0, put_op("o1", 10)))

    assert entry["offset"] == 1
    assert entry["value"] == {"total": 10}


def test_follower_applies_topic_and_acks_leader() -> None:
    leader = TestClient(create_app(ReplicaSettings(), FakeTopic()))
    follower = ReplicaNode(follower_settings())
    topic = FakeTopic([put_op("o1", 10), put_op("o2", 20), put_op("o1", 11)])
    consumer = topic.consumer()

    replicator = KafkaFollowerReplicator(follower, consumer, ack_client=leader)

    assert consumer.position == 0  # nothing applied yet: read from the start
    assert replicator.sync_once(timeout=0) == 3
    assert follower.applied_offset == 3
    assert follower.status()["lag"] == 0
    # The follower reported its offset; the leader counts it for the quorum.
    assert leader.app.state.node.log.follower_progress() == {"shard-1-b": 3}


def test_follower_seeks_to_saved_offset_after_restart(tmp_path: Path) -> None:
    topic = FakeTopic([put_op("o1", 10), put_op("o2", 20)])
    first = ReplicaNode(follower_settings(tmp_path))
    KafkaFollowerReplicator(first, topic.consumer(), None).sync_once(0)

    topic.append(put_op("o3", 30))  # written while the follower is down
    restarted = ReplicaNode(follower_settings(tmp_path))
    consumer = topic.consumer()
    replicator = KafkaFollowerReplicator(restarted, consumer, None)

    assert restarted.applied_offset == 2  # restored from its own log copy
    assert consumer.position == 2  # Kafka offset of the 3rd message
    assert replicator.sync_once(0) == 1
    assert (tmp_path / "offset").read_text() == "3"


def test_leader_collects_acks_only_on_leader() -> None:
    leader = TestClient(create_app(ReplicaSettings(), FakeTopic()))
    follower = TestClient(
        create_app(
            ReplicaSettings(
                replica_id="shard-1-b", role=FOLLOWER, leader_url="http://shard-1:8000"
            )
        )
    )
    ack = {"replicaId": "shard-1-b", "appliedOffset": 5}

    assert leader.post("/internal/ack", json=ack).status_code == 200
    assert follower.post("/internal/ack", json=ack).status_code == 409
