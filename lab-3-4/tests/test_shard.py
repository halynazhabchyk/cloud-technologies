import asyncio
import json

import httpx
from fake_kafka import FakeTopic
from fastapi.testclient import TestClient

import app.registration as registration
from app.core.config import ReplicaSettings
from app.main import create_app

BASE = "/internal/tables/orders"


def test_shard_put_get_exists_delete() -> None:
    client = TestClient(create_app(ReplicaSettings(), FakeTopic()))
    params = {"partitionKey": "c1", "sortKey": "o1"}

    created = client.put(
        f"{BASE}/records", params={**params, "timestamp": 100}, json={"value": {"n": 1}}
    )
    replaced = client.put(
        f"{BASE}/records", params={**params, "timestamp": 200}, json={"value": {"n": 2}}
    )

    assert created.json() == {"created": True, "version": 1, "timestamp": 100}
    assert replaced.json() == {"created": False, "version": 2, "timestamp": 200}
    assert client.get(f"{BASE}/record", params=params).json() == {
        "sortKey": "o1",
        "value": {"n": 2},
        "version": 2,
        "timestamp": 200,
    }
    assert client.head(f"{BASE}/records", params=params).status_code == 200
    assert client.delete(
        f"{BASE}/records", params={**params, "timestamp": 300}
    ).json() == {"deleted": True, "version": 3, "timestamp": 300}
    assert client.get(f"{BASE}/record", params=params).status_code == 404
    assert client.head(f"{BASE}/records", params=params).status_code == 404
    assert client.delete(f"{BASE}/records", params=params).status_code == 404


def test_shard_lists_partition_sorted_by_sort_key() -> None:
    client = TestClient(create_app(ReplicaSettings(), FakeTopic()))
    for sort_key in ("o2", "o1"):
        client.put(
            f"{BASE}/records",
            params={"partitionKey": "c1", "sortKey": sort_key},
            json={"value": {"id": sort_key}},
        )
    client.put(
        f"{BASE}/records",
        params={"partitionKey": "c2", "sortKey": "o9"},
        json={"value": {"id": "o9"}},
    )

    partition = client.get(f"{BASE}/records", params={"partitionKey": "c1"}).json()

    assert [record["sortKey"] for record in partition] == ["o1", "o2"]
    assert len(client.get("/internal/records").json()) == 3


def test_shard_operations_work_without_sort_key() -> None:
    client = TestClient(create_app(ReplicaSettings(), FakeTopic()))
    params = {"partitionKey": "u1"}

    client.put(f"{BASE}/records", params=params, json={"value": {"n": 1}})

    assert client.head(f"{BASE}/records", params=params).status_code == 200
    assert client.get(f"{BASE}/record", params=params).json()["sortKey"] is None
    assert client.delete(f"{BASE}/records", params=params).status_code == 200


def test_shard_re_registers_after_coordinator_restart(monkeypatch) -> None:
    settings = ReplicaSettings(shard_id="shard-1", replica_id="shard-1-a")
    registry: dict[str, dict] = {}
    posts: list[str] = []

    def coordinator_api(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            posts.append(body["replicaId"])
            if body["replicaId"] in registry:
                return httpx.Response(409, json={"detail": "exists"})
            registry[body["replicaId"]] = body
            return httpx.Response(201, json=body)
        shards = [{"shardId": "shard-1", "replicas": list(registry.values())}]
        return httpx.Response(200, json=shards)

    class StopLoop(Exception):
        pass

    sleeps = 0

    async def fake_sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 1:
            registry.clear()  # coordinator restarted and forgot every replica
        if sleeps == 3:
            raise StopLoop

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        registration.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(
            transport=httpx.MockTransport(coordinator_api), **kwargs
        ),
    )
    monkeypatch.setattr(registration.asyncio, "sleep", fake_sleep)

    try:
        asyncio.run(registration.register_replica(settings))
    except StopLoop:
        pass

    assert posts == ["shard-1-a", "shard-1-a"]
    assert registry["shard-1-a"]["role"] == "leader"
