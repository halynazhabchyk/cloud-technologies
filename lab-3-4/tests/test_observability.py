import json
import logging

from fastapi.testclient import TestClient

from common.json_logging import JsonFormatter
from common.trace import TRACE_HEADER, use_trace_id
from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.main import create_app

TABLE = {"tableName": "orders", "partitionKey": "customerId", "sortKey": "orderId"}
URL = "/tables/orders/items/c1?sortKey=o1"


def test_coordinator_creates_or_reuses_trace_id() -> None:
    client = TestClient(create_app(CoordinatorSettings()))

    generated = client.get("/tables").headers[TRACE_HEADER]
    reused = client.get("/tables", headers={TRACE_HEADER: "demo-trace-1"})

    assert len(generated) == 32
    assert reused.headers[TRACE_HEADER] == "demo-trace-1"


def test_invalid_trace_id_is_replaced() -> None:
    client = TestClient(create_app(CoordinatorSettings()))

    response = client.get("/tables", headers={TRACE_HEADER: "bad id\nwith newline"})

    assert response.headers[TRACE_HEADER] != "bad id\nwith newline"


def test_trace_id_reaches_the_replication_log(cluster) -> None:
    """The coordinator passes the trace ID on; the leader stores it in the log."""
    cluster.register_all()
    cluster.coordinator.post("/tables", json=TABLE)

    written = cluster.coordinator.put(
        URL, json={"value": {"total": 1}}, headers={TRACE_HEADER: "trace-put-1"}
    )

    topic = cluster.topics[written.json()["shardId"]]
    assert topic.read_all()[-1]["traceId"] == "trace-put-1"


def test_errors_use_common_format_with_trace_id() -> None:
    client = TestClient(create_app(CoordinatorSettings()))

    missing = client.get("/tables/nope", headers={TRACE_HEADER: "trace-404"})
    invalid = client.post("/tables", json={})

    assert missing.status_code == 404
    assert missing.json() == {
        "error": "Table 'nope' does not exist",
        "trace_id": "trace-404",
    }
    assert invalid.status_code == 422
    assert invalid.json()["error"] == "Request validation failed"
    assert invalid.json()["trace_id"] == invalid.headers[TRACE_HEADER]


def test_json_log_line_contains_trace_id() -> None:
    formatter = JsonFormatter("coordinator")
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", None, None)

    with use_trace_id("trace-log-1"):
        line = json.loads(formatter.format(record))

    assert line["msg"] == "hello"
    assert line["service"] == "coordinator"
    assert line["trace_id"] == "trace-log-1"


def test_metrics_use_route_templates_and_count_errors(cluster) -> None:
    cluster.register_all()
    cluster.coordinator.post("/tables", json=TABLE)
    cluster.coordinator.put(URL, json={"value": {"total": 1}})
    cluster.coordinator.get("/tables/orders/items/c1?sortKey=missing")

    metrics = cluster.coordinator.get("/metrics").text

    route = "/tables/{tableName}/items/{partitionKey}"
    assert (
        f'kv_requests_total{{method="PUT",route="{route}",'
        'service="coordinator",status="201"}'
    ) in metrics  # the registry is shared by all tests: check the series only
    assert "/tables/orders/items/c1" not in metrics  # no per-key series
    assert 'status="404"' in metrics
    assert "kv_request_duration_seconds_bucket" in metrics
    assert 'kv_replica_up{replica="shard-1",role="leader",shard="shard-1"} 1.0' in (
        metrics
    )


def test_replica_metrics_show_keys_and_lag(replicated_cluster) -> None:
    cluster = replicated_cluster()
    cluster.coordinator.post("/tables", json=TABLE)
    shard_id = cluster.coordinator.put(URL, json={"value": {"total": 1}}).json()[
        "shardId"
    ]
    cluster.sync(f"{shard_id}-b")  # one follower acks, the other is behind

    metrics = cluster.shards[shard_id].get("/metrics").text

    leader_labels = f'replica="{shard_id}",role="leader",shard="{shard_id}"'
    assert f"kv_keys_per_shard{{{leader_labels}}} 1.0" in metrics
    lag = f'kv_replication_lag_entries{{follower="{shard_id}-b",shard="{shard_id}"}}'
    assert f"{lag} 0.0" in metrics


def test_version_endpoint_reports_build(monkeypatch) -> None:
    monkeypatch.setenv("GIT_SHA", "abc1234")
    client = TestClient(create_app(CoordinatorSettings()))

    assert client.get("/version").json() == {
        "service": "coordinator",
        "version": "4.0.0",
        "gitSha": "abc1234",
        "buildTime": "unknown",
    }
