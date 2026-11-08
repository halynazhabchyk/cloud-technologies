from fastapi.testclient import TestClient

from coordinator.app.main import create_app


def test_local_coordinator_supports_composite_key_crud() -> None:
    client = TestClient(create_app())

    response = client.post(
        "/tables",
        json={"tableName": "users", "partitionKey": "userId", "sortKey": "eventId"},
    )
    assert response.status_code == 201

    response = client.put(
        "/tables/users/items/user-1?sortKey=event-1",
        json={"value": {"name": "Alex"}},
    )
    assert response.status_code == 201
    assert response.json()["shardId"] is None

    response = client.head("/tables/users/items/user-1?sortKey=event-1")
    assert response.status_code == 200

    response = client.get("/tables/users/items/user-1")
    assert response.status_code == 200
    assert response.json()["records"] == [
        {"sortKey": "event-1", "value": {"name": "Alex"}}
    ]

    response = client.delete("/tables/users/items/user-1?sortKey=event-1")
    assert response.status_code == 204
    assert client.head("/tables/users/items/user-1?sortKey=event-1").status_code == 404


def test_route_parameters_follow_openapi_limits() -> None:
    client = TestClient(create_app())
    oversized_table_name = "t" * 129

    response = client.get(f"/tables/{oversized_table_name}")

    assert response.status_code == 422


def test_composite_key_validation_returns_bad_request() -> None:
    client = TestClient(create_app())
    client.post(
        "/tables",
        json={"tableName": "events", "partitionKey": "userId", "sortKey": "eventId"},
    )

    response = client.put(
        "/tables/events/items/user-1",
        json={"value": {"name": "Alex"}},
    )

    assert response.status_code == 400
