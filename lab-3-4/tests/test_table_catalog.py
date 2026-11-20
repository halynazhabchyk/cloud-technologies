from pathlib import Path

from fastapi.testclient import TestClient

from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.main import create_app

ORDERS_TABLE = {"tableName": "orders", "partitionKey": "orderId", "sortKey": "itemId"}


def test_tables_survive_coordinator_restart(tmp_path: Path) -> None:
    settings = CoordinatorSettings(tables_file=tmp_path / "tables.json")

    first_run = TestClient(create_app(settings))
    assert first_run.post("/tables", json=ORDERS_TABLE).status_code == 201

    # A new app instance with the same file models a coordinator restart.
    second_run = TestClient(create_app(settings))
    assert second_run.get("/tables/orders").json() == ORDERS_TABLE
    assert second_run.post("/tables", json=ORDERS_TABLE).status_code == 409

    response = second_run.put("/tables/orders/items/o1?sortKey=i1", json={"value": {}})
    assert response.status_code == 201


def test_tables_stay_in_memory_without_a_file() -> None:
    first_run = TestClient(create_app(CoordinatorSettings()))
    assert first_run.post("/tables", json=ORDERS_TABLE).status_code == 201

    second_run = TestClient(create_app(CoordinatorSettings()))
    assert second_run.get("/tables/orders").status_code == 404
