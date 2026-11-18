import json

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from mcp_server.config import McpSettings
from mcp_server.coordinator_client import CoordinatorClient
from mcp_server.server import build_server

TABLE = {"tableName": "orders", "partitionKey": "customerId", "sortKey": "orderId"}
KEY = {"table": "orders", "partition_key": "c1", "sort_key": "o1"}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def tool_result(result) -> dict:
    assert not result.isError, result.content
    return json.loads(result.content[0].text)


def server_for(cluster, allow_delete: bool = True):
    cluster.register_all()
    cluster.coordinator.post("/tables", json=TABLE)
    client = CoordinatorClient(cluster.coordinator)
    return build_server(client, McpSettings(allow_delete=allow_delete))


@pytest.mark.anyio
async def test_agent_can_exists_create_read_delete(cluster) -> None:
    server = server_for(cluster)
    async with create_connected_server_and_client_session(server) as session:
        missing = tool_result(await session.call_tool("kv_exists", KEY))
        created = tool_result(
            await session.call_tool("kv_create", {**KEY, "value": {"total": 10}})
        )
        found = tool_result(await session.call_tool("kv_exists", KEY))
        read = tool_result(await session.call_tool("kv_read", KEY))
        deleted = tool_result(await session.call_tool("kv_delete", KEY))
        after = tool_result(await session.call_tool("kv_read", KEY))

    assert missing["exists"] is False
    assert created["created"] is True and created["shardId"]
    assert found["exists"] is True
    assert read["value"] == {"total": 10}
    assert read["traceId"].startswith("mcp-")
    assert deleted["deleted"] is True
    assert after["found"] is False


@pytest.mark.anyio
async def test_errors_come_back_as_tool_errors(cluster) -> None:
    server = server_for(cluster)
    async with create_connected_server_and_client_session(server) as session:
        read = await session.call_tool(
            "kv_read", {"table": "missing", "partition_key": "c1"}
        )
        exists = await session.call_tool(
            "kv_exists", {"table": "missing", "partition_key": "c1"}
        )

    for result in (read, exists):
        assert result.isError
        assert "Table 'missing' does not exist" in result.content[0].text


@pytest.mark.anyio
async def test_delete_tool_can_be_disabled(cluster) -> None:
    server = server_for(cluster, allow_delete=False)
    async with create_connected_server_and_client_session(server) as session:
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}

    assert set(tools) == {
        "list_tables",
        "cluster_health",
        "kv_exists",
        "kv_create",
        "kv_read",
    }
    assert tools["kv_read"].annotations.readOnlyHint is True


def test_http_transport_settings_reach_fastmcp() -> None:
    settings = McpSettings(transport="streamable-http", host="0.0.0.0", port=8100)
    server = build_server(CoordinatorClient(None), settings)  # type: ignore[arg-type]

    assert server.settings.host == "0.0.0.0"
    assert server.settings.port == 8100
    assert server.settings.streamable_http_path == "/mcp"


def test_unknown_transport_is_rejected() -> None:
    with pytest.raises(ValueError):
        McpSettings(transport="websocket")  # type: ignore[arg-type]
