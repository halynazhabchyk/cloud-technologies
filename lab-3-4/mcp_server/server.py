import logging
import sys
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from mcp_server.config import McpSettings, load_settings
from mcp_server.coordinator_client import CoordinatorClient, CoordinatorError

INSTRUCTIONS = (
    "Tools for a distributed key-value store. Records live in tables; a record "
    "is addressed by table, partition key and, if the table has one, sort key. "
    "Call list_tables first to learn table names and their key attributes. "
    "Values are JSON objects. Every answer has a traceId for the server logs."
)

READ_ONLY = ToolAnnotations(readOnlyHint=True, idempotentHint=True)
WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True)
DESTRUCTIVE = ToolAnnotations(
    readOnlyHint=False, destructiveHint=True, idempotentHint=True
)


# Gemini rejects JSON schemas with a list of types ("string" or null), which
# ``str | None`` produces; an empty string means "no sort key" instead.
NO_SORT_KEY = ""


def _sort_key(sort_key: str) -> str | None:
    return sort_key or None


def _call(operation, *args: Any) -> Any:
    """Run a coordinator call; turn its failure into an MCP tool error."""
    try:
        return operation(*args)
    except CoordinatorError as error:
        raise ToolError(str(error)) from error


def build_server(client: CoordinatorClient, settings: McpSettings) -> FastMCP:
    # DEFENSE: агенту лише CRUD записів; адмін-операцій немає, delete – destructive.
    mcp = FastMCP(
        "kv-store",
        instructions=INSTRUCTIONS,
        host=settings.host,
        port=settings.port,
    )

    @mcp.tool(annotations=READ_ONLY)
    def list_tables() -> list[dict]:
        """List tables with their partition key and sort key attribute names."""
        return _call(client.list_tables)

    @mcp.tool(annotations=READ_ONLY)
    def cluster_health() -> dict:
        """Cluster state: shards, replicas, how many are healthy, quorums."""
        return _call(client.health)

    @mcp.tool(annotations=READ_ONLY)
    def kv_exists(table: str, partition_key: str, sort_key: str = NO_SORT_KEY) -> dict:
        """EXISTS: check whether a record exists. Pass sort_key if the table has one."""
        return _call(client.exists, table, partition_key, _sort_key(sort_key))

    @mcp.tool(annotations=WRITE)
    def kv_create(
        table: str,
        partition_key: str,
        value: dict[str, Any],
        sort_key: str = NO_SORT_KEY,
    ) -> dict:
        """CREATE: store a JSON object under a key; an existing record is replaced."""
        return _call(client.put, table, partition_key, _sort_key(sort_key), value)

    @mcp.tool(annotations=READ_ONLY)
    def kv_read(table: str, partition_key: str, sort_key: str = NO_SORT_KEY) -> dict:
        return _call(client.read, table, partition_key, _sort_key(sort_key))

    if settings.allow_delete:

        @mcp.tool(annotations=DESTRUCTIVE)
        def kv_delete(
            table: str, partition_key: str, sort_key: str = NO_SORT_KEY
        ) -> dict:
            """DELETE: permanently remove one record. Confirm with the user first."""
            return _call(client.delete, table, partition_key, _sort_key(sort_key))

    return mcp


def main() -> None:
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    settings = load_settings()
    http = httpx.Client(
        base_url=settings.coordinator_url, timeout=settings.request_timeout
    )
    build_server(CoordinatorClient(http), settings).run(settings.transport)


if __name__ == "__main__":
    main()
