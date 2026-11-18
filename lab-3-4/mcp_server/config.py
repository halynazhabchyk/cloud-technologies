import os
from dataclasses import dataclass
from typing import Literal

Transport = Literal["stdio", "streamable-http"]
TRANSPORTS: tuple[Transport, ...] = ("stdio", "streamable-http")


@dataclass(frozen=True)
class McpSettings:
    """MCP server configuration, read from environment variables."""

    coordinator_url: str = "http://127.0.0.1:8000"
    request_timeout: float = 5.0
    allow_delete: bool = True
    # stdio: the client starts the process (Claude Code, MCP Inspector).
    # streamable-http: a network server at http://<host>:<port>/mcp (n8n).
    transport: Transport = "stdio"
    host: str = "127.0.0.1"
    port: int = 8100

    def __post_init__(self) -> None:
        if self.transport not in TRANSPORTS:
            raise ValueError(f"MCP_TRANSPORT must be one of {TRANSPORTS}")


def load_settings() -> McpSettings:
    return McpSettings(
        coordinator_url=os.getenv("COORDINATOR_URL", "http://127.0.0.1:8000"),
        request_timeout=float(os.getenv("REQUEST_TIMEOUT", "5")),
        allow_delete=os.getenv("MCP_ALLOW_DELETE", "true").lower() == "true",
        transport=os.getenv("MCP_TRANSPORT", "stdio"),  # type: ignore[arg-type]
        host=os.getenv("MCP_HOST", "127.0.0.1"),
        port=int(os.getenv("MCP_PORT", "8100")),
    )
