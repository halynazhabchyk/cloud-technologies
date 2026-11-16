import os
from dataclasses import dataclass


@dataclass(frozen=True)
class McpSettings:
    """MCP server configuration, read from environment variables."""

    coordinator_url: str = "http://127.0.0.1:8000"
    request_timeout: float = 5.0
    allow_delete: bool = True


def load_settings() -> McpSettings:
    return McpSettings(
        coordinator_url=os.getenv("COORDINATOR_URL", "http://127.0.0.1:8000"),
        request_timeout=float(os.getenv("REQUEST_TIMEOUT", "5")),
        allow_delete=os.getenv("MCP_ALLOW_DELETE", "true").lower() == "true",
    )
