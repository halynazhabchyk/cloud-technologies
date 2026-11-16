from coordinator.app.services.shard_client import ShardUnavailableError

__all__ = ["InvalidRequestError", "ShardUnavailableError", "WriteConflictError"]


class InvalidRequestError(ValueError):
    """The request is well-formed but invalid for the table (HTTP 400)."""


class WriteConflictError(RuntimeError):
    """Last-Write-Wins rejected the write: a newer version is stored (HTTP 409)."""
