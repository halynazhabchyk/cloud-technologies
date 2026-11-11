"""Domain errors of the coordinator, mapped to HTTP codes in api/errors.py."""

from coordinator.app.services.shard_client import ShardUnavailableError

__all__ = ["InvalidRequestError", "ShardUnavailableError"]


class InvalidRequestError(ValueError):
    """The request is well-formed but invalid for the table (HTTP 400)."""
