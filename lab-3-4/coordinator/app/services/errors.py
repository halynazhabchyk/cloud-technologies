SHARD_UNAVAILABLE_DETAIL = "No available shard can handle the request"


class InvalidRequestError(ValueError):
    """The request is well-formed but invalid for the table (HTTP 400)."""


class WriteConflictError(RuntimeError):
    """Last-Write-Wins rejected the write: a newer version is stored (HTTP 409)."""


class ShardUnavailableError(RuntimeError):
    """No replica could serve the request, or a replica answered incorrectly (503)."""

    def __init__(self, detail: str = SHARD_UNAVAILABLE_DETAIL):
        super().__init__(detail)
