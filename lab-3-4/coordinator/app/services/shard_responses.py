"""Parsing and validation of replica responses.

A replica that answers with malformed JSON is treated like an unavailable one:
the coordinator returns 503 instead of passing broken data to the client.
"""

from typing import Any

import httpx

from common.replica_headers import APPLIED_OFFSET_HEADER
from coordinator.app.services.errors import (
    SHARD_UNAVAILABLE_DETAIL,
    ShardUnavailableError,
)

UNKNOWN_OFFSET = -1


def error_detail(response: httpx.Response) -> str:
    """Error message of a replica response in the common error format."""
    try:
        detail = response.json().get("error")
    except (TypeError, ValueError, AttributeError):
        detail = None
    return detail if isinstance(detail, str) else SHARD_UNAVAILABLE_DETAIL


def applied_offset(response: httpx.Response) -> int:
    """Log offset the answering replica has applied (-1 when unknown)."""
    try:
        return int(response.headers.get(APPLIED_OFFSET_HEADER, UNKNOWN_OFFSET))
    except ValueError:
        return UNKNOWN_OFFSET


def read_json_object(response: httpx.Response) -> dict[str, Any]:
    """Parse a replica response that must be a JSON object."""
    payload = _read_json(response)
    if not isinstance(payload, dict):
        raise ShardUnavailableError()
    return payload


def read_records(response: httpx.Response) -> list[dict[str, Any]]:
    """Parse a replica response that must be a list of records."""
    payload = _read_json(response)
    if not isinstance(payload, list) or not all(map(_is_valid_record, payload)):
        raise ShardUnavailableError()
    return payload


def _read_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except (TypeError, ValueError) as error:
        raise ShardUnavailableError() from error


def _is_valid_record(record: Any) -> bool:
    """A record has an object ``value`` and a string or null ``sortKey``."""
    if not isinstance(record, dict) or not isinstance(record.get("value"), dict):
        return False
    if "sortKey" not in record:
        return False
    return record["sortKey"] is None or isinstance(record["sortKey"], str)
