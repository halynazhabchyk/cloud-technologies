from typing import Any
from urllib.parse import quote

import httpx

from common.trace import TRACE_HEADER, new_trace_id

# Agent requests are easy to find in the logs: grep "mcp-".
TRACE_PREFIX = "mcp-"
RECORD_NOT_FOUND = "Record not found"


class CoordinatorError(RuntimeError):
    """The coordinator answered with an error (message from its error body)."""


def _items_path(table: str, partition_key: str) -> str:
    return f"/tables/{quote(table, safe='')}/items/{quote(partition_key, safe='')}"


def _key_params(sort_key: str | None) -> dict[str, str]:
    return {"sortKey": sort_key} if sort_key is not None else {}


def _is_missing_record(response: httpx.Response) -> bool:
    if response.status_code != 404:
        return False
    try:
        return response.json().get("error") == RECORD_NOT_FOUND
    except ValueError:
        return False


class CoordinatorClient:
    def __init__(self, http: httpx.Client):
        """``http`` has the coordinator as its base URL (tests pass a TestClient)."""
        self.http = http

    def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> tuple[httpx.Response, str]:
        trace_id = f"{TRACE_PREFIX}{new_trace_id()}"
        try:
            response = self.http.request(
                method, path, headers={TRACE_HEADER: trace_id}, **kwargs
            )
        except httpx.RequestError as error:
            raise CoordinatorError(f"Coordinator is unreachable: {error}") from error
        return response, trace_id

    @staticmethod
    def _fail(response: httpx.Response, trace_id: str) -> CoordinatorError:
        try:
            message = response.json().get("error", response.text)
        except ValueError:
            message = response.text or f"HTTP {response.status_code}"
        return CoordinatorError(f"{message} (HTTP {response.status_code}, {trace_id})")

    def exists(self, table: str, partition_key: str, sort_key: str | None) -> dict:
        response, trace_id = self._request(
            "HEAD", _items_path(table, partition_key), params=_key_params(sort_key)
        )
        if response.status_code == 404:
            # A HEAD response has no body: check that the table itself exists.
            self.describe_table(table)
        elif response.status_code != 200:
            raise self._fail(response, trace_id)
        return {"exists": response.status_code == 200, "traceId": trace_id}

    def put(
        self,
        table: str,
        partition_key: str,
        sort_key: str | None,
        value: dict[str, Any],
    ) -> dict:
        response, trace_id = self._request(
            "PUT",
            _items_path(table, partition_key),
            params=_key_params(sort_key),
            json={"value": value},
        )
        if response.status_code not in (200, 201):
            raise self._fail(response, trace_id)
        return {
            **response.json(),
            "created": response.status_code == 201,
            "traceId": trace_id,
        }

    def read(self, table: str, partition_key: str, sort_key: str | None) -> dict:
        response, trace_id = self._request(
            "GET", _items_path(table, partition_key), params=_key_params(sort_key)
        )
        if _is_missing_record(response):
            return {"found": False, "traceId": trace_id}
        if response.status_code != 200:
            raise self._fail(response, trace_id)
        return {"found": True, **response.json(), "traceId": trace_id}

    def delete(self, table: str, partition_key: str, sort_key: str | None) -> dict:
        response, trace_id = self._request(
            "DELETE", _items_path(table, partition_key), params=_key_params(sort_key)
        )
        if response.status_code != 204 and not _is_missing_record(response):
            raise self._fail(response, trace_id)
        return {"deleted": response.status_code == 204, "traceId": trace_id}

    def describe_table(self, table: str) -> dict:
        response, trace_id = self._request("GET", f"/tables/{quote(table, safe='')}")
        if response.status_code != 200:
            raise self._fail(response, trace_id)
        return response.json()

    def list_tables(self) -> list[dict]:
        response, trace_id = self._request("GET", "/tables")
        if response.status_code != 200:
            raise self._fail(response, trace_id)
        return response.json()

    def health(self) -> dict:
        response, trace_id = self._request("GET", "/health")
        if response.status_code != 200:
            raise self._fail(response, trace_id)
        return response.json()
