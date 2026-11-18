import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from common.trace import trace_headers
from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.services.errors import (
    SHARD_UNAVAILABLE_DETAIL,
    ShardUnavailableError,
)
from coordinator.app.services.metrics import SHARD_CALL_DURATION
from coordinator.app.sharding.shard_registry import (
    NoAvailableShardsError,
    ShardRegistry,
)
from coordinator.app.storage.storage import RecordKey, RecordValue

logger = logging.getLogger(__name__)

# Contract with the replica API (see shard/app/api/routes/records.py).
APPLIED_OFFSET_HEADER = "X-Applied-Offset"
REPLICA_ID_HEADER = "X-Replica-Id"


@dataclass(frozen=True)
class ShardResponse:
    shard_id: str
    response: httpx.Response
    replica_id: str | None = None


def _record_params(key: RecordKey) -> dict[str, Any]:
    params: dict[str, Any] = {"partitionKey": key.partition_key}
    if key.sort_key is not None:
        params["sortKey"] = key.sort_key
    return params


def _record_url(base_url: str, table_name: str, resource: str) -> str:
    table_segment = quote(table_name, safe="")
    return f"{base_url}/internal/tables/{table_segment}/{resource}"


def error_detail(response: httpx.Response) -> str:
    """Error message of a replica response in the common error format."""
    try:
        detail = response.json().get("error")
    except (TypeError, ValueError, AttributeError):
        detail = None
    return detail if isinstance(detail, str) else SHARD_UNAVAILABLE_DETAIL


def _applied_offset(response: httpx.Response) -> int:
    try:
        return int(response.headers.get(APPLIED_OFFSET_HEADER, "-1"))
    except ValueError:
        return -1


def read_json_object(response: httpx.Response) -> dict[str, Any]:
    """Parse a replica response that must be a JSON object."""
    try:
        payload = response.json()
    except (TypeError, ValueError) as error:
        raise ShardUnavailableError() from error
    if not isinstance(payload, dict):
        raise ShardUnavailableError()
    return payload


def read_records(response: httpx.Response) -> list[dict[str, Any]]:
    """Parse and validate a replica's list of records."""
    try:
        payload = response.json()
    except (TypeError, ValueError) as error:
        raise ShardUnavailableError() from error
    if not isinstance(payload, list):
        raise ShardUnavailableError()

    for record in payload:
        if (
            not isinstance(record, dict)
            or not isinstance(record.get("value"), dict)
            or "sortKey" not in record
            or (
                record["sortKey"] is not None and not isinstance(record["sortKey"], str)
            )
        ):
            raise ShardUnavailableError()
    return payload


def timed_request(
    client: httpx.Client,
    shard_id: str,
    replica_id: str,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Send one request to a replica with the trace ID; measure its latency."""
    started = time.perf_counter()
    outcome = "error"
    try:
        response = client.request(method, url, headers=trace_headers(), **kwargs)
        outcome = "error" if response.status_code >= 500 else "ok"
        return response
    finally:
        SHARD_CALL_DURATION.labels(shard_id, replica_id, method, outcome).observe(
            time.perf_counter() - started
        )


class ShardClient:
    def __init__(self, registry: ShardRegistry, settings: CoordinatorSettings):
        self.registry = registry
        self.settings = settings

    def write(
        self,
        method: str,
        table_name: str,
        key: RecordKey,
        value: RecordValue | None = None,
        *,
        timestamp: int | None = None,
        shard_id: str | None = None,
    ) -> ShardResponse:
        """Send PUT/DELETE to the leader of the owning shard (or of ``shard_id``)."""
        try:
            shard = self.registry.select_shard(key.partition_key, shard_id)
        except NoAvailableShardsError as error:
            raise ShardUnavailableError() from error

        params = _record_params(key)
        params["acks"] = self.settings.write_quorum
        if timestamp is not None:
            params["timestamp"] = timestamp
        logger.info(
            "Write routed to %s leader %s",
            shard["shardId"],
            shard["replicaId"],
            extra={"shard": shard["shardId"], "target": shard["replicaId"]},
        )
        try:
            with httpx.Client(timeout=self.settings.request_timeout) as client:
                response = timed_request(
                    client,
                    shard["shardId"],
                    shard["replicaId"],
                    method,
                    _record_url(shard["baseUrl"], table_name, "records"),
                    params=params,
                    json={"value": value} if method == "PUT" else None,
                )
        except httpx.RequestError as error:
            raise ShardUnavailableError() from error

        if response.status_code >= 500:
            raise ShardUnavailableError(error_detail(response))
        return ShardResponse(shard["shardId"], response, shard["replicaId"])

    def read(
        self,
        method: str,
        table_name: str,
        key: RecordKey,
        resource: str,
        *,
        shard_id: str | None = None,
    ) -> ShardResponse:
        try:
            shard_id, replicas = self.registry.select_read_replicas(
                key.partition_key, shard_id
            )
        except NoAvailableShardsError as error:
            raise ShardUnavailableError() from error
        read_quorum = self.settings.read_quorum
        params = _record_params(key)
        answers: list[tuple[int, str, httpx.Response]] = []
        with httpx.Client(timeout=self.settings.request_timeout) as client:
            for replica in replicas:
                if len(answers) == read_quorum:
                    break
                url = _record_url(replica.base_url, table_name, resource)
                try:
                    response = timed_request(
                        client, shard_id, replica.replica_id, method, url, params=params
                    )
                except httpx.RequestError:
                    response = None
                if response is None or response.status_code >= 500:
                    self.registry.mark_unavailable(
                        shard_id,
                        replica.replica_id,
                        self.settings.replica_cooldown_seconds,
                    )
                    continue
                answers.append(
                    (_applied_offset(response), replica.replica_id, response)
                )
        if len(answers) < read_quorum:
            raise ShardUnavailableError(
                f"Read quorum not reached: {len(answers)} of {read_quorum} "
                "replicas answered"
            )
        _, replica_id, response = max(answers, key=lambda answer: answer[0])
        logger.info(
            "Read served by %s",
            replica_id,
            extra={"shard": shard_id, "target": replica_id},
        )
        return ShardResponse(shard_id, response, replica_id)

    def list_all_records(self, shard_id: str) -> list[dict[str, Any]]:
        """Every live record of a shard, read from its leader (rebalancing)."""
        try:
            leader = self.registry.select_shard("", shard_id)
            with httpx.Client(timeout=self.settings.request_timeout) as client:
                response = timed_request(
                    client,
                    shard_id,
                    leader["replicaId"],
                    "GET",
                    f"{leader['baseUrl']}/internal/records",
                )
        except (NoAvailableShardsError, httpx.RequestError) as error:
            raise ShardUnavailableError() from error
        if response.status_code != 200:
            raise ShardUnavailableError(error_detail(response))
        return read_records(response)
