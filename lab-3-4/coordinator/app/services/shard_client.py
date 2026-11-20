"""HTTP client from the coordinator to the replicas of a shard.

Writes go to the leader of the owning shard. Reads ask replicas one by one
until the read quorum (R) has answered and return the freshest answer.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from common.trace import trace_headers
from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.services.errors import ShardUnavailableError
from coordinator.app.services.metrics import SHARD_CALL_DURATION
from coordinator.app.services.shard_responses import (
    applied_offset,
    error_detail,
    read_records,
)
from coordinator.app.sharding.shard_registry import (
    NoAvailableShardsError,
    Replica,
    ShardRegistry,
)
from coordinator.app.storage.storage import RecordKey, RecordValue

logger = logging.getLogger(__name__)

# Replica API paths (see shard/app/api/routes/records.py).
ONE_RECORD_PATH = "record"
RECORDS_PATH = "records"


@dataclass(frozen=True)
class ShardResponse:
    """A replica response together with the shard and replica that produced it."""

    shard_id: str
    response: httpx.Response
    replica_id: str | None = None


@dataclass(frozen=True)
class ReplicaAnswer:
    """One answer within a read quorum."""

    replica_id: str
    applied_offset: int
    response: httpx.Response


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


def records_url(base_url: str, table_name: str, path: str) -> str:
    """URL of a table resource on a replica, e.g. ``.../tables/orders/records``."""
    return f"{base_url}/internal/tables/{quote(table_name, safe='')}/{path}"


def key_params(key: RecordKey) -> dict[str, Any]:
    """Query parameters that identify a record (or a whole partition)."""
    params: dict[str, Any] = {"partitionKey": key.partition_key}
    if key.sort_key is not None:
        params["sortKey"] = key.sort_key
    return params


class ShardClient:
    def __init__(self, registry: ShardRegistry, settings: CoordinatorSettings):
        self.registry = registry
        self.settings = settings

    # --- writes: always to the leader --------------------------------------

    def put_record(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue,
        *,
        timestamp: int | None,
        shard_id: str | None = None,
    ) -> ShardResponse:
        """Create or replace a record on the leader of the owning shard."""
        return self._write_to_leader(
            "PUT", table_name, key, timestamp, shard_id, body={"value": value}
        )

    def delete_record(
        self,
        table_name: str,
        key: RecordKey,
        *,
        timestamp: int | None,
        shard_id: str | None = None,
    ) -> ShardResponse:
        """Delete a record on the leader of the owning shard."""
        return self._write_to_leader("DELETE", table_name, key, timestamp, shard_id)

    def _write_to_leader(
        self,
        method: str,
        table_name: str,
        key: RecordKey,
        timestamp: int | None,
        shard_id: str | None,
        body: dict[str, Any] | None = None,
    ) -> ShardResponse:
        """``shard_id`` overrides routing (rebalancing writes to a chosen shard)."""
        try:
            shard_id, leader = self.registry.select_leader(key.partition_key, shard_id)
        except NoAvailableShardsError as error:
            raise ShardUnavailableError() from error

        params = key_params(key)
        # DEFENSE: acks = W, the leader confirms only after W replicas hold the write.
        params["acks"] = self.settings.write_quorum
        if timestamp is not None:
            params["timestamp"] = timestamp
        logger.info(
            "Write routed to %s leader %s",
            shard_id,
            leader.replica_id,
            extra={"shard": shard_id, "target": leader.replica_id},
        )
        url = records_url(leader.base_url, table_name, RECORDS_PATH)
        try:
            with httpx.Client(timeout=self.settings.request_timeout) as client:
                response = timed_request(
                    client,
                    shard_id,
                    leader.replica_id,
                    method,
                    url,
                    params=params,
                    json=body,
                )
        except httpx.RequestError as error:
            raise ShardUnavailableError() from error

        if response.status_code >= 500:
            raise ShardUnavailableError(error_detail(response))
        return ShardResponse(shard_id, response, leader.replica_id)

    # --- reads: a quorum of replicas ----------------------------------------

    def get_record(
        self, table_name: str, key: RecordKey, *, shard_id: str | None = None
    ) -> ShardResponse:
        """Read one record by its full key."""
        return self._quorum_read("GET", table_name, ONE_RECORD_PATH, key, shard_id)

    def get_partition(
        self, table_name: str, partition_key: str, *, shard_id: str | None = None
    ) -> ShardResponse:
        """Read every record of one partition."""
        key = RecordKey(partition_key)
        return self._quorum_read("GET", table_name, RECORDS_PATH, key, shard_id)

    def head_record(
        self, table_name: str, key: RecordKey, *, shard_id: str | None = None
    ) -> ShardResponse:
        """Check that a record exists (HEAD: status code only, no body)."""
        return self._quorum_read("HEAD", table_name, RECORDS_PATH, key, shard_id)

    def _quorum_read(
        self,
        method: str,
        table_name: str,
        path: str,
        key: RecordKey,
        shard_id: str | None,
    ) -> ShardResponse:
        # DEFENSE: R + W > N, so at least one of the R answers holds the latest
        # write; the replica with the highest applied offset is the freshest.
        try:
            shard_id, replicas = self.registry.select_read_replicas(
                key.partition_key, shard_id
            )
        except NoAvailableShardsError as error:
            raise ShardUnavailableError() from error

        answers = self._collect_answers(
            method, table_name, path, key, shard_id, replicas
        )
        read_quorum = self.settings.read_quorum
        if len(answers) < read_quorum:
            raise ShardUnavailableError(
                f"Read quorum not reached: {len(answers)} of {read_quorum} "
                "replicas answered"
            )

        freshest = max(answers, key=lambda answer: answer.applied_offset)
        logger.info(
            "Read served by %s",
            freshest.replica_id,
            extra={"shard": shard_id, "target": freshest.replica_id},
        )
        return ShardResponse(shard_id, freshest.response, freshest.replica_id)

    def _collect_answers(
        self,
        method: str,
        table_name: str,
        path: str,
        key: RecordKey,
        shard_id: str,
        replicas: list[Replica],
    ) -> list[ReplicaAnswer]:
        """Ask replicas in order until ``read_quorum`` of them have answered."""
        answers: list[ReplicaAnswer] = []
        with httpx.Client(timeout=self.settings.request_timeout) as client:
            for replica in replicas:
                if len(answers) == self.settings.read_quorum:
                    break
                url = records_url(replica.base_url, table_name, path)
                response = self._ask_replica(
                    client, shard_id, replica, method, url, key_params(key)
                )
                if response is not None:
                    answers.append(
                        ReplicaAnswer(
                            replica.replica_id, applied_offset(response), response
                        )
                    )
        return answers

    def _ask_replica(
        self,
        client: httpx.Client,
        shard_id: str,
        replica: Replica,
        method: str,
        url: str,
        params: dict[str, Any],
    ) -> httpx.Response | None:
        """One replica's answer, or None if it failed (it is then skipped a while)."""
        try:
            response = timed_request(
                client, shard_id, replica.replica_id, method, url, params=params
            )
        except httpx.RequestError:
            response = None
        if response is None or response.status_code >= 500:
            self.registry.mark_unavailable(
                shard_id, replica.replica_id, self.settings.replica_cooldown_seconds
            )
            return None
        return response

    # --- rebalancing ---------------------------------------------------------

    def list_all_records(self, shard_id: str) -> list[dict[str, Any]]:
        """Every live record of a shard, read from its leader."""
        try:
            shard_id, leader = self.registry.select_leader("", shard_id)
            with httpx.Client(timeout=self.settings.request_timeout) as client:
                response = timed_request(
                    client,
                    shard_id,
                    leader.replica_id,
                    "GET",
                    f"{leader.base_url}/internal/records",
                )
        except (NoAvailableShardsError, httpx.RequestError) as error:
            raise ShardUnavailableError() from error
        if response.status_code != 200:
            raise ShardUnavailableError(error_detail(response))
        return read_records(response)
