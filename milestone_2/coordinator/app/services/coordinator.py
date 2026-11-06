from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from coordinator.app.sharding.shard_registry import (
    NoAvailableShardsError,
    ShardRegistry,
)
from coordinator.app.storage.storage import RecordKey, RecordValue, Storage

SHARD_UNAVAILABLE_DETAIL = "No available shard can handle the request"


class ShardUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShardResponse:
    shard_id: str
    response: httpx.Response


class Coordinator:
    def __init__(
        self,
        storage: Storage,
        shard_registry: ShardRegistry | None = None,
        sharding_enabled: bool = False,
        request_timeout: float = 3.0,
    ):
        self.storage = storage
        self.shard_registry = shard_registry or ShardRegistry()
        self.sharding_enabled = sharding_enabled
        self.request_timeout = request_timeout

    def _request_shard(
        self,
        method: str,
        table_name: str,
        key: RecordKey,
        value: RecordValue | None = None,
    ) -> ShardResponse:
        try:
            shard = self.shard_registry.select_shard(key.partition_key)
        except NoAvailableShardsError as error:
            raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL) from error

        table_segment = quote(table_name, safe="")
        url = f"{shard['baseUrl']}/internal/tables/{table_segment}/records"
        params = {"partitionKey": key.partition_key}
        if key.sort_key is not None:
            params["sortKey"] = key.sort_key

        try:
            with httpx.Client(timeout=self.request_timeout) as client:
                response = client.request(
                    method,
                    url,
                    params=params,
                    json={"value": value} if method == "PUT" else None,
                )
        except httpx.RequestError as error:
            raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL) from error

        if response.status_code >= 500:
            raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)
        return ShardResponse(shard_id=shard["shardId"], response=response)

    @staticmethod
    def _read_shard_payload(response: httpx.Response) -> Any:
        try:
            return response.json()
        except (TypeError, ValueError) as error:
            raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL) from error

    @classmethod
    def _read_shard_json(cls, response: httpx.Response) -> dict[str, Any]:
        payload = cls._read_shard_payload(response)
        if not isinstance(payload, dict):
            raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)

        return payload

    @classmethod
    def _read_shard_records(
        cls,
        response: httpx.Response,
    ) -> list[dict[str, Any]]:
        payload = cls._read_shard_payload(response)
        if not isinstance(payload, list):
            raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)

        for record in payload:
            if (
                not isinstance(record, dict)
                or not isinstance(record.get("value"), dict)
                or "sortKey" not in record
                or (
                    record["sortKey"] is not None
                    and not isinstance(record["sortKey"], str)
                )
            ):
                raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)

        return payload

    def create_table(
        self,
        table_name: str,
        partition_key: str,
        sort_key: str | None = None,
    ) -> dict[str, Any]:
        if not table_name.strip():
            raise ValueError("Table name cannot be empty")

        if not partition_key.strip():
            raise ValueError("Partition key cannot be empty")

        if sort_key is not None and not sort_key.strip():
            raise ValueError("Sort key cannot be empty")

        return self.storage.create_table(
            table_name=table_name,
            partition_key=partition_key,
            sort_key=sort_key,
        )

    def list_tables(self) -> list[dict[str, Any]]:
        return self.storage.list_tables()

    def get_table(self, table_name: str) -> dict[str, Any]:
        return self.storage.get_table(table_name)

    def _validate_record_key(
        self,
        table_name: str,
        key: RecordKey,
    ) -> None:
        table = self.storage.get_table(table_name)
        sort_key_name = table.get("sortKey")

        if not key.partition_key.strip():
            raise ValueError("Partition key value cannot be empty")

        if sort_key_name is not None and key.sort_key is None:
            raise ValueError("Sort key value is required")

        if sort_key_name is None and key.sort_key is not None:
            raise ValueError("This table does not use a sort key")

        if key.sort_key is not None and not key.sort_key.strip():
            raise ValueError("Sort key value cannot be empty")

    def create_record(
        self,
        table_name: str,
        key: RecordKey,
        value: RecordValue,
    ) -> tuple[bool, str | None]:
        self._validate_record_key(table_name, key)

        if self.sharding_enabled:
            shard_response = self._request_shard("PUT", table_name, key, value)
            response = shard_response.response
            if response.status_code not in (200, 201):
                raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)

            payload = self._read_shard_json(response)
            created = payload.get("created")
            if not isinstance(created, bool):
                raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)
            return created, shard_response.shard_id

        created = self.storage.put_record(
            table_name=table_name,
            key=key,
            value=value,
        )
        return created, None

    def read_records_by_partition(
        self,
        table_name: str,
        partition_key: str,
    ) -> dict[str, Any]:
        self.storage.get_table(table_name)
        if not partition_key.strip():
            raise ValueError("Partition key value cannot be empty")

        if self.sharding_enabled:
            shard_response = self._request_shard(
                "GET",
                table_name,
                RecordKey(partition_key=partition_key),
            )
            response = shard_response.response
            if response.status_code == 404:
                raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)
            if response.status_code != 200:
                raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)
            return {
                "shardId": shard_response.shard_id,
                "records": self._read_shard_records(response),
            }

        return {
            "shardId": None,
            "records": self.storage.list_records_by_partition(
                table_name=table_name,
                partition_key=partition_key,
            ),
        }

    def delete_record(
        self,
        table_name: str,
        key: RecordKey,
    ) -> bool:
        self._validate_record_key(table_name, key)

        if self.sharding_enabled:
            response = self._request_shard("DELETE", table_name, key).response
            if response.status_code == 404:
                return False
            if response.status_code == 204:
                return True
            if response.status_code == 200:
                payload = self._read_shard_json(response)
                if payload.get("deleted") is True:
                    return True
            raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)

        return self.storage.delete_record(
            table_name=table_name,
            key=key,
        )

    def record_exists(
        self,
        table_name: str,
        key: RecordKey,
    ) -> bool:
        self._validate_record_key(table_name, key)

        if self.sharding_enabled:
            response = self._request_shard("HEAD", table_name, key).response
            if response.status_code == 404:
                return False
            if response.status_code != 200:
                raise ShardUnavailableError(SHARD_UNAVAILABLE_DETAIL)
            return True

        return self.storage.record_exists(
            table_name=table_name,
            key=key,
        )
