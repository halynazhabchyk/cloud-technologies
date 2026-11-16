import logging
import threading

import httpx

from coordinator.app.core.config import CoordinatorSettings
from coordinator.app.sharding.shard_registry import ShardRegistry

logger = logging.getLogger(__name__)


class ReplicaHealthChecker:
    def __init__(self, registry: ShardRegistry, settings: CoordinatorSettings):
        self.registry = registry
        self.settings = settings
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def check_once(self) -> None:
        # DEFENSE: падіння видно за 5 с + таймаут 1 с ≈ 6 с, навіть без трафіку.
        with httpx.Client(timeout=self.settings.health_check_timeout) as client:
            for shard_id, replica in self.registry.replicas():
                healthy = self._probe(client, replica.base_url)
                if self.registry.set_health(shard_id, replica.replica_id, healthy):
                    self._log_change(shard_id, replica.replica_id, healthy)

    @staticmethod
    def _probe(client: httpx.Client, base_url: str) -> bool:
        try:
            return client.get(f"{base_url}/health").status_code == 200
        except httpx.RequestError:
            return False

    @staticmethod
    def _log_change(shard_id: str, replica_id: str, healthy: bool) -> None:
        if healthy:
            logger.info("Replica %s is back up", replica_id, extra={"shard": shard_id})
        else:
            logger.warning("Replica %s is down", replica_id, extra={"shard": shard_id})

    def _run(self) -> None:
        while not self._stop.wait(self.settings.health_check_seconds):
            try:
                self.check_once()
            except Exception:  # the checker must outlive any single failure
                logger.exception("Health check round failed")

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="replica-health-checker", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
