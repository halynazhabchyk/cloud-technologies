import os
from dataclasses import dataclass
from pathlib import Path

LEADER = "leader"
FOLLOWER = "follower"

REGISTRATION_RETRY_SECONDS = 3


@dataclass(frozen=True)
class ReplicaSettings:
    """Configuration of one replica process (one member of a shard's replica set)."""

    shard_id: str = "shard-1"
    replica_id: str = "shard-1"
    role: str = LEADER
    base_url: str = "http://shard1:8000"
    coordinator_url: str = "http://coordinator:8000"
    leader_url: str | None = None
    data_dir: Path | None = None
    # How long a follower waits in Kafka poll() when the topic has no new entries.
    poll_wait_seconds: float = 1.0
    # How long the leader waits for follower acknowledgements (write quorum).
    ack_timeout_seconds: float = 2.0
    registration_check_seconds: float = 10.0
    kafka_bootstrap: str | None = None
    kafka_topic_prefix: str = "kv-log-"

    def __post_init__(self) -> None:
        if self.role not in (LEADER, FOLLOWER):
            raise ValueError(f"REPLICA_ROLE must be '{LEADER}' or '{FOLLOWER}'")
        if self.role == FOLLOWER and not self.leader_url:
            raise ValueError("A follower needs LEADER_URL")

    @property
    def is_leader(self) -> bool:
        return self.role == LEADER


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


def load_settings() -> ReplicaSettings:
    shard_id = os.getenv("SHARD_ID", "shard-1")
    data_dir = os.getenv("DATA_DIR")
    return ReplicaSettings(
        shard_id=shard_id,
        replica_id=os.getenv("REPLICA_ID", shard_id),
        role=os.getenv("REPLICA_ROLE", LEADER),
        base_url=os.getenv("SHARD_URL", "http://shard1:8000"),
        coordinator_url=os.getenv("COORDINATOR_URL", "http://coordinator:8000"),
        leader_url=os.getenv("LEADER_URL") or None,
        data_dir=Path(data_dir) if data_dir else None,
        poll_wait_seconds=float(os.getenv("POLL_WAIT_SECONDS", "1")),
        ack_timeout_seconds=float(os.getenv("ACK_TIMEOUT_SECONDS", "2")),
        registration_check_seconds=float(os.getenv("REGISTRATION_CHECK_SECONDS", "10")),
        kafka_bootstrap=_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        kafka_topic_prefix=os.getenv("KAFKA_TOPIC_PREFIX", "kv-log-"),
    )
