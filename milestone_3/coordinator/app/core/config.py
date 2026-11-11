import os
from dataclasses import dataclass


def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).lower() == "true"


@dataclass(frozen=True)
class CoordinatorSettings:
    """Coordinator configuration, read from environment variables."""

    sharding_enabled: bool = False
    request_timeout: float = 3.0
    # Replication: N replicas per shard, W must confirm a write, R are read.
    # R + W > N makes every read quorum overlap the latest write quorum.
    replication_factor: int = 1
    write_quorum: int = 1
    read_quorum: int = 1
    # How long a replica that failed a request is skipped by reads.
    replica_cooldown_seconds: float = 5.0

    def __post_init__(self) -> None:
        for name, quorum in (
            ("WRITE_QUORUM", self.write_quorum),
            ("READ_QUORUM", self.read_quorum),
        ):
            if not 1 <= quorum <= self.replication_factor:
                raise ValueError(f"{name} must be between 1 and REPLICATION_FACTOR")


def load_settings() -> CoordinatorSettings:
    return CoordinatorSettings(
        sharding_enabled=_env_bool("SHARDING_ENABLED"),
        request_timeout=float(os.getenv("REQUEST_TIMEOUT", "3")),
        replication_factor=int(os.getenv("REPLICATION_FACTOR", "1")),
        write_quorum=int(os.getenv("WRITE_QUORUM", "1")),
        read_quorum=int(os.getenv("READ_QUORUM", "1")),
        replica_cooldown_seconds=float(os.getenv("REPLICA_COOLDOWN_SECONDS", "5")),
    )
