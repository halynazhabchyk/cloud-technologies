from prometheus_client import Counter, Gauge, Histogram

from common.metrics import LATENCY_BUCKETS
from coordinator.app.sharding.shard_registry import ShardRegistry

REPLICA_UP = Gauge(
    "kv_replica_up",
    "1 if the replica passed its last health check, 0 if it is down",
    ("shard", "replica", "role"),
)
SHARD_CALL_DURATION = Histogram(
    "kv_shard_call_duration_seconds",
    "Latency of coordinator calls to shard replicas",
    ("shard", "replica", "method", "outcome"),
    buckets=LATENCY_BUCKETS,
)
REBALANCE_KEYS_MOVED = Counter(
    "kv_rebalance_keys_moved_total",
    "Records moved to their new shard by background rebalancing",
)


def update_cluster_metrics(registry: ShardRegistry) -> None:
    for shard_id, replica in registry.replicas():
        REPLICA_UP.labels(shard_id, replica.replica_id, replica.role).set(
            1 if replica.healthy else 0
        )
