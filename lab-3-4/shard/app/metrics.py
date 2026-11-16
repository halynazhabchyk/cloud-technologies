from prometheus_client import Gauge

from app.replication.node import ReplicaNode

REPLICA_LABELS = ("shard", "replica", "role")

KEYS_PER_SHARD = Gauge(
    "kv_keys_per_shard", "Live records stored by the replica", REPLICA_LABELS
)
APPLIED_OFFSET = Gauge(
    "kv_applied_offset", "Last shard log offset applied by the replica", REPLICA_LABELS
)
REPLICATION_LAG_SECONDS = Gauge(
    "kv_replication_lag_seconds",
    "Age of the oldest log entry a follower has not acknowledged (leader's view)",
    ("shard", "follower"),
)
REPLICATION_LAG_ENTRIES = Gauge(
    "kv_replication_lag_entries",
    "Log entries a follower has not acknowledged yet (leader's view)",
    ("shard", "follower"),
)


def update_replica_metrics(node: ReplicaNode) -> None:
    status = node.status()
    labels = (status["shardId"], status["replicaId"], status["role"])
    KEYS_PER_SHARD.labels(*labels).set(status["keys"])
    APPLIED_OFFSET.labels(*labels).set(status["appliedOffset"])
    for follower_id, progress in status.get("followers", {}).items():
        REPLICATION_LAG_SECONDS.labels(status["shardId"], follower_id).set(
            progress["lagSeconds"]
        )
        REPLICATION_LAG_ENTRIES.labels(status["shardId"], follower_id).set(
            progress["lag"]
        )
