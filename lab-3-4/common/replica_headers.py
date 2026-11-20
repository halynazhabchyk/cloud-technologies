"""HTTP headers of the replica API: the contract between coordinator and shards.

Every replica response says which replica answered and how far it has applied
the replication log. The coordinator uses the offset to pick the freshest
answer of a read quorum.
"""

APPLIED_OFFSET_HEADER = "X-Applied-Offset"
REPLICA_ID_HEADER = "X-Replica-Id"
