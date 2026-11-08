import os

SHARD_ID = os.getenv("SHARD_ID", "shard-1")
SHARD_URL = os.getenv("SHARD_URL", "http://shard1:8000")
COORDINATOR_URL = os.getenv(
    "COORDINATOR_URL",
    "http://coordinator:8000",
)
REGISTRATION_RETRY_SECONDS = 3
