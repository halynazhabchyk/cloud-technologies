import os

SHARDING_ENABLED = os.getenv("SHARDING_ENABLED", "false").lower() == "true"
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "3"))
