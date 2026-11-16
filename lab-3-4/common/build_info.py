import os

APP_VERSION = "4.0.0"
UNKNOWN = "unknown"


def build_info(service: str) -> dict[str, str]:
    return {
        "service": service,
        "version": APP_VERSION,
        "gitSha": os.getenv("GIT_SHA", UNKNOWN),
        "buildTime": os.getenv("BUILD_TIME", UNKNOWN),
    }
