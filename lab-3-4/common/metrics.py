from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.responses import Response

from common.build_info import APP_VERSION, build_info

REQUEST_LABELS = ("service", "method", "route", "status")
LATENCY_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.15,
    0.2,
    0.3,
    0.5,
    0.75,
    1.0,
    2.0,
    5.0,
)

REQUEST_DURATION = Histogram(
    "kv_request_duration_seconds",
    "HTTP request latency",
    REQUEST_LABELS,
    buckets=LATENCY_BUCKETS,
)
REQUESTS_TOTAL = Counter("kv_requests_total", "HTTP requests handled", REQUEST_LABELS)
BUILD_INFO = Gauge(
    "kv_build_info",
    "Build of the running service (value is always 1)",
    ("service", "version", "git_sha"),
)


def record_request(
    service: str, method: str, route: str, status: int, seconds: float
) -> None:
    labels = (service, method, route, str(status))
    REQUEST_DURATION.labels(*labels).observe(seconds)
    REQUESTS_TOTAL.labels(*labels).inc()


def record_build_info(service: str) -> None:
    BUILD_INFO.labels(service, APP_VERSION, build_info(service)["gitSha"]).set(1)


def metrics_response() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
