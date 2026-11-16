import logging
import time
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from common.build_info import build_info
from common.metrics import metrics_response, record_build_info, record_request
from common.trace import TRACE_HEADER, accept_trace_id, reset_trace_id, set_trace_id

logger = logging.getLogger("kv.http")

UNTRACKED_ROUTES = frozenset(
    {"/metrics", "/health", "/internal/ack", "/internal/prometheus-targets"}
)
UNMATCHED_ROUTE = "unmatched"
_TRACE_HEADER_BYTES = TRACE_HEADER.lower().encode("latin-1")


def route_template(scope: Scope) -> str:
    return getattr(scope.get("route"), "path", UNMATCHED_ROUTE)


def _incoming_trace_id(scope: Scope) -> str:
    for name, value in scope["headers"]:
        if name == _TRACE_HEADER_BYTES:
            return accept_trace_id(value.decode("latin-1"))
    return accept_trace_id(None)


class ObservabilityMiddleware:

    def __init__(self, app: ASGIApp, service: str):
        self.app = app
        self.service = service

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        trace_id = _incoming_trace_id(scope)
        token = set_trace_id(trace_id)
        started = time.perf_counter()
        status = 500  # stays 500 if the handler raises

        async def send_with_trace_id(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = list(message.get("headers", []))
                headers.append((_TRACE_HEADER_BYTES, trace_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_trace_id)
        finally:
            self._observe(scope, status, time.perf_counter() - started)
            reset_trace_id(token)

    def _observe(self, scope: Scope, status: int, seconds: float) -> None:
        route = route_template(scope)
        if route in UNTRACKED_ROUTES:
            return
        method, path = scope["method"], scope["path"]
        record_request(self.service, method, route, status, seconds)
        extra: dict[str, Any] = {
            "method": method,
            "path": path,
            "status": status,
            "duration_ms": round(seconds * 1000, 1),
        }
        logger.info("%s %s -> %s", method, path, status, extra=extra)


def install_observability(
    app: FastAPI,
    service: str,
    before_scrape: Callable[[], None] | None = None,
) -> None:
    record_build_info(service)
    app.add_middleware(ObservabilityMiddleware, service=service)

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        if before_scrape is not None:
            before_scrape()
        return metrics_response()

    @app.get("/version", tags=["Health"])
    def version() -> dict[str, str]:
        """Version and git commit of the running build."""
        return build_info(service)
