import json
import logging
import os
import sys
from datetime import datetime, timezone

from common.trace import get_trace_id

# Optional fields a log call can pass with ``extra={...}``.
EXTRA_FIELDS = (
    "method",
    "path",
    "status",
    "duration_ms",
    "shard",
    "target",
    "offset",
    "op",
    "applied",
)


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, replica_id: str | None = None):
        super().__init__()
        self.service = service
        self.replica_id = replica_id

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if self.replica_id:
            payload["replica"] = self.replica_id
        trace_id = getattr(record, "trace_id", None) or get_trace_id()
        if trace_id:
            payload["trace_id"] = trace_id
        for field in EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(service: str, replica_id: str | None = None) -> None:
    root = logging.getLogger()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    # Idempotent: tests build several apps in one process.
    if not any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service, replica_id))
        root.addHandler(handler)

    for name in ("uvicorn", "uvicorn.error"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("httpx").setLevel(logging.WARNING)
