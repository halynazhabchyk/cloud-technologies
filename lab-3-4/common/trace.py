import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token

TRACE_HEADER = "X-Trace-Id"
_VALID_TRACE_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

_current_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)


def new_trace_id() -> str:
    return uuid.uuid4().hex


def accept_trace_id(header_value: str | None) -> str:
    # DEFENSE: trace ID із X-Trace-Id або новий UUID; далі йде в усі виклики й логи.
    """Use the caller's trace ID if it is valid, otherwise start a new trace."""
    if header_value and _VALID_TRACE_ID.match(header_value):
        return header_value
    return new_trace_id()


def get_trace_id() -> str | None:
    return _current_trace_id.get()


def set_trace_id(trace_id: str | None) -> Token[str | None]:
    return _current_trace_id.set(trace_id)


def reset_trace_id(token: Token[str | None]) -> None:
    _current_trace_id.reset(token)


@contextmanager
def use_trace_id(trace_id: str | None) -> Iterator[None]:
    """Run a block under the given trace ID (background threads, log replay)."""
    token = set_trace_id(trace_id)
    try:
        yield
    finally:
        reset_trace_id(token)


def trace_headers() -> dict[str, str]:
    """Headers that pass the current trace ID on to the next service."""
    trace_id = get_trace_id()
    return {TRACE_HEADER: trace_id} if trace_id else {}
