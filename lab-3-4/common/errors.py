from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from common.trace import get_trace_id


def error_response(
    status_code: int,
    message: str,
    headers: Mapping[str, str] | None = None,
    **fields: Any,
) -> JSONResponse:
    content: dict[str, Any] = {"error": message, "trace_id": get_trace_id(), **fields}
    return JSONResponse(
        status_code=status_code,
        content=content,
        headers=dict(headers) if headers else None,
    )


def install_error_format(app: FastAPI) -> None:
    """Replace FastAPI's ``{"detail": ...}`` errors with the common format."""

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_request: Request, error: StarletteHTTPException):
        return error_response(error.status_code, str(error.detail), error.headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, error: RequestValidationError):
        details = [
            {"location": list(item["loc"]), "message": item["msg"]}
            for item in error.errors()
        ]
        return error_response(422, "Request validation failed", details=details)
