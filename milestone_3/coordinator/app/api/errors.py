"""Map domain errors to HTTP responses once, instead of in every route."""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from coordinator.app.services.errors import InvalidRequestError, ShardUnavailableError


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(InvalidRequestError)
    async def invalid_request(_request: Request, error: InvalidRequestError):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST, content={"detail": str(error)}
        )

    @app.exception_handler(ShardUnavailableError)
    async def shard_unavailable(_request: Request, error: ShardUnavailableError):
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": str(error)},
        )
