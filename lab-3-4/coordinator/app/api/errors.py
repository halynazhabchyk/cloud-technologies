from fastapi import FastAPI, Request, status

from common.errors import error_response
from coordinator.app.services.errors import (
    InvalidRequestError,
    ShardUnavailableError,
    WriteConflictError,
)


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(InvalidRequestError)
    async def invalid_request(_request: Request, error: InvalidRequestError):
        return error_response(status.HTTP_400_BAD_REQUEST, str(error))

    @app.exception_handler(WriteConflictError)
    async def write_conflict(_request: Request, error: WriteConflictError):
        return error_response(status.HTTP_409_CONFLICT, str(error))

    @app.exception_handler(ShardUnavailableError)
    async def shard_unavailable(_request: Request, error: ShardUnavailableError):
        return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, str(error))
