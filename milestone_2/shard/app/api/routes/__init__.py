"""HTTP route definitions for the storage shard."""

from fastapi import APIRouter

from app.api.routes.records import router as records_router

router = APIRouter()
router.include_router(records_router)

__all__ = ["router"]
