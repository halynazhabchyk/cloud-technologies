from fastapi import APIRouter

from app.api.routes.records import router as records_router
from app.api.routes.replication import router as replication_router

router = APIRouter()
router.include_router(records_router)
router.include_router(replication_router)

__all__ = ["router"]
