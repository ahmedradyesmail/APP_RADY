"""Admin-only visibility into Postgres-backed الفرز imports."""

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from config import settings
from dependencies.auth import require_admin
from models.user import User
from services.check_postgres import admin_list_check_storage_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/check-storage", tags=["admin-check-storage"])


@router.get("/summary")
async def check_storage_summary(_admin: User = Depends(require_admin)):
    url = (settings.check_postgres_dsn or "").strip()
    if not url:
        raise HTTPException(
            status_code=503,
            detail="CHECK_POSTGRES_URL is not configured.",
        )
    try:
        rows = await asyncio.to_thread(
            admin_list_check_storage_sync, url, _admin.id, True
        )
    except Exception:
        logger.exception("admin_list_check_storage_sync failed")
        raise HTTPException(
            status_code=500,
            detail="Failed to read check storage from Postgres.",
        )
    return JSONResponse({"items": rows})
