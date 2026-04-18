"""Admin-only visibility into Postgres-backed الفرز imports."""

import asyncio
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from starlette.background import BackgroundTask

from config import settings
from dependencies.auth import require_admin
from models.user import User
from services.check_postgres import (
    admin_get_import_meta_sync,
    admin_list_check_storage_sync,
    admin_list_imports_detailed_sync,
    admin_list_import_rows_page_sync,
    admin_write_import_csv_tempfile_sync,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/check-storage", tags=["admin-check-storage"])


def _check_pg_url() -> str:
    u = (settings.check_postgres_dsn or "").strip()
    if not u:
        raise HTTPException(
            status_code=503,
            detail="CHECK_POSTGRES_URL is not configured.",
        )
    return u


@router.get("/summary")
async def check_storage_summary(_admin: User = Depends(require_admin)):
    url = _check_pg_url()
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


@router.get("/imports")
async def check_storage_imports(_admin: User = Depends(require_admin)):
    url = _check_pg_url()
    try:
        items = await asyncio.to_thread(
            admin_list_imports_detailed_sync, url, _admin.id, True
        )
    except Exception:
        logger.exception("admin_list_imports_detailed_sync failed")
        raise HTTPException(
            status_code=500,
            detail="Failed to list imports from Postgres.",
        )
    return JSONResponse({"items": items})


@router.get("/import/{import_id}/rows")
async def check_storage_import_rows(
    import_id: int,
    _admin: User = Depends(require_admin),
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=500),
):
    url = _check_pg_url()
    if import_id <= 0:
        raise HTTPException(status_code=400, detail="invalid import_id")
    try:
        rows, total = await asyncio.to_thread(
            admin_list_import_rows_page_sync,
            url,
            _admin.id,
            True,
            import_id,
            offset,
            limit,
        )
    except Exception:
        logger.exception("admin_list_import_rows_page_sync failed")
        raise HTTPException(
            status_code=500,
            detail="Failed to read import rows.",
        )
    if total == 0:
        meta = await asyncio.to_thread(
            admin_get_import_meta_sync, url, _admin.id, True, import_id
        )
        if meta is None:
            raise HTTPException(status_code=404, detail="Import not found.")
    return JSONResponse({"import_id": import_id, "total": total, "offset": offset, "rows": rows})


@router.get("/import/{import_id}/export.csv")
async def check_storage_export_csv(import_id: int, _admin: User = Depends(require_admin)):
    url = _check_pg_url()
    if import_id <= 0:
        raise HTTPException(status_code=400, detail="invalid import_id")
    try:
        path, fname = await asyncio.to_thread(
            admin_write_import_csv_tempfile_sync, url, _admin.id, True, import_id
        )
    except ValueError as e:
        if "not found" in str(e).lower() or "import not" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e)) from e
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception:
        logger.exception("admin_write_import_csv_tempfile_sync failed")
        raise HTTPException(
            status_code=500,
            detail="Failed to build CSV export.",
        )

    def _unlink_safe(p: str) -> None:
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass

    return FileResponse(
        path,
        media_type="text/csv; charset=utf-8",
        filename=fname,
        background=BackgroundTask(_unlink_safe, path),
    )
