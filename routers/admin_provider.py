"""Admin: Gemini model catalog, API key pools (Redis)."""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from db import get_db
from dependencies.auth import require_admin
from models import User
from models.provider_config import GeminiModelCatalog
from services.gemini_catalog import list_gemini_models_sync
from services.provider_keys import (
    admin_add_key,
    admin_delete_key,
    admin_list_pools,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/provider", tags=["admin-provider"])


class GeminiModelCreate(BaseModel):
    channel: str = Field(..., pattern="^(rest|live)$")
    model_id: str = Field(..., min_length=2, max_length=200)
    label: str | None = Field(None, max_length=200)
    sort_order: int = 0


class KeyAddPayload(BaseModel):
    value: str = Field(..., min_length=1, max_length=8000)


@router.get("/gemini-models")
async def admin_list_models(
    channel: str | None = None,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    if channel and channel not in ("rest", "live"):
        raise HTTPException(status_code=400, detail="channel must be rest|live")
    return list_gemini_models_sync(db, channel)


@router.post("/gemini-models")
async def admin_add_model(
    payload: GeminiModelCreate,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    row = GeminiModelCatalog(
        channel=payload.channel,
        model_id=payload.model_id.strip(),
        label=(payload.label or "").strip() or None,
        enabled=True,
        sort_order=int(payload.sort_order),
    )
    db.add(row)
    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("admin_add_model commit failed")
        raise HTTPException(status_code=400, detail="duplicate or invalid model")
    db.refresh(row)
    return {"id": row.id, "channel": row.channel, "model_id": row.model_id}


@router.delete("/gemini-models/{model_row_id}")
async def admin_delete_model(
    model_row_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    row = db.query(GeminiModelCatalog).filter(GeminiModelCatalog.id == model_row_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    db.delete(row)
    db.commit()
    return {"deleted": True, "id": model_row_id}


@router.get("/key-pools")
async def admin_get_key_pools(
    _admin: User = Depends(require_admin),
) -> dict[str, Any]:
    return admin_list_pools()


@router.post("/key-pools/{kind}/keys")
async def admin_post_key(
    kind: str,
    payload: KeyAddPayload,
    _admin: User = Depends(require_admin),
):
    try:
        return admin_add_key(kind, payload.value)
    except ValueError as e:
        msg = str(e)
        if msg == "redis_unconfigured":
            raise HTTPException(
                status_code=503,
                detail="Redis غير مضبوط — عيّن REDIS_URL في البيئة.",
            ) from e
        if msg == "invalid_kind":
            raise HTTPException(status_code=400, detail="kind must be gemini|ors|gmaps") from e
        raise HTTPException(status_code=400, detail=msg) from e


@router.delete("/key-pools/{kind}/keys/{key_id}")
async def admin_remove_key(
    kind: str,
    key_id: str,
    _admin: User = Depends(require_admin),
):
    try:
        admin_delete_key(kind, key_id)
    except ValueError as e:
        if str(e) == "redis_unconfigured":
            raise HTTPException(status_code=503, detail="Redis غير مضبوط.") from e
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"deleted": True}


# Deprecated: kept so old clients do not 404
@router.get("/api-keys")
async def admin_get_api_keys_deprecated(
    _admin: User = Depends(require_admin),
) -> dict[str, str]:
    return {
        "gemini_api_key": "",
        "ors_api_key": "",
        "gmaps_api_key": "",
        "message": "استخدم GET /admin/provider/key-pools",
    }


@router.put("/api-keys")
async def admin_put_api_keys_deprecated(
    _admin: User = Depends(require_admin),
):
    raise HTTPException(
        status_code=410,
        detail="تم استبدال حفظ المفاتيح — استخدم POST /admin/provider/key-pools/{kind}/keys",
    )
