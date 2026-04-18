"""Admin: Gemini model catalog, API keys (in-memory only)."""

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
from services.provider_keys import set_all_keys, snapshot_for_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/provider", tags=["admin-provider"])


class GeminiModelCreate(BaseModel):
    channel: str = Field(..., pattern="^(rest|live)$")
    model_id: str = Field(..., min_length=2, max_length=200)
    label: str | None = Field(None, max_length=200)
    sort_order: int = 0


class ProviderKeysPayload(BaseModel):
    gemini_api_key: str = ""
    ors_api_key: str = ""
    gmaps_api_key: str = ""


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


@router.get("/api-keys")
async def admin_get_api_keys(
    _admin: User = Depends(require_admin),
) -> dict[str, Any]:
    return snapshot_for_admin()


@router.put("/api-keys")
async def admin_put_api_keys(
    payload: ProviderKeysPayload,
    _admin: User = Depends(require_admin),
):
    set_all_keys(
        payload.gemini_api_key,
        payload.ors_api_key,
        payload.gmaps_api_key,
    )
    return {"ok": True}
