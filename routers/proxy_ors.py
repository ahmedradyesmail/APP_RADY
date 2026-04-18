"""Proxy OpenRouteService directions using server ORS key from DB."""

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from dependencies.auth import get_current_user
from models import User
from services.provider_keys import get_ors_api_key_sync

logger = logging.getLogger(__name__)

ORS_URL = "https://api.openrouteservice.org/v2/directions/driving-car"

router = APIRouter(prefix="/api/proxy", tags=["proxy"])


@router.post("/ors/directions")
async def proxy_ors_directions(
    payload: dict[str, Any],
    _user: User = Depends(get_current_user),
):
    api_key = get_ors_api_key_sync()
    if not api_key:
        raise HTTPException(status_code=503, detail="خدمة المسارات غير متاحة مؤقتاً.")

    async with httpx.AsyncClient(timeout=45.0) as client:
        try:
            r = await client.post(
                ORS_URL,
                headers={
                    "Authorization": api_key,
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        except Exception as e:
            logger.warning("ORS proxy request failed: %s", e)
            raise HTTPException(status_code=503, detail="خدمة المسارات غير متاحة مؤقتاً.") from e
        try:
            body = r.json()
        except Exception:
            body = {"raw": r.text[:500]}
        return JSONResponse(content=body, status_code=r.status_code)
