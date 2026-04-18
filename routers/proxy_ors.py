"""Proxy OpenRouteService directions using server ORS key pool (Redis)."""

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from dependencies.auth import get_current_user
from models import User
from services.provider_key_pool import (
    delete_key_forever,
    get_sync_redis,
    iter_round_robin,
    park_until_midnight_utc,
    promote_parked_keys,
)
from services.provider_keys import classify_ors_http, has_any_ors_keys

logger = logging.getLogger(__name__)

ORS_URL = "https://api.openrouteservice.org/v2/directions/driving-car"

router = APIRouter(prefix="/api/proxy", tags=["proxy"])


@router.post("/ors/directions")
async def proxy_ors_directions(
    payload: dict[str, Any],
    _user: User = Depends(get_current_user),
):
    if not has_any_ors_keys():
        raise HTTPException(status_code=503, detail="خدمة المسارات غير متاحة مؤقتاً.")

    r = get_sync_redis()
    if not r:
        raise HTTPException(status_code=503, detail="خدمة المسارات غير متاحة مؤقتاً.")

    promote_parked_keys(r, "ors")
    last_status = 503
    for key_id, api_key in iter_round_robin(r, "ors"):
        async with httpx.AsyncClient(timeout=45.0) as client:
            try:
                resp = await client.post(
                    ORS_URL,
                    headers={
                        "Authorization": api_key,
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
            except Exception as e:
                logger.warning("ORS proxy request failed: %s", e)
                raise HTTPException(
                    status_code=503, detail="خدمة المسارات غير متاحة مؤقتاً."
                ) from e
        bucket = classify_ors_http(resp.status_code, None)
        if bucket == "quota":
            park_until_midnight_utc(r, "ors", key_id)
            logger.warning("ORS 429 — parked key id=%s", key_id[:8])
            last_status = 429
            continue
        if bucket == "invalid":
            delete_key_forever(r, "ors", key_id)
            logger.warning("ORS invalid key removed id=%s", key_id[:8])
            last_status = 401
            continue
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:500]}
        return JSONResponse(content=body, status_code=resp.status_code)

    raise HTTPException(
        status_code=429 if last_status == 429 else 503,
        detail="خدمة المسارات غير متاحة — جرّب لاحقاً أو راجع مفاتيح ORS.",
    )
