"""Gemini model catalog helpers (admin DB). Not for API key storage."""

from __future__ import annotations

from sqlalchemy.orm import Session

from db import SessionLocal
from models.provider_config import GeminiModelCatalog


def is_gemini_model_allowed_sync(channel: str, model_id: str) -> bool:
    mid = (model_id or "").strip()
    if not mid or channel not in ("rest", "live"):
        return False
    with SessionLocal() as db:
        return (
            db.query(GeminiModelCatalog)
            .filter(
                GeminiModelCatalog.channel == channel,
                GeminiModelCatalog.enabled.is_(True),
                GeminiModelCatalog.model_id == mid,
            )
            .first()
            is not None
        )


def list_gemini_models_sync(db: Session, channel: str | None) -> list[dict]:
    q = db.query(GeminiModelCatalog)
    if channel in ("rest", "live"):
        q = q.filter(GeminiModelCatalog.channel == channel)
    rows = q.order_by(
        GeminiModelCatalog.channel.asc(),
        GeminiModelCatalog.sort_order.asc(),
        GeminiModelCatalog.id.asc(),
    ).all()
    return [
        {
            "id": r.id,
            "channel": r.channel,
            "model_id": r.model_id,
            "label": r.label,
            "enabled": bool(r.enabled),
            "sort_order": r.sort_order,
        }
        for r in rows
    ]


def list_public_gemini_models_sync(channel: str) -> list[dict]:
    with SessionLocal() as db:
        rows = (
            db.query(GeminiModelCatalog)
            .filter(
                GeminiModelCatalog.channel == channel,
                GeminiModelCatalog.enabled.is_(True),
            )
            .order_by(GeminiModelCatalog.sort_order.asc(), GeminiModelCatalog.id.asc())
            .all()
        )
        return [
            {"id": r.id, "model_id": r.model_id, "label": r.label or r.model_id}
            for r in rows
        ]
