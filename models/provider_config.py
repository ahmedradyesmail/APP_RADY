"""Gemini model catalog (admin). API keys: in-process only via Admin (see services/provider_keys.py)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db import Base


class GeminiModelCatalog(Base):
    """Admin-defined Gemini model IDs per channel (REST vs Live)."""

    __tablename__ = "gemini_model_catalog"
    __table_args__ = (UniqueConstraint("channel", "model_id", name="uq_gemini_channel_model"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(8), index=True, nullable=False)  # rest | live
    model_id: Mapped[str] = mapped_column(String(200), nullable=False)
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
