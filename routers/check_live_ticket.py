"""Mint one-time tickets for WebSocket /ws/check-live (no JWT in URL)."""

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from dependencies.auth import get_current_user
from models import User
from services.rate_limit import limiter
from services.ws_check_live_ticket import WsTicketRedisRequired, mint_ticket

router = APIRouter(prefix="/api/check-live", tags=["check-live"])


class WsTicketOut(BaseModel):
    ticket: str = Field(..., min_length=16)
    expires_in: int = Field(..., ge=15, le=300)


@router.post("/ws-ticket", response_model=WsTicketOut)
@limiter.limit("30/minute")
async def mint_check_live_ws_ticket(
    request: Request,
    user: User = Depends(get_current_user),
):
    try:
        ticket, ttl = mint_ticket(user.id)
    except WsTicketRedisRequired as e:
        raise HTTPException(status_code=503, detail=e.detail) from e
    return WsTicketOut(ticket=ticket, expires_in=ttl)
