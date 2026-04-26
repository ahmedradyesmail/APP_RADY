import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import sessionmaker

from core.plate_checker_ws import handle_plate_checker_client
from db import engine
from models import User
from services.ws_check_live_ticket import consume_ticket

logger = logging.getLogger(__name__)

SessionLocal = sessionmaker(bind=engine)

router = APIRouter(tags=["check-live"])


@router.websocket("/ws/check-live")
async def check_live_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    ticket = (websocket.query_params.get("ticket") or "").strip()
    if not ticket:
        await websocket.close(code=4401, reason="missing ticket")
        return
    user_id = consume_ticket(ticket)
    if user_id is None:
        await websocket.close(code=4401, reason="invalid or used ticket")
        return
    with SessionLocal() as db:
        user = db.get(User, user_id)
    if user is None or not user.is_active:
        await websocket.close(code=4401, reason="invalid ticket")
        return
    try:
        await handle_plate_checker_client(websocket, user.id, bool(user.is_admin))
    except WebSocketDisconnect:
        logger.debug("Live check WS disconnect user_id=%s", user.id)
    except Exception:
        logger.exception("Live check WS error user_id=%s", user.id)
