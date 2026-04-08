import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import sessionmaker

from core.plate_checker_ws import handle_plate_checker_client
from db import engine
from dependencies.ws_auth import get_user_from_access_token

logger = logging.getLogger(__name__)

SessionLocal = sessionmaker(bind=engine)

router = APIRouter(tags=["check-live"])


@router.websocket("/ws/check-live")
async def check_live_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    token = (websocket.query_params.get("token") or "").strip()
    if not token:
        await websocket.close(code=4401, reason="missing token")
        return
    with SessionLocal() as db:
        user = get_user_from_access_token(token, db)
    if user is None:
        await websocket.close(code=4401, reason="invalid token")
        return
    try:
        await handle_plate_checker_client(websocket)
    except WebSocketDisconnect:
        logger.debug("Live check WS disconnect user_id=%s", user.id)
    except Exception:
        logger.exception("Live check WS error user_id=%s", user.id)
