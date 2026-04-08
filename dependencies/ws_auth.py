"""Authenticate WebSocket connections using the same JWT as REST."""

from sqlalchemy.orm import Session

from db import engine
from models import User
from services.security import decode_token


def get_user_from_access_token(token: str, db: Session) -> User | None:
    if not (token or "").strip():
        return None
    try:
        payload = decode_token(token.strip())
    except Exception:
        return None
    if payload.get("type") != "access":
        return None
    subject = payload.get("sub")
    if not subject or not str(subject).isdigit():
        return None
    user = db.get(User, int(subject))
    if user is None or not user.is_active:
        return None
    return user
