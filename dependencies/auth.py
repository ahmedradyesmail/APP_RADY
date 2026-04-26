from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from config import settings
from db import get_db
from models import User
from services.security import decode_token


optional_bearer = HTTPBearer(auto_error=False)


def _raw_access_token(
    request: Request,
    bearer: HTTPAuthorizationCredentials | None,
) -> str | None:
    if bearer and bearer.credentials:
        t = bearer.credentials.strip()
        if t:
            return t
    c = (request.cookies.get(settings.auth_cookie_access_name) or "").strip()
    return c or None


def get_current_user(
    request: Request,
    db: Session = Depends(get_db),
    bearer: HTTPAuthorizationCredentials | None = Depends(optional_bearer),
    x_device_id: str | None = Header(default=None, alias="X-Device-Id"),
) -> User:
    token = _raw_access_token(request, bearer)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    if payload.get("type") != "access":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token type")

    subject = payload.get("sub")
    if not subject or not str(subject).isdigit():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token subject")

    user = db.get(User, int(subject))
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Account is disabled")
    # Enforce single-device usage for non-admin users on all authenticated routes.
    if not user.is_admin:
        if not x_device_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="X-Device-Id header is required",
            )
        if not user.device_id or user.device_id != x_device_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This account is linked to another device",
            )
    return user


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if not current_user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return current_user


def require_device_header(x_device_id: str | None = Header(default=None, alias="X-Device-Id")) -> str:
    if not x_device_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Device-Id header is required",
        )
    return x_device_id
