from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session, joinedload

from config import settings
from db import get_db
from dependencies.auth import get_current_user, require_device_header
from models import User
from schemas.auth import AuthSessionOut, LoginRequest, MeOut
from services.auth_cookies import clear_auth_cookies, set_auth_cookies
from services.auth_service import (
    AuthServiceError,
    login as auth_login,
    refresh as auth_refresh,
    revoke_user_device_tokens,
)
from services.rate_limit import limiter


router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me", response_model=MeOut)
async def me(
    current: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    u = (
        db.query(User)
        .options(joinedload(User.group))
        .filter(User.id == current.id)
        .first()
    )
    if not u:
        raise HTTPException(status_code=401, detail="User not found")
    return MeOut(
        username=u.username,
        is_admin=u.is_admin,
        group_id=u.group_id,
        group_name=u.group.name if u.group else None,
    )


@router.post("/login", response_model=AuthSessionOut)
# SECURITY FIX: rate limited to prevent brute-force
@limiter.limit("5/minute")
async def login(
    request: Request,
    payload: LoginRequest,
    x_device_id: str = Depends(require_device_header),
    db: Session = Depends(get_db),
):
    try:
        access_token, refresh_token, is_admin = auth_login(
            db=db,
            username=payload.username,
            password=payload.password,
            device_id=x_device_id,
        )
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    body = AuthSessionOut(is_admin=is_admin).model_dump()
    resp = JSONResponse(content=body)
    set_auth_cookies(resp, access_token, refresh_token)
    return resp


@router.post("/refresh", response_model=AuthSessionOut)
# SECURITY FIX: rate limited to prevent brute-force
@limiter.limit("10/minute")
async def refresh_token(
    request: Request,
    x_device_id: str = Depends(require_device_header),
    db: Session = Depends(get_db),
):
    rt = ""
    try:
        body = await request.json()
        if isinstance(body, dict) and body.get("refresh_token"):
            rt = str(body["refresh_token"]).strip()
    except Exception:
        pass
    if not rt:
        rt = (request.cookies.get(settings.auth_cookie_refresh_name) or "").strip()
    if not rt:
        raise HTTPException(status_code=401, detail="Missing refresh token")
    try:
        access_token, refresh_token, is_admin = auth_refresh(
            db=db,
            refresh_token=rt,
            device_id=x_device_id,
        )
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    body = AuthSessionOut(is_admin=is_admin).model_dump()
    resp = JSONResponse(content=body)
    set_auth_cookies(resp, access_token, refresh_token)
    return resp


@router.post("/logout")
async def logout(
    current: User = Depends(get_current_user),
    x_device_id: str = Depends(require_device_header),
    db: Session = Depends(get_db),
):
    revoke_user_device_tokens(db=db, user_id=current.id, device_id=x_device_id)
    resp = JSONResponse(content={"detail": "Logged out successfully"})
    clear_auth_cookies(resp)
    return resp
