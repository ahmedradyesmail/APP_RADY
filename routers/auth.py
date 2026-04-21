from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session, joinedload

from db import get_db
from dependencies.auth import get_current_user, require_device_header
from models import User
from schemas.auth import LoginRequest, MeOut, RefreshRequest, TokenResponse
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


@router.post("/login", response_model=TokenResponse)
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

    return TokenResponse(access_token=access_token, refresh_token=refresh_token, is_admin=is_admin)


@router.post("/refresh", response_model=TokenResponse)
# SECURITY FIX: rate limited to prevent brute-force
@limiter.limit("10/minute")
async def refresh_token(
    request: Request,
    payload: RefreshRequest,
    x_device_id: str = Depends(require_device_header),
    db: Session = Depends(get_db),
):
    try:
        access_token, refresh_token, is_admin = auth_refresh(
            db=db,
            refresh_token=payload.refresh_token,
            device_id=x_device_id,
        )
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    return TokenResponse(access_token=access_token, refresh_token=refresh_token, is_admin=is_admin)


@router.post("/logout")
async def logout(
    _request: Request,
    current: User = Depends(get_current_user),
    x_device_id: str = Depends(require_device_header),
    db: Session = Depends(get_db),
):
    # SECURITY FIX: refresh token rotation with DB validation.
    revoke_user_device_tokens(db=db, user_id=current.id, device_id=x_device_id)
    return {"detail": "Logged out successfully"}
