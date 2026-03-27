from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from db import get_db
from dependencies.auth import require_admin
from models import User
from schemas.user import CreateUserRequest, UserActiveUpdate, UserOut
from services.auth_service import (
    AuthServiceError,
    create_user as auth_create_user,
    reset_user_device as auth_reset_user_device,
    set_user_active as auth_set_user_active,
)


router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/users", response_model=UserOut)
async def create_user(
    payload: CreateUserRequest,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    try:
        return auth_create_user(
            db=db,
            username=payload.username,
            password=payload.password,
            is_admin=payload.is_admin,
        )
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/users", response_model=list[UserOut])
async def list_users(
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    return db.query(User).order_by(User.id.asc()).all()


@router.patch("/users/{user_id}", response_model=UserOut)
async def update_user_active(
    user_id: int,
    payload: UserActiveUpdate,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    try:
        return auth_set_user_active(db=db, admin=admin, user_id=user_id, is_active=payload.is_active)
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.post("/users/{user_id}/reset-device", response_model=UserOut)
async def reset_device(
    user_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    try:
        return auth_reset_user_device(db=db, user_id=user_id)
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    if user_id == admin.id:
        raise HTTPException(status_code=400, detail="لا يمكن حذف حسابك الحالي")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")

    if user.is_admin:
        admins_count = db.query(User).filter(User.is_admin == True).count()  # noqa: E712
        if admins_count <= 1:
            raise HTTPException(status_code=400, detail="لا يمكن حذف آخر Admin في النظام")

    db.delete(user)
    db.commit()
    return {"deleted": True, "user_id": user_id}
