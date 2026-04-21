from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from config import settings
from db import get_db
from dependencies.auth import require_admin
from models import RefreshToken, User, UserGroup
from schemas.user import (
    CreateGroupRequest,
    CreateUserRequest,
    GroupLargeRowsLimitUpdate,
    GroupOut,
    UserActiveUpdate,
    UserGroupUpdate,
    UserLargeRowsLimitUpdate,
    UserOut,
)
from services.auth_service import (
    AuthServiceError,
    create_user as auth_create_user,
    reset_user_device as auth_reset_user_device,
    set_user_active as auth_set_user_active,
)
from services.check_group_sync import (
    delete_group_mirrors_postgres,
    sync_user_group_membership_postgres,
)
from services.check_postgres import count_rows_for_user_ids_sync


router = APIRouter(prefix="/admin", tags=["admin"])


def _check_pg_dsn() -> str | None:
    u = (settings.check_postgres_dsn or "").strip()
    return u or None


def _sync_pg_membership(user_id: int, group_id: int | None) -> None:
    dsn = _check_pg_dsn()
    if dsn:
        sync_user_group_membership_postgres(dsn, user_id, group_id)


def _user_out(db_user: User) -> UserOut:
    return UserOut(
        id=db_user.id,
        username=db_user.username,
        is_admin=db_user.is_admin,
        is_active=db_user.is_active,
        device_id=db_user.device_id,
        group_id=db_user.group_id,
        group_name=db_user.group.name if db_user.group else None,
        max_stored_large_rows=(
            int(db_user.max_stored_large_rows)
            if db_user.max_stored_large_rows is not None
            else None
        ),
        used_stored_large_rows=0,
    )


def _user_used_rows_map(db: Session, admin: User, user_ids: list[int]) -> dict[int, int]:
    dsn = _check_pg_dsn()
    if not dsn or not user_ids:
        return {int(uid): 0 for uid in user_ids}
    out: dict[int, int] = {}
    for uid in user_ids:
        try:
            out[int(uid)] = count_rows_for_user_ids_sync(
                dsn, int(admin.id), bool(admin.is_admin), [int(uid)]
            )
        except Exception:
            out[int(uid)] = 0
    return out


def _group_used_rows_map(db: Session, admin: User, group_ids: list[int]) -> dict[int, int]:
    dsn = _check_pg_dsn()
    if not dsn or not group_ids:
        return {int(gid): 0 for gid in group_ids}
    out: dict[int, int] = {}
    for gid in group_ids:
        members = [int(u.id) for u in db.query(User).filter(User.group_id == gid).all()]
        if not members:
            out[int(gid)] = 0
            continue
        try:
            out[int(gid)] = count_rows_for_user_ids_sync(
                dsn, int(admin.id), bool(admin.is_admin), members
            )
        except Exception:
            out[int(gid)] = 0
    return out


def _ensure_group_quota_allows_membership(
    db: Session,
    *,
    member_user_id: int,
    target_group_id: int | None,
    requester_user_id: int,
    requester_is_admin: bool,
) -> None:
    if target_group_id is None:
        return
    g = db.get(UserGroup, target_group_id)
    if g is None:
        raise HTTPException(status_code=400, detail="المجموعة غير موجودة")
    limit = int(getattr(g, "max_stored_large_rows", 0) or 0)
    if limit <= 0:
        return
    dsn = _check_pg_dsn()
    if not dsn:
        return
    current_ids = [int(u.id) for u in db.query(User).filter(User.group_id == target_group_id).all()]
    if member_user_id not in current_ids:
        current_ids.append(int(member_user_id))
    try:
        total_rows = count_rows_for_user_ids_sync(
            dsn, requester_user_id, requester_is_admin, current_ids
        )
    except Exception:
        raise HTTPException(
            status_code=500, detail="تعذّر التحقق من رصيد صفوف المجموعة حالياً."
        ) from None
    if total_rows > limit:
        raise HTTPException(
            status_code=400,
            detail=(
                "لا يمكن إتمام العملية: بيانات المستخدم + بيانات المجموعة ستتجاوز حد المجموعة المسموح."
            ),
        )


@router.post("/users", response_model=UserOut)
async def create_user(
    payload: CreateUserRequest,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    _ensure_group_quota_allows_membership(
        db,
        member_user_id=-1,  # new user has no stored rows yet
        target_group_id=payload.group_id,
        requester_user_id=_admin.id,
        requester_is_admin=bool(_admin.is_admin),
    )
    try:
        user = auth_create_user(
            db=db,
            username=payload.username,
            password=payload.password,
            is_admin=payload.is_admin,
            group_id=payload.group_id,
            max_stored_large_rows=payload.max_stored_large_rows,
        )
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    u = (
        db.query(User)
        .options(joinedload(User.group))
        .filter(User.id == user.id)
        .first()
    )
    assert u is not None
    _sync_pg_membership(u.id, u.group_id)
    return _user_out(u)


@router.get("/users", response_model=list[UserOut])
async def list_users(
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(User)
        .options(joinedload(User.group))
        .order_by(User.id.asc())
        .all()
    )
    used_map = _user_used_rows_map(db, admin, [int(u.id) for u in rows])
    out: list[UserOut] = []
    for u in rows:
        item = _user_out(u)
        item.used_stored_large_rows = int(used_map.get(int(u.id), 0))
        out.append(item)
    return out


@router.get("/groups", response_model=list[GroupOut])
async def list_groups(
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    rows = db.query(UserGroup).order_by(UserGroup.id.asc()).all()
    used_map = _group_used_rows_map(db, admin, [int(g.id) for g in rows])
    return [
        GroupOut(
            id=int(g.id),
            name=str(g.name),
            max_stored_large_rows=(
                int(g.max_stored_large_rows)
                if g.max_stored_large_rows is not None
                else None
            ),
            used_stored_large_rows=int(used_map.get(int(g.id), 0)),
        )
        for g in rows
    ]


@router.post("/groups", response_model=GroupOut)
async def create_group(
    payload: CreateGroupRequest,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="اسم المجموعة مطلوب")
    g = UserGroup(name=name, max_stored_large_rows=int(payload.max_stored_large_rows))
    db.add(g)
    try:
        db.commit()
        db.refresh(g)
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=409, detail="يوجد مجموعة بنفس الاسم مسبقاً"
        ) from None
    return g


@router.delete("/groups/{group_id}")
async def delete_group(
    group_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    g = db.get(UserGroup, group_id)
    if not g:
        raise HTTPException(status_code=404, detail="المجموعة غير موجودة")
    users = db.query(User).filter(User.group_id == group_id).all()
    for u in users:
        u.group_id = None
        db.add(u)
    db.delete(g)
    db.commit()
    dsn = _check_pg_dsn()
    if dsn:
        delete_group_mirrors_postgres(dsn, group_id)
    return {"deleted": True, "group_id": group_id}


@router.patch("/users/{user_id}/group", response_model=UserOut)
async def update_user_group(
    user_id: int,
    payload: UserGroupUpdate,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    if payload.group_id is not None:
        _ensure_group_quota_allows_membership(
            db,
            member_user_id=user_id,
            target_group_id=payload.group_id,
            requester_user_id=_admin.id,
            requester_is_admin=bool(_admin.is_admin),
        )
    target = (
        db.query(User)
        .options(joinedload(User.group))
        .filter(User.id == user_id)
        .first()
    )
    if not target:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")
    target.group_id = payload.group_id
    db.add(target)
    db.commit()
    db.refresh(target)
    _sync_pg_membership(target.id, target.group_id)
    u = (
        db.query(User)
        .options(joinedload(User.group))
        .filter(User.id == user_id)
        .first()
    )
    assert u is not None
    return _user_out(u)


@router.patch("/users/{user_id}/rows-limit", response_model=UserOut)
async def update_user_rows_limit(
    user_id: int,
    payload: UserLargeRowsLimitUpdate,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    target = (
        db.query(User)
        .options(joinedload(User.group))
        .filter(User.id == user_id)
        .first()
    )
    if not target:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود")
    target.max_stored_large_rows = int(payload.max_stored_large_rows)
    db.add(target)
    db.commit()
    db.refresh(target)
    return _user_out(target)


@router.patch("/groups/{group_id}/rows-limit", response_model=GroupOut)
async def update_group_rows_limit(
    group_id: int,
    payload: GroupLargeRowsLimitUpdate,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    g = db.get(UserGroup, group_id)
    if not g:
        raise HTTPException(status_code=404, detail="المجموعة غير موجودة")
    new_limit = int(payload.max_stored_large_rows)
    dsn = _check_pg_dsn()
    if dsn:
        member_ids = [int(u.id) for u in db.query(User).filter(User.group_id == group_id).all()]
        if member_ids:
            try:
                cur_rows = count_rows_for_user_ids_sync(
                    dsn, _admin.id, bool(_admin.is_admin), member_ids
                )
            except Exception:
                raise HTTPException(
                    status_code=500, detail="تعذّر التحقق من رصيد صفوف المجموعة حالياً."
                ) from None
            if cur_rows > new_limit:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "لا يمكن تقليل حد المجموعة: البيانات الحالية تتجاوز الحد الجديد."
                    ),
                )
    g.max_stored_large_rows = new_limit
    db.add(g)
    db.commit()
    db.refresh(g)
    return g


@router.patch("/users/{user_id}", response_model=UserOut)
async def update_user_active(
    user_id: int,
    payload: UserActiveUpdate,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    try:
        u = auth_set_user_active(db=db, admin=admin, user_id=user_id, is_active=payload.is_active)
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    u2 = (
        db.query(User)
        .options(joinedload(User.group))
        .filter(User.id == u.id)
        .first()
    )
    assert u2 is not None
    return _user_out(u2)


@router.post("/users/{user_id}/reset-device", response_model=UserOut)
async def reset_device(
    user_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    try:
        u = auth_reset_user_device(db=db, user_id=user_id)
    except AuthServiceError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    u2 = (
        db.query(User)
        .options(joinedload(User.group))
        .filter(User.id == u.id)
        .first()
    )
    assert u2 is not None
    return _user_out(u2)


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

    _sync_pg_membership(user_id, None)

    db.query(RefreshToken).filter(RefreshToken.user_id == user_id).delete(
        synchronize_session=False
    )
    db.delete(user)
    db.commit()
    return {"deleted": True, "user_id": user_id}
