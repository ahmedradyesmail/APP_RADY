from __future__ import annotations

from sqlalchemy import update
from sqlalchemy.orm import Session

from models import RefreshToken, User
from services.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    hash_token,
    token_exp_to_datetime,
    verify_password,
)


class AuthServiceError(Exception):
    def __init__(self, *, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def login(
    *,
    db: Session,
    username: str,
    password: str,
    device_id: str,
) -> tuple[str, str, bool]:
    """
    Returns (access_token, refresh_token, is_admin).

    Non-admin users are bound to exactly one device on first successful login.
    """
    user = db.query(User).filter(User.username == username).first()
    if not user or not verify_password(password, user.password_hash):
        raise AuthServiceError(status_code=401, detail="Invalid username or password")

    if not user.is_active:
        raise AuthServiceError(status_code=403, detail="Account is disabled")

    if not user.is_admin:
        if user.device_id is None:
            user.device_id = device_id
            db.add(user)
            db.commit()
        elif user.device_id != device_id:
            raise AuthServiceError(status_code=403, detail="This account is linked to another device")

    subject = str(user.id)
    # SECURITY FIX: refresh token rotation with DB validation.
    refresh_token = create_refresh_token(subject)
    refresh_payload = decode_token(refresh_token)
    db.add(
        RefreshToken(
            token_hash=hash_token(refresh_token),
            user_id=user.id,
            device_id=device_id,
            expires_at=token_exp_to_datetime(refresh_payload),
            is_revoked=False,
        )
    )
    db.commit()
    return (
        create_access_token(subject),
        refresh_token,
        user.is_admin,
    )


def refresh(
    *,
    db: Session,
    refresh_token: str,
    device_id: str,
) -> tuple[str, str, bool]:
    """Returns (access_token, refresh_token, is_admin)."""
    try:
        token_payload = decode_token(refresh_token)
    except Exception as e:
        raise AuthServiceError(status_code=401, detail="Invalid refresh token") from e

    if token_payload.get("type") != "refresh":
        raise AuthServiceError(status_code=401, detail="Invalid token type")

    subject = token_payload.get("sub")
    if not subject or not str(subject).isdigit():
        raise AuthServiceError(status_code=401, detail="Invalid token subject")

    user = db.get(User, int(subject))
    if user is None:
        raise AuthServiceError(status_code=401, detail="User not found")

    if not user.is_active:
        raise AuthServiceError(status_code=403, detail="Account is disabled")

    if not user.is_admin and user.device_id != device_id:
        raise AuthServiceError(status_code=403, detail="Token refresh not allowed from this device")

    # SECURITY FIX: refresh token rotation with DB validation.
    token_hash = hash_token(refresh_token)
    token_row = (
        db.query(RefreshToken)
        .filter(RefreshToken.token_hash == token_hash)
        .first()
    )
    if token_row is None:
        raise AuthServiceError(status_code=401, detail="Invalid refresh token")
    if token_row.is_revoked:
        raise AuthServiceError(status_code=401, detail="Invalid refresh token")
    if token_row.user_id != user.id:
        raise AuthServiceError(status_code=401, detail="Invalid refresh token")
    if token_row.device_id != device_id:
        raise AuthServiceError(status_code=401, detail="Invalid refresh token")

    user_subject = str(user.id)
    # SECURITY FIX: refresh token rotation with DB validation.
    token_row.is_revoked = True
    next_refresh_token = create_refresh_token(user_subject)
    next_payload = decode_token(next_refresh_token)
    db.add(
        RefreshToken(
            token_hash=hash_token(next_refresh_token),
            user_id=user.id,
            device_id=device_id,
            expires_at=token_exp_to_datetime(next_payload),
            is_revoked=False,
        )
    )
    db.add(token_row)
    db.commit()
    return (
        create_access_token(user_subject),
        next_refresh_token,
        user.is_admin,
    )


def create_user(
    *,
    db: Session,
    username: str,
    password: str,
    is_admin: bool = False,
) -> User:
    existing = db.query(User).filter(User.username == username).first()
    if existing:
        raise AuthServiceError(status_code=409, detail="Username already exists")

    user = User(
        username=username,
        password_hash=hash_password(password),
        is_admin=is_admin,
        is_active=True,
        device_id=None,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def set_user_active(
    *,
    db: Session,
    admin: User,
    user_id: int,
    is_active: bool,
) -> User:
    if admin.id == user_id and not is_active:
        raise AuthServiceError(status_code=400, detail="You cannot deactivate your own account")

    target = db.get(User, user_id)
    if target is None:
        raise AuthServiceError(status_code=404, detail="User not found")

    target.is_active = is_active
    db.add(target)
    # SECURITY FIX: refresh token rotation with DB validation.
    if not is_active:
        db.execute(
            update(RefreshToken)
            .where(RefreshToken.user_id == target.id)
            .values(is_revoked=True)
        )
    db.commit()
    db.refresh(target)
    return target


def reset_user_device(
    *,
    db: Session,
    user_id: int,
) -> User:
    target = db.get(User, user_id)
    if target is None:
        raise AuthServiceError(status_code=404, detail="User not found")

    target.device_id = None
    db.add(target)
    db.commit()
    db.refresh(target)
    return target


# SECURITY FIX: refresh token rotation with DB validation.
def revoke_user_device_tokens(
    *,
    db: Session,
    user_id: int,
    device_id: str,
) -> None:
    db.execute(
        update(RefreshToken)
        .where(
            RefreshToken.user_id == user_id,
            RefreshToken.device_id == device_id,
            RefreshToken.is_revoked == False,  # noqa: E712
        )
        .values(is_revoked=True)
    )
    db.commit()

