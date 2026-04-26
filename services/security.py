import hashlib
import secrets
from datetime import datetime, timedelta, timezone

import jwt
from passlib.context import CryptContext

from config import settings


pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


def create_token(subject: str, token_type: str, expires_delta: timedelta) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "type": token_type,
        "iat": now,
        "exp": now + expires_delta,
        # Unique per issuance so parallel logins / same-second refresh never collide on token_hash.
        "jti": secrets.token_urlsafe(24),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def create_access_token(subject: str) -> str:
    expire = timedelta(minutes=settings.access_token_expire_minutes)
    return create_token(subject=subject, token_type="access", expires_delta=expire)


def create_refresh_token(subject: str) -> str:
    expire = timedelta(days=settings.refresh_token_expire_days)
    return create_token(subject=subject, token_type="refresh", expires_delta=expire)


def decode_token(token: str) -> dict:
    return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])


# SECURITY FIX: hash refresh tokens before storing in database.
def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


# SECURITY FIX: normalize JWT exp claim to timezone-aware datetime.
def token_exp_to_datetime(payload: dict) -> datetime:
    exp = payload.get("exp")
    if isinstance(exp, datetime):
        return exp if exp.tzinfo else exp.replace(tzinfo=timezone.utc)
    if isinstance(exp, (int, float)):
        return datetime.fromtimestamp(exp, tz=timezone.utc)
    raise ValueError("Invalid token exp claim")
