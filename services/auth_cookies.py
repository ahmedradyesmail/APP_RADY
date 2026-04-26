"""HttpOnly cookies for JWT access/refresh (browser session; XSS cannot read tokens)."""

from __future__ import annotations

from starlette.responses import Response

from config import settings


def _cookie_common() -> dict:
    st = (settings.auth_cookie_samesite or "lax").strip().lower()
    if st not in ("lax", "strict", "none"):
        st = "lax"
    secure = bool(settings.auth_cookie_secure)
    if st == "none" and not secure:
        secure = True
    return {
        "path": "/",
        "httponly": True,
        "secure": secure,
        "samesite": st,
    }


def set_auth_cookies(response: Response, access_token: str, refresh_token: str) -> None:
    common = _cookie_common()
    access_max = max(60, int(settings.access_token_expire_minutes) * 60)
    refresh_max = max(3600, int(settings.refresh_token_expire_days) * 86400)
    response.set_cookie(
        settings.auth_cookie_access_name,
        access_token,
        max_age=access_max,
        **common,
    )
    response.set_cookie(
        settings.auth_cookie_refresh_name,
        refresh_token,
        max_age=refresh_max,
        **common,
    )


def clear_auth_cookies(response: Response) -> None:
    common = _cookie_common()
    response.delete_cookie(settings.auth_cookie_access_name, **common)
    response.delete_cookie(settings.auth_cookie_refresh_name, **common)
