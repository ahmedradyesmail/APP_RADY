"""
Provider API keys: Redis pools (Gemini, ORS, Google Maps) with round-robin,
park on 429 until next midnight UTC, remove invalid keys permanently.

Requires REDIS_URL. Legacy data/provider_keys.json is no longer read.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Awaitable, Callable, TypeVar

from services.provider_key_pool import (
    delete_key_forever,
    get_sync_redis,
    has_any_key,
    iter_round_robin,
    list_keys_detail,
    park_until_midnight_utc,
    peek_one_key,
    promote_parked_keys,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _r():
    return get_sync_redis()


def has_any_gemini_keys() -> bool:
    r = _r()
    return bool(r and has_any_key(r, "gemini"))


def has_any_ors_keys() -> bool:
    r = _r()
    return bool(r and has_any_key(r, "ors"))


def has_any_gmaps_keys() -> bool:
    r = _r()
    return bool(r and has_any_key(r, "gmaps"))


def get_gemini_api_key_sync() -> str | None:
    """First available Gemini key (no rotation). For compatibility / quick checks."""
    r = _r()
    if not r:
        return None
    return peek_one_key(r, "gemini")


def get_ors_api_key_sync() -> str | None:
    r = _r()
    if not r:
        return None
    return peek_one_key(r, "ors")


def get_gmaps_api_key_sync() -> str | None:
    """Next Maps key in rotation (one RPOPLPUSH cycle step)."""
    r = _r()
    if not r:
        return None
    promote_parked_keys(r, "gmaps")
    for _uid, sec in iter_round_robin(r, "gmaps"):
        return sec
    return None


def classify_gemini_error(exc: BaseException) -> str:
    """quota | invalid | other"""
    t = f"{type(exc).__name__} {exc!s}".lower()
    if (
        "429" in t
        or "resource_exhausted" in t
        or "quota" in t
        or "rate limit" in t
        or "too many requests" in t
    ):
        return "quota"
    if (
        "api key not valid" in t
        or "invalid api key" in t
        or "permission denied" in t
        or "api_key_invalid" in t
        or re.search(r"\b401\b", t)
        and "key" in t
    ):
        return "invalid"
    if "400" in t and ("api key" in t or "apikey" in t or "invalid" in t):
        return "invalid"
    return "other"


def classify_ors_http(status_code: int, _body: Any) -> str:
    if status_code == 429:
        return "quota"
    if status_code in (401, 403):
        return "invalid"
    return "other"


async def async_gemini_try_all(
    factory: Callable[[str], Awaitable[T]],
) -> tuple[T | None, BaseException | None]:
    """
    Try each Gemini key in rotation order until success or exhaustion.
    On quota: park key until midnight UTC. On invalid: delete key.
    """
    r = _r()
    if not r:
        return None, RuntimeError("no_redis")

    last_exc: BaseException | None = None
    for key_id, api_key in iter_round_robin(r, "gemini"):
        try:
            out = await factory(api_key)
            return out, None
        except Exception as e:
            last_exc = e
            bucket = classify_gemini_error(e)
            if bucket == "quota":
                park_until_midnight_utc(r, "gemini", key_id)
                logger.warning(
                    "Gemini 429/quota — parked key id=%s until next UTC midnight",
                    key_id[:8],
                )
                continue
            if bucket == "invalid":
                delete_key_forever(r, "gemini", key_id)
                logger.warning("Gemini invalid API key removed id=%s", key_id[:8])
                continue
            return None, e
    if last_exc is not None:
        return None, last_exc
    return None, RuntimeError("no_gemini_key")


def admin_list_pools() -> dict[str, Any]:
    r = _r()
    if not r:
        return {
            "redis": False,
            "gemini": [],
            "ors": [],
            "gmaps": [],
            "detail": "REDIS_URL غير مضبوط",
        }
    return {
        "redis": True,
        "gemini": list_keys_detail(r, "gemini"),
        "ors": list_keys_detail(r, "ors"),
        "gmaps": list_keys_detail(r, "gmaps"),
    }


def admin_add_key(kind: str, value: str) -> dict[str, Any]:
    from services.provider_key_pool import add_key

    r = _r()
    if not r:
        raise ValueError("redis_unconfigured")
    k = (kind or "").strip().lower()
    if k not in ("gemini", "ors", "gmaps"):
        raise ValueError("invalid_kind")
    uid = add_key(r, k, value)
    return {"id": uid, "kind": k}


def admin_delete_key(kind: str, key_id: str) -> None:
    r = _r()
    if not r:
        raise ValueError("redis_unconfigured")
    k = (kind or "").strip().lower()
    if k not in ("gemini", "ors", "gmaps"):
        raise ValueError("invalid_kind")
    delete_key_forever(r, k, (key_id or "").strip())


# Legacy no-op for old imports
def snapshot_for_admin() -> dict[str, str]:
    return {
        "gemini_api_key": "",
        "ors_api_key": "",
        "gmaps_api_key": "",
        "legacy": "use /admin/provider/key-pools",
    }


def set_all_keys(
    gemini_api_key: str, ors_api_key: str, gmaps_api_key: str
) -> None:
    """Deprecated: keys are managed via Redis pools only."""
    _ = (gemini_api_key, ors_api_key, gmaps_api_key)
    logger.warning("set_all_keys ignored — use admin key-pools API")
