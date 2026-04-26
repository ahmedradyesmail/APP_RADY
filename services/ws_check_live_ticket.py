"""
One-time WebSocket tickets for /ws/check-live (avoid JWT in query string).

Storage: Redis when REDIS_URL is set (required when effective_app_worker_count > 1);
otherwise in-process dict (single worker / dev only).
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
from typing import Any

from config import settings

logger = logging.getLogger(__name__)

PREFIX = "tafreegh:wsclive:ticket"


class WsTicketRedisRequired(Exception):
    """Raised when multiple workers need Redis for ticket storage but Redis is missing or failing."""

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(detail)

# Redis: GET+DEL in one script so the ticket is consumed exactly once.
_REDIS_CONSUME_LUA = """
local v = redis.call('GET', KEYS[1])
if v == false then return nil end
redis.call('DEL', KEYS[1])
return v
"""

_lock = threading.Lock()
_memory: dict[str, tuple[float, int]] = {}


def _ticket_key(raw: str) -> str:
    return f"{PREFIX}:{raw}"


def _clamp_ttl_seconds(raw: int) -> int:
    return max(15, min(300, int(raw)))


def _multi_worker_mode() -> bool:
    return settings.effective_app_worker_count > 1


def mint_ticket(user_id: int) -> tuple[str, int]:
    """
    Create a new one-time ticket. Returns (opaque_ticket, ttl_seconds).
    """
    ttl = _clamp_ttl_seconds(settings.ws_check_live_ticket_ttl_seconds)
    token = secrets.token_urlsafe(32)
    multi = _multi_worker_mode()
    r: Any = None
    try:
        from services.provider_key_pool import get_sync_redis

        r = get_sync_redis()
    except Exception:
        logger.debug("ws ticket: redis unavailable", exc_info=True)
        r = None
    if multi and not r:
        raise WsTicketRedisRequired(
            "REDIS_URL مطلوب عند تشغيل أكثر من worker — تذاكر WebSocket لا تُشارك بين العمليات بدون Redis. "
            "REDIS_URL is required for multiple workers (WS tickets are not shared without Redis)."
        )
    exp = time.time() + ttl
    if r:
        try:
            r.set(_ticket_key(token), str(int(user_id)), ex=ttl)
            return token, ttl
        except Exception as e:
            if multi:
                raise WsTicketRedisRequired(
                    "فشل تخزين تذكرة WebSocket في Redis — تحقق من REDIS_URL. Redis SET failed for WS ticket."
                ) from e
            logger.warning("ws ticket: Redis SET failed, falling back to memory", exc_info=True)
    with _lock:
        _memory_prune_unlocked()
        _memory[token] = (exp, int(user_id))
    return token, ttl


def _memory_consume(raw: str) -> int | None:
    now = time.time()
    with _lock:
        _memory_prune_unlocked()
        item = _memory.pop(raw, None)
        if not item:
            return None
        exp, uid = item
        if now > exp:
            return None
        return int(uid)


def consume_ticket(token: str) -> int | None:
    """
    Validate and consume a ticket. Returns user_id or None if invalid/expired/used.
    """
    raw = (token or "").strip()
    if len(raw) < 16:
        return None
    key = _ticket_key(raw)
    r: Any = None
    multi = _multi_worker_mode()
    try:
        from services.provider_key_pool import get_sync_redis

        r = get_sync_redis()
    except Exception:
        r = None
    if multi and not r:
        return None
    if r:
        try:
            uid_s = r.eval(_REDIS_CONSUME_LUA, 1, key)
            if uid_s is not None:
                s = uid_s.decode() if isinstance(uid_s, bytes) else str(uid_s)
                if s.isdigit():
                    return int(s)
        except Exception:
            logger.warning("ws ticket: Redis consume failed", exc_info=True)
        if not multi:
            return _memory_consume(raw)
        return None
    return _memory_consume(raw)


def _memory_prune_unlocked() -> None:
    if len(_memory) < 2000:
        return
    now = time.time()
    dead = [k for k, (exp, _) in _memory.items() if exp < now]
    for k in dead[:500]:
        _memory.pop(k, None)
