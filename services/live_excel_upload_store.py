"""Temporary disk-backed Excel upload references for Live WS flow."""

from __future__ import annotations

import secrets
import threading
import time
from typing import Any

from services.provider_key_pool import get_sync_redis

PREFIX = "tafreegh:live_excel_upload"
TTL_SEC = 10 * 60
_lock = threading.Lock()
_memory: dict[str, tuple[float, str]] = {}
_POP_LUA = """
local v = redis.call('GET', KEYS[1])
if v == false then return nil end
redis.call('DEL', KEYS[1])
return v
"""


def _k(token: str) -> str:
    return f"{PREFIX}:{token}"


def put_upload_path(tmp_path: str) -> tuple[str, int]:
    token = secrets.token_urlsafe(24)
    r: Any = get_sync_redis()
    if r:
        r.setex(_k(token), TTL_SEC, tmp_path)
        return token, TTL_SEC
    exp = time.time() + TTL_SEC
    with _lock:
        _memory_prune_unlocked()
        _memory[token] = (exp, tmp_path)
    return token, TTL_SEC


def pop_upload_path(token: str) -> str | None:
    raw = (token or "").strip()
    if len(raw) < 8:
        return None
    r: Any = get_sync_redis()
    if r:
        v = r.eval(_POP_LUA, 1, _k(raw))
        if v is None:
            return None
        return v.decode() if isinstance(v, bytes) else str(v)
    now = time.time()
    with _lock:
        _memory_prune_unlocked()
        item = _memory.pop(raw, None)
        if not item:
            return None
        exp, p = item
        if now > exp:
            return None
        return p


def _memory_prune_unlocked() -> None:
    if len(_memory) < 2000:
        return
    now = time.time()
    dead = [k for k, (exp, _) in _memory.items() if exp < now]
    for k in dead[:500]:
        _memory.pop(k, None)
