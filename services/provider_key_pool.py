"""
Multi-key API key pools in Redis: round-robin (atomic RPOPLPUSH), park on 429 until
next midnight UTC, permanent delete on invalid key. Three kinds: gemini, ors, gmaps.

Requires REDIS_URL in environment (same Redis as job_store is fine).
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from config import settings

logger = logging.getLogger(__name__)

PREFIX = "tafreegh:pk"

KINDS = frozenset({"gemini", "ors", "gmaps"})

_lock = threading.Lock()
_redis: Any = None


def _order_key(kind: str) -> str:
    return f"{PREFIX}:{kind}:order"


def _secrets_key(kind: str) -> str:
    return f"{PREFIX}:{kind}:secrets"


def _parked_key(kind: str) -> str:
    return f"{PREFIX}:{kind}:parked"


def get_sync_redis():
    """Singleton sync Redis client; None if REDIS_URL unset."""
    global _redis
    url = (settings.redis_url or "").strip()
    if not url:
        return None
    with _lock:
        if _redis is None:
            import redis as redis_sync

            _redis = redis_sync.Redis.from_url(
                url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=30,
            )
            _redis.ping()
    return _redis


def next_midnight_utc_ts() -> float:
    now = datetime.now(timezone.utc)
    tomorrow = datetime.combine(now.date(), time.min, tzinfo=timezone.utc) + timedelta(
        days=1
    )
    return tomorrow.timestamp()


def promote_parked_keys(r, kind: str) -> None:
    """Move keys whose park-unlock time has passed back to the rotation list."""
    if kind not in KINDS:
        return
    zkey = _parked_key(kind)
    okey = _order_key(kind)
    now = time.time()
    due = r.zrangebyscore(zkey, "-inf", now)
    for uid in due:
        r.lpush(okey, uid)
        r.zrem(zkey, uid)


def is_parked(r, kind: str, uid: str) -> bool:
    zkey = _parked_key(kind)
    sc = r.zscore(zkey, uid)
    if sc is None:
        return False
    return float(sc) > time.time()


def park_until_midnight_utc(r, kind: str, uid: str) -> None:
    """Remove uid from order list and park until next midnight UTC."""
    if kind not in KINDS:
        return
    r.lrem(_order_key(kind), 0, uid)
    unlock = next_midnight_utc_ts()
    r.zadd(_parked_key(kind), {uid: unlock})


def delete_key_forever(r, kind: str, uid: str) -> None:
    if kind not in KINDS:
        return
    r.lrem(_order_key(kind), 0, uid)
    r.hdel(_secrets_key(kind), uid)
    r.zrem(_parked_key(kind), uid)


def add_key(r, kind: str, secret: str) -> str:
    if kind not in KINDS:
        raise ValueError("invalid kind")
    s = (secret or "").strip()
    if not s:
        raise ValueError("empty key")
    uid = str(uuid.uuid4())
    r.hset(_secrets_key(kind), uid, s)
    r.lpush(_order_key(kind), uid)
    return uid


def list_keys_detail(r, kind: str) -> list[dict[str, Any]]:
    """Admin list: id, masked secret, status."""
    if kind not in KINDS:
        return []
    promote_parked_keys(r, kind)
    okey = _order_key(kind)
    skey = _secrets_key(kind)
    zkey = _parked_key(kind)
    uids = r.lrange(okey, 0, -1)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for uid in uids:
        if uid in seen:
            continue
        seen.add(uid)
        val = r.hget(skey, uid) or ""
        masked = _mask(val)
        st = "active"
        parked_until = None
        sc = r.zscore(zkey, uid)
        if sc is not None and float(sc) > time.time():
            st = "parked_quota"
            parked_until = int(float(sc))
        out.append(
            {
                "id": uid,
                "masked": masked,
                "status": st,
                "parked_until_ts": parked_until,
            }
        )
    # Orphan secrets (not in list): show as orphaned
    all_h = r.hgetall(skey)
    for uid, val in all_h.items():
        if uid not in seen:
            out.append(
                {
                    "id": uid,
                    "masked": _mask(val),
                    "status": "orphan",
                    "parked_until_ts": None,
                }
            )
    return out


def _mask(s: str) -> str:
    s = s.strip()
    if len(s) <= 8:
        return "****"
    return s[:4] + "…" + s[-4:]


def has_any_key(r, kind: str) -> bool:
    if not r or kind not in KINDS:
        return False
    promote_parked_keys(r, kind)
    return r.llen(_order_key(kind)) > 0


def iter_round_robin(
    r, kind: str
) -> Iterator[tuple[str, str]]:
    """
    One full cycle of RPOPLPUSH over the order list, skipping parked keys.
    Yields (key_id, secret) for each active entry once per generator run.
    """
    if not r or kind not in KINDS:
        return
    promote_parked_keys(r, kind)
    okey = _order_key(kind)
    skey = _secrets_key(kind)
    n = r.llen(okey)
    if n == 0:
        return
    seen_cycle: set[str] = set()
    for _ in range(n):
        uid = r.rpoplpush(okey, okey)
        if not uid or uid in seen_cycle:
            break
        seen_cycle.add(uid)
        if is_parked(r, kind, uid):
            continue
        sec = r.hget(skey, uid)
        if sec:
            yield uid, sec


def peek_one_key(r, kind: str) -> str | None:
    """First active secret without rotating the pool (health checks)."""
    if not r or kind not in KINDS:
        return None
    promote_parked_keys(r, kind)
    okey = _order_key(kind)
    skey = _secrets_key(kind)
    for uid in r.lrange(okey, 0, -1):
        if is_parked(r, kind, uid):
            continue
        sec = r.hget(skey, uid)
        if sec:
            return sec
    return None
