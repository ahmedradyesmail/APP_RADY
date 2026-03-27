"""Job status store: Redis when REDIS_URL is set (multi-worker), else in-process dict."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

from config import settings

logger = logging.getLogger(__name__)

try:
    import redis.asyncio as redis_async
except ImportError:
    redis_async = None

KEY_PREFIX = "tafreegh:job:"

_redis: Any = None
_use_redis = False
_memory: dict[str, dict[str, Any]] = {}

TTL_PROCESSING_SEC = 3600
TTL_TERMINAL_SEC = 600


def new_job_id() -> str:
    return str(uuid.uuid4())


def _key(job_id: str) -> str:
    return f"{KEY_PREFIX}{job_id}"


async def init_job_store() -> None:
    """Connect to Redis if redis_url is configured."""
    global _redis, _use_redis
    url = (settings.redis_url or "").strip()
    if not url:
        _redis = None
        _use_redis = False
        logger.info("job_store: using in-memory store (REDIS_URL not set)")
        return
    if redis_async is None:
        raise RuntimeError("redis package is required when REDIS_URL is set. pip install redis")
    _redis = redis_async.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=30,
    )
    await _redis.ping()
    _use_redis = True
    logger.info("job_store: using Redis for shared job state")


async def close_job_store() -> None:
    global _redis, _use_redis
    if _redis is not None:
        try:
            await _redis.aclose()
        except Exception:
            logger.exception("job_store: error closing Redis client")
        _redis = None
    _use_redis = False


async def job_save(
    job_id: str,
    payload: dict[str, Any],
    *,
    ttl_seconds: int = TTL_PROCESSING_SEC,
) -> None:
    if _use_redis and _redis is not None:
        await _redis.setex(_key(job_id), ttl_seconds, json.dumps(payload, default=str))
    else:
        _memory[job_id] = payload


async def job_get(job_id: str) -> dict[str, Any] | None:
    if _use_redis and _redis is not None:
        raw = await _redis.get(_key(job_id))
        if raw is None:
            return None
        return json.loads(raw)
    return _memory.get(job_id)


def schedule_job_cleanup(job_id: str, delay_sec: float = float(TTL_TERMINAL_SEC)) -> None:
    """Expire in-memory jobs after delay. Redis uses TTL on job_save instead."""

    if _use_redis:
        return

    async def _cleanup() -> None:
        await asyncio.sleep(delay_sec)
        _memory.pop(job_id, None)

    asyncio.create_task(_cleanup())
