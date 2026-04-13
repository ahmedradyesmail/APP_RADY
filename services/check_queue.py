"""Queue for heavy check (فرز) jobs.

Uses Redis shared queue when REDIS_URL is configured; otherwise falls back to
in-process asyncio queue (dev/single-instance).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable
from typing import Any

from config import settings

logger = logging.getLogger(__name__)

try:
    import redis.asyncio as redis_async
except ImportError:
    redis_async = None


class CheckQueueFullError(Exception):
    pass


_queue: asyncio.Queue[dict] | None = None
_workers: list[asyncio.Task] = []
_stopping = False
_processor: Callable[[dict], Awaitable[None]] | None = None
_redis: Any = None
_use_redis = False
_redis_key = "tafreegh:check:queue"


def _max_depth() -> int:
    return max(1, int(settings.check_queue_max_depth))


def _workers_count() -> int:
    return max(1, int(settings.check_queue_workers))


async def queue_depth() -> int:
    if _use_redis and _redis is not None:
        try:
            return int(await _redis.llen(_redis_key))
        except Exception:
            logger.exception("check_queue: failed to read Redis queue depth")
            return 0
    q = _queue
    return 0 if q is None else q.qsize()


async def start_check_queue(
    processor: Callable[[dict], Awaitable[None]],
) -> None:
    global _queue, _workers, _stopping, _processor, _redis, _use_redis
    if _workers:
        return
    _processor = processor
    _stopping = False
    url = (settings.redis_url or "").strip()
    if url:
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
    else:
        _queue = asyncio.Queue(maxsize=_max_depth())
        _use_redis = False

    async def _worker_loop(idx: int) -> None:
        logger.info("check_queue worker-%s started (redis=%s)", idx, _use_redis)
        if _use_redis:
            assert _redis is not None
            while not _stopping:
                try:
                    pair = await _redis.brpop(_redis_key, timeout=2)
                    if not pair:
                        continue
                    _key, raw = pair
                    item = json.loads(raw)
                    if _processor is not None:
                        await _processor(item)
                except asyncio.CancelledError:
                    return
                except Exception:
                    logger.exception("check_queue worker-%s failed processing Redis item", idx)
            logger.info("check_queue worker-%s stopping", idx)
            return
        else:
            assert _queue is not None
            while True:
                item = await _queue.get()
                if item is None:  # sentinel
                    _queue.task_done()
                    logger.info("check_queue worker-%s stopping", idx)
                    return
                try:
                    if _processor is not None:
                        await _processor(item)
                except Exception:
                    logger.exception("check_queue worker-%s failed processing item", idx)
                finally:
                    _queue.task_done()

    for i in range(_workers_count()):
        _workers.append(asyncio.create_task(_worker_loop(i + 1)))
    logger.info(
        "check_queue started: workers=%s max_depth=%s",
        _workers_count(),
        _max_depth(),
    )


async def stop_check_queue() -> None:
    global _queue, _workers, _stopping, _redis, _use_redis, _processor
    if not _workers:
        return
    _stopping = True
    if _use_redis:
        for t in _workers:
            t.cancel()
    else:
        assert _queue is not None
        for _ in _workers:
            await _queue.put(None)  # sentinel
    await asyncio.gather(*_workers, return_exceptions=True)
    _workers = []
    _queue = None
    if _redis is not None:
        try:
            await _redis.aclose()
        except Exception:
            logger.exception("check_queue: error closing Redis client")
    _redis = None
    _use_redis = False
    _processor = None
    _stopping = False
    logger.info("check_queue stopped")


async def enqueue_check_job(item: dict) -> None:
    if _stopping:
        raise CheckQueueFullError("queue is stopping")
    if _use_redis:
        if _redis is None:
            raise RuntimeError("check_queue Redis client is not started")
        depth = int(await _redis.llen(_redis_key))
        if depth >= _max_depth():
            raise CheckQueueFullError("check queue is full")
        await _redis.lpush(_redis_key, json.dumps(item, ensure_ascii=False))
        return
    if _queue is None:
        raise RuntimeError("check_queue is not started")
    try:
        _queue.put_nowait(item)
    except asyncio.QueueFull as e:
        raise CheckQueueFullError("check queue is full") from e
