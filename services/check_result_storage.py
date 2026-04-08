"""Persist فرز (check) Excel output on disk; job payload stays small (no base64)."""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from config import settings

logger = logging.getLogger(__name__)

_JOB_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _results_root() -> Path:
    return Path(settings.check_results_dir).resolve()


def job_id_valid(job_id: str) -> bool:
    return bool(_JOB_ID_RE.match((job_id or "").strip()))


def result_path_for_job(job_id: str) -> Path | None:
    jid = (job_id or "").strip()
    if not job_id_valid(jid):
        return None
    return _results_root() / f"{jid}.xlsx"


def ensure_check_results_dir() -> Path:
    root = _results_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def write_result_file_sync(job_id: str, content: bytes) -> None:
    path = result_path_for_job(job_id)
    if path is None:
        raise ValueError("invalid job_id")
    ensure_check_results_dir()
    path.write_bytes(content)


def delete_result_file_sync(job_id: str) -> None:
    path = result_path_for_job(job_id)
    if path is None or not path.is_file():
        return
    try:
        path.unlink()
    except OSError:
        logger.warning("check_result_storage: could not delete %s", path, exc_info=True)


def schedule_result_file_cleanup(job_id: str, delay_sec: float) -> None:
    """Remove on-disk result after TTL (same window as job payload expiry)."""

    async def _cleanup() -> None:
        await asyncio.sleep(delay_sec)
        await asyncio.to_thread(delete_result_file_sync, job_id)

    asyncio.create_task(_cleanup())
