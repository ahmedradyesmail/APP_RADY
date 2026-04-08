"""Persist تفريغ الصوت (Gemini) JSON on disk; job store only holds metadata."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from config import settings
from services.check_result_storage import job_id_valid

logger = logging.getLogger(__name__)


def _root() -> Path:
    return Path(settings.transcribe_results_dir).resolve()


def transcribe_path_for_job(job_id: str) -> Path | None:
    jid = (job_id or "").strip()
    if not job_id_valid(jid):
        return None
    return _root() / f"{jid}.json"


def ensure_transcribe_dir() -> Path:
    r = _root()
    r.mkdir(parents=True, exist_ok=True)
    return r


def write_transcribe_json_sync(job_id: str, payload: dict[str, Any]) -> None:
    path = transcribe_path_for_job(job_id)
    if path is None:
        raise ValueError("invalid job_id")
    ensure_transcribe_dir()
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    path.write_bytes(raw)


def delete_transcribe_file_sync(job_id: str) -> None:
    path = transcribe_path_for_job(job_id)
    if path is None or not path.is_file():
        return
    try:
        path.unlink()
    except OSError:
        logger.warning("transcribe_result_storage: could not delete %s", path, exc_info=True)


def schedule_transcribe_file_cleanup(job_id: str, delay_sec: float) -> None:
    async def _cleanup() -> None:
        await asyncio.sleep(delay_sec)
        await asyncio.to_thread(delete_transcribe_file_sync, job_id)

    asyncio.create_task(_cleanup())
