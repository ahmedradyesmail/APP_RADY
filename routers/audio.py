import asyncio
import json
import logging
import os
import re

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from dependencies.auth import get_current_user
from services.gemini import process_audio
from services.gemini_catalog import is_gemini_model_allowed_sync
from services.job_store import (
    TTL_PROCESSING_SEC,
    TTL_TERMINAL_SEC,
    job_get,
    job_save,
    new_job_id,
    schedule_job_cleanup,
)
from services.provider_keys import async_gemini_try_all, has_any_gemini_keys
from services.upload_security import MAX_AUDIO_BYTES, save_upload_to_temp_with_limit


logger = logging.getLogger(__name__)
_JOB_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _job_id_valid(job_id: str) -> bool:
    return bool(_JOB_ID_RE.match((job_id or "").strip()))

# Limit concurrent Gemini/audio jobs per worker (reduces API spikes and memory).
_AUDIO_JOB_CONCURRENCY = 8
_audio_job_semaphore = asyncio.Semaphore(_AUDIO_JOB_CONCURRENCY)

router = APIRouter(
    prefix="/api",
    tags=["audio"],
    dependencies=[Depends(get_current_user)],
)


async def _audio_job_task(
    job_id: str,
    file_path: str,
    filename: str,
    model_name: str,
    recorder_name: str,
    sheet_name: str,
    gps_points: list,
) -> None:
    async with _audio_job_semaphore:
        try:
            if not has_any_gemini_keys():
                raise RuntimeError("no_gemini_key")
            plates = None
            model_err_detail: str | None = None

            async def _attempt(api_key: str):
                return await process_audio(
                    file_path=file_path,
                    filename=filename,
                    api_key=api_key,
                    model_name=model_name,
                    recorder_name=recorder_name,
                    sheet_name=sheet_name,
                    gps_points=gps_points,
                )

            plates, audio_err = await async_gemini_try_all(_attempt)
            if audio_err is not None and plates is None:
                err_text = str(audio_err or "")
                if "no_gemini_key" in err_text or "no_redis" in err_text:
                    raise RuntimeError("no_gemini_key") from audio_err
                if (
                    "404" in err_text
                    and "not found" in err_text.lower()
                    and "model" in err_text.lower()
                ):
                    model_err_detail = (
                        "موديل REST المختار غير مدعوم/غير موجود على Gemini API. "
                        "حدّث موديلات REST من صفحة Admin."
                    )
                    logger.warning("Gemini REST model error: %s", err_text[:400])
                else:
                    logger.exception("Gemini REST failed")
                    raise RuntimeError("gemini_failed") from audio_err

            if plates is None:
                if model_err_detail:
                    raise RuntimeError(f"model_not_supported::{model_err_detail}")
                raise RuntimeError("gemini_failed")

            payload = {"plates": plates, "total": len(plates)}
            await job_save(
                job_id,
                {
                    "status": "done",
                    "data": {
                        "kind": "transcribe",
                        "storage": "inline",
                        "payload": payload,
                        "total": len(plates),
                    },
                },
                ttl_seconds=TTL_TERMINAL_SEC,
            )
        except Exception as e:
            logger.exception("Audio processing failed")
            detail = "An internal error occurred. Please try again."
            if str(e) == "no_gemini_key":
                detail = "خدمة التسجيل غير متاحة مؤقتاً."
            elif str(e) == "gemini_failed":
                detail = "تعذّر التسجيل. تحقق من مفتاح Gemini في لوحة الأدمن أو أعد المحاولة لاحقاً."
            elif str(e).startswith("model_not_supported::"):
                detail = str(e).split("::", 1)[1] or "موديل REST غير مدعوم."
            await job_save(
                job_id,
                {
                    "status": "error",
                    "data": None,
                    "detail": detail,
                },
                ttl_seconds=TTL_TERMINAL_SEC,
            )
        finally:
            if file_path:
                try:
                    await asyncio.to_thread(os.unlink, file_path)
                except OSError:
                    pass
        schedule_job_cleanup(job_id)


@router.post("/process")
async def process(
    background_tasks: BackgroundTasks,
    model_name: str = Form(...),
    recorder_name: str = Form(""),
    sheet_name: str = Form("بيانات المركبات"),
    gps_data: str = Form("[]"),
    audio: UploadFile = File(...),
):
    model_name = model_name.strip()
    if not model_name:
        raise HTTPException(status_code=400, detail="اختر موديل التسجيل (REST).")

    if not await asyncio.to_thread(is_gemini_model_allowed_sync, "rest", model_name):
        raise HTTPException(
            status_code=400,
            detail="موديل REST غير مسموح أو غير مفعّل — اختر من القائمة.",
        )
    if not has_any_gemini_keys():
        raise HTTPException(
            status_code=503,
            detail="خدمة التسجيل غير متاحة — أضف مفتاح Gemini في Redis من لوحة الأدمن (REDIS_URL).",
        )

    try:
        gps_points = json.loads(gps_data)
    except Exception:
        gps_points = []

    file_name = (audio.filename or "recording.webm").strip() or "recording.webm"
    suffix = "." + file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ".webm"
    file_path = await save_upload_to_temp_with_limit(
        audio,
        max_bytes=MAX_AUDIO_BYTES,
        max_mb=10,
        prefix="audio_upload_",
        suffix=suffix,
    )
    try:
        job_id = new_job_id()
        await job_save(
            job_id,
            {"status": "processing", "data": None},
            ttl_seconds=TTL_PROCESSING_SEC,
        )
        background_tasks.add_task(
            _audio_job_task,
            job_id,
            file_path,
            file_name,
            model_name,
            recorder_name.strip(),
            sheet_name.strip(),
            gps_points,
        )
        return JSONResponse({"job_id": job_id, "status": "processing"})
    except Exception:
        try:
            await asyncio.to_thread(os.unlink, file_path)
        except OSError:
            pass
        raise


@router.get("/transcribe/status/{job_id}")
async def transcribe_status(job_id: str):
    row = await job_get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(row)


@router.get("/transcribe/result/{job_id}")
async def transcribe_result_payload(job_id: str):
    """Full plates JSON from inline job payload."""
    if not _job_id_valid(job_id):
        raise HTTPException(status_code=400, detail="Invalid job id")
    row = await job_get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    if row.get("status") != "done":
        raise HTTPException(status_code=409, detail="Result not ready")
    meta = row.get("data") or {}
    if meta.get("kind") == "transcribe":
        payload = meta.get("payload")
        if isinstance(payload, dict):
            return JSONResponse(payload)
    return JSONResponse(meta)
