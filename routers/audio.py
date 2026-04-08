import asyncio
import json
import logging

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from dependencies.auth import get_current_user
from services.check_result_storage import job_id_valid
from services.gemini import process_audio
from services.job_store import (
    TTL_PROCESSING_SEC,
    TTL_TERMINAL_SEC,
    job_get,
    job_save,
    new_job_id,
    schedule_job_cleanup,
)
from services.transcribe_result_storage import (
    schedule_transcribe_file_cleanup,
    transcribe_path_for_job,
    write_transcribe_json_sync,
)
from services.upload_security import MAX_AUDIO_BYTES, read_upload_with_limit


logger = logging.getLogger(__name__)

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
    file_content: bytes,
    filename: str,
    api_key: str,
    model_name: str,
    recorder_name: str,
    sheet_name: str,
    gps_points: list,
) -> None:
    async with _audio_job_semaphore:
        try:
            plates = await process_audio(
                file_content=file_content,
                filename=filename,
                api_key=api_key,
                model_name=model_name,
                recorder_name=recorder_name,
                sheet_name=sheet_name,
                gps_points=gps_points,
            )
            payload = {"plates": plates, "total": len(plates)}
            try:
                await asyncio.to_thread(write_transcribe_json_sync, job_id, payload)
            except Exception:
                logger.exception("Failed to write transcribe result file job_id=%s", job_id)
                await job_save(
                    job_id,
                    {
                        "status": "error",
                        "data": None,
                        "detail": "تعذّر حفظ نتيجة التفريغ على الخادم.",
                    },
                    ttl_seconds=TTL_TERMINAL_SEC,
                )
            else:
                await job_save(
                    job_id,
                    {
                        "status": "done",
                        "data": {
                            "kind": "transcribe",
                            "storage": "file",
                            "total": len(plates),
                        },
                    },
                    ttl_seconds=TTL_TERMINAL_SEC,
                )
                schedule_transcribe_file_cleanup(job_id, float(TTL_TERMINAL_SEC))
        except Exception:
            logger.exception("Audio processing failed")
            await job_save(
                job_id,
                {
                    "status": "error",
                    "data": None,
                    "detail": "An internal error occurred. Please try again.",
                },
                ttl_seconds=TTL_TERMINAL_SEC,
            )
        schedule_job_cleanup(job_id)


@router.post("/process")
async def process(
    background_tasks: BackgroundTasks,
    api_key: str = Form(...),
    model_name: str = Form("gemini-2.5-flash"),
    recorder_name: str = Form(""),
    sheet_name: str = Form("بيانات المركبات"),
    gps_data: str = Form("[]"),
    audio: UploadFile = File(...),
):
    api_key = api_key.strip()
    model_name = model_name.strip() or "gemini-2.5-flash"

    if not api_key:
        raise HTTPException(status_code=400, detail="أدخل مفتاح Gemini API")

    try:
        gps_points = json.loads(gps_data)
    except Exception:
        gps_points = []

    # SECURITY FIX: file size limit to prevent DoS via large uploads
    file_content = await read_upload_with_limit(audio, MAX_AUDIO_BYTES, 10)

    job_id = new_job_id()
    await job_save(
        job_id,
        {"status": "processing", "data": None},
        ttl_seconds=TTL_PROCESSING_SEC,
    )
    background_tasks.add_task(
        _audio_job_task,
        job_id,
        file_content,
        audio.filename or "audio.mp3",
        api_key,
        model_name,
        recorder_name.strip(),
        sheet_name.strip(),
        gps_points,
    )
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/transcribe/status/{job_id}")
async def transcribe_status(job_id: str):
    row = await job_get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(row)


@router.get("/transcribe/result/{job_id}")
async def transcribe_result_payload(job_id: str):
    """Full plates JSON from disk (job row only references this file)."""
    if not job_id_valid(job_id):
        raise HTTPException(status_code=400, detail="Invalid job id")
    row = await job_get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    if row.get("status") != "done":
        raise HTTPException(status_code=409, detail="Result not ready")
    meta = row.get("data") or {}
    if meta.get("storage") == "file" and meta.get("kind") == "transcribe":
        path = transcribe_path_for_job(job_id)
        if path is None or not path.is_file():
            raise HTTPException(status_code=404, detail="Result file expired or missing")
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("transcribe result read failed job_id=%s", job_id)
            raise HTTPException(
                status_code=500,
                detail="An internal error occurred. Please try again.",
            )
        return JSONResponse(obj)
    # Legacy jobs: plates still inline in Redis/memory
    return JSONResponse(meta)
