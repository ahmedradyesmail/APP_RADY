import json
import logging

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from dependencies.auth import get_current_user
from services.gemini import process_audio
from services.job_store import (
    TTL_PROCESSING_SEC,
    TTL_TERMINAL_SEC,
    job_get,
    job_save,
    new_job_id,
    schedule_job_cleanup,
)
from services.upload_security import MAX_AUDIO_BYTES, read_upload_with_limit


logger = logging.getLogger(__name__)

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
        await job_save(
            job_id,
            {
                "status": "done",
                "data": {"plates": plates, "total": len(plates)},
            },
            ttl_seconds=TTL_TERMINAL_SEC,
        )
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
