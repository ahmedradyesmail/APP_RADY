import json
import logging

from fastapi import APIRouter, Depends, Form, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

from dependencies.auth import get_current_user
from services.gemini import process_audio
from services.upload_security import MAX_AUDIO_BYTES, read_upload_with_limit


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api",
    tags=["audio"],
    dependencies=[Depends(get_current_user)],
)


@router.post("/process")
async def process(
    api_key:       str        = Form(...),
    model_name:    str        = Form("gemini-2.5-flash"),
    recorder_name: str        = Form(""),
    sheet_name:    str        = Form("بيانات المركبات"),
    gps_data:      str        = Form("[]"),
    audio:         UploadFile = File(...),
):
    api_key    = api_key.strip()
    model_name = model_name.strip() or "gemini-2.5-flash"

    if not api_key:
        raise HTTPException(status_code=400, detail="أدخل مفتاح Gemini API")

    try:
        gps_points = json.loads(gps_data)
    except Exception:
        gps_points = []

    # SECURITY FIX: file size limit to prevent DoS via large uploads
    file_content = await read_upload_with_limit(audio, MAX_AUDIO_BYTES, 10)

    try:
        plates = await process_audio(
            file_content=file_content,
            filename=audio.filename or "audio.mp3",
            api_key=api_key,
            model_name=model_name,
            recorder_name=recorder_name.strip(),
            sheet_name=sheet_name.strip(),
            gps_points=gps_points,
        )
    except Exception:
        # SECURITY FIX: hiding internal exception details from client
        logger.exception("Audio processing failed")
        raise HTTPException(
            status_code=500,
            detail="An internal error occurred. Please try again.",
        )

    return JSONResponse({"plates": plates, "total": len(plates)})