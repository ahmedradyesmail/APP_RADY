"""
# SECURITY FIX: centralized upload size checks for Excel/audio endpoints.
"""

from fastapi import HTTPException, UploadFile


# SECURITY FIX: explicit file-size ceilings to reduce DoS risk.
MAX_EXCEL_BYTES = 30 * 1024 * 1024
MAX_AUDIO_BYTES = 10 * 1024 * 1024


# SECURITY FIX: validate upload size before expensive parsing/transcoding.
async def read_upload_with_limit(file: UploadFile, max_bytes: int, max_mb: int) -> bytes:
    content = await file.read()
    if len(content) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum allowed size is {max_mb} MB.",
        )
    return content
