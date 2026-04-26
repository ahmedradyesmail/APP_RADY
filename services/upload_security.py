"""
# SECURITY FIX: centralized upload size checks for Excel/audio endpoints.
"""

import os
import tempfile

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


async def save_upload_to_temp_with_limit(
    file: UploadFile,
    *,
    max_bytes: int,
    max_mb: int,
    prefix: str,
    suffix: str = "",
) -> str:
    """
    Stream upload to a temp file with size checks to avoid large RAM allocations.
    Caller is responsible for deleting the returned file path.
    """
    fd, tmp_path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    os.close(fd)
    written = 0
    try:
        with open(tmp_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Maximum allowed size is {max_mb} MB.",
                    )
                out.write(chunk)
        return tmp_path
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    finally:
        await file.close()
