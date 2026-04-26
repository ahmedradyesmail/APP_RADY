"""HTTP upload endpoint for Live Excel; WS receives one-time reference token."""

from fastapi import APIRouter, Depends, File, UploadFile
from pydantic import BaseModel, Field

from dependencies.auth import get_current_user
from models import User
from services.live_excel_upload_store import put_upload_path
from services.upload_security import MAX_EXCEL_BYTES, save_upload_to_temp_with_limit

router = APIRouter(prefix="/api/check-live", tags=["check-live"])


class LiveExcelUploadOut(BaseModel):
    upload_token: str = Field(..., min_length=8)
    expires_in: int = Field(..., ge=60, le=3600)


@router.post("/upload-excel", response_model=LiveExcelUploadOut)
async def upload_live_excel(
    file: UploadFile = File(...),
    _user: User = Depends(get_current_user),
):
    tmp_path = await save_upload_to_temp_with_limit(
        file,
        max_bytes=MAX_EXCEL_BYTES,
        max_mb=30,
        prefix="live_excel_",
        suffix=".xlsx",
    )
    token, ttl = put_upload_path(tmp_path)
    return LiveExcelUploadOut(upload_token=token, expires_in=ttl)
