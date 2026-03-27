import asyncio
import base64
import logging

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from dependencies.auth import get_current_user
from services.check_match import run_check_plates_sync
from services.excel_utils import (
    find_best_sheet_async,
    load_workbook_from_bytes_async,
    load_workbook_maybe_encrypted_async,
)
from services.job_store import (
    TTL_PROCESSING_SEC,
    TTL_TERMINAL_SEC,
    job_get,
    job_save,
    new_job_id,
    schedule_job_cleanup,
)
from services.plate_utils import auto_detect_plate_col
from services.upload_security import MAX_EXCEL_BYTES, read_upload_with_limit


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api",
    tags=["check"],
    dependencies=[Depends(get_current_user)],
)


def _get_headers(ws) -> list[str]:
    for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
        return [str(h).strip() if h is not None else "" for h in row]
    return []


@router.post("/check-headers")
async def check_headers(
    large_file: UploadFile | None = File(None),
    small_file: UploadFile | None = File(None),
    password: str = Form(""),
):
    result = {}

    if large_file:
        try:
            # SECURITY FIX: file size limit to prevent DoS via large uploads
            content = await read_upload_with_limit(large_file, MAX_EXCEL_BYTES, 30)
            wb = await load_workbook_maybe_encrypted_async(content, password.strip())
            ws = await find_best_sheet_async(wb)
            headers = _get_headers(ws)
            result["large"] = {
                "headers": headers,
                "detected": auto_detect_plate_col(headers),
                "sheet_name": ws.title,
                "all_sheets": wb.sheetnames,
            }
        except Exception as e:
            result["large"] = {"error": str(e)}

    if small_file:
        try:
            # SECURITY FIX: file size limit to prevent DoS via large uploads
            content = await read_upload_with_limit(small_file, MAX_EXCEL_BYTES, 30)
            wb = await load_workbook_from_bytes_async(content)
            ws = await find_best_sheet_async(wb)
            headers = _get_headers(ws)
            result["small"] = {
                "headers": headers,
                "detected": auto_detect_plate_col(headers),
                "sheet_name": ws.title,
                "all_sheets": wb.sheetnames,
            }
        except Exception as e:
            result["small"] = {"error": str(e)}

    return JSONResponse(result)


async def _check_job_task(
    job_id: str,
    lc_bytes: bytes,
    sc_bytes: bytes,
    password: str,
    large_col: str,
    small_col: str,
    large_sheet: str,
    small_sheet: str,
) -> None:
    try:
        raw = await asyncio.to_thread(
            run_check_plates_sync,
            lc_bytes,
            sc_bytes,
            password,
            large_col,
            small_col,
            large_sheet,
            small_sheet,
        )
        if raw["kind"] == "xlsx":
            await job_save(
                job_id,
                {
                    "status": "done",
                    "data": {
                        "kind": "xlsx",
                        "filename": raw["filename"],
                        "content_b64": base64.b64encode(raw["content"]).decode("ascii"),
                    },
                },
                ttl_seconds=TTL_TERMINAL_SEC,
            )
        else:
            await job_save(
                job_id,
                {
                    "status": "done",
                    "data": {
                        "kind": "json",
                        "status_code": raw["status_code"],
                        "body": raw["body"],
                    },
                },
                ttl_seconds=TTL_TERMINAL_SEC,
            )
    except ValueError as e:
        msg = str(e)
        if "فشل فك تشفير" in msg or "فشل فك" in msg:
            logger.exception("Failed to open encrypted large workbook")
            await job_save(
                job_id,
                {
                    "status": "error",
                    "data": None,
                    "detail": "An internal error occurred. Please try again.",
                },
                ttl_seconds=TTL_TERMINAL_SEC,
            )
        else:
            await job_save(
                job_id,
                {
                    "status": "error",
                    "data": None,
                    "detail": msg,
                },
                ttl_seconds=TTL_TERMINAL_SEC,
            )
    except Exception:
        logger.exception("Check plates job failed")
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


@router.post("/check")
async def check_plates(
    background_tasks: BackgroundTasks,
    large_file: UploadFile = File(...),
    small_file: UploadFile = File(...),
    password: str = Form(""),
    large_col: str = Form(""),
    small_col: str = Form(""),
    large_sheet: str = Form(""),
    small_sheet: str = Form(""),
):
    # SECURITY FIX: file size limit to prevent DoS via large uploads
    lc_bytes = await read_upload_with_limit(large_file, MAX_EXCEL_BYTES, 30)
    # SECURITY FIX: file size limit to prevent DoS via large uploads
    sc_bytes = await read_upload_with_limit(small_file, MAX_EXCEL_BYTES, 30)

    job_id = new_job_id()
    await job_save(
        job_id,
        {"status": "processing", "data": None},
        ttl_seconds=TTL_PROCESSING_SEC,
    )
    background_tasks.add_task(
        _check_job_task,
        job_id,
        lc_bytes,
        sc_bytes,
        password.strip(),
        large_col.strip(),
        small_col.strip(),
        large_sheet.strip(),
        small_sheet.strip(),
    )
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/check/status/{job_id}")
async def check_status(job_id: str):
    row = await job_get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(row)
