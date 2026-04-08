import asyncio
import json
import logging

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from dependencies.auth import get_current_user
from services.check_match import run_check_plates_sync
from services.check_result_storage import (
    job_id_valid,
    result_path_for_job,
    schedule_result_file_cleanup,
    write_result_file_sync,
)
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

# Cap concurrent heavy Excel compares per worker to avoid RAM/thread pile-up under load.
_CHECK_JOB_CONCURRENCY = 4
_check_job_semaphore = asyncio.Semaphore(_CHECK_JOB_CONCURRENCY)

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


def _parse_col_list(raw: str) -> list[str]:
    s = (raw or "").strip()
    if not s:
        return []
    try:
        data = json.loads(s)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    return []


async def _check_job_task(
    job_id: str,
    lc_bytes: bytes,
    sc_bytes: bytes,
    password: str,
    large_col: str,
    small_col: str,
    large_sheet: str,
    small_sheet: str,
    large_export_cols: list[str],
    small_export_cols: list[str],
) -> None:
    async with _check_job_semaphore:
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
                large_export_cols,
                small_export_cols,
            )
            if raw["kind"] == "xlsx":
                try:
                    await asyncio.to_thread(
                        write_result_file_sync, job_id, raw["content"]
                    )
                except Exception:
                    logger.exception("Failed to write check result file job_id=%s", job_id)
                    await job_save(
                        job_id,
                        {
                            "status": "error",
                            "data": None,
                            "detail": "تعذّر حفظ ملف النتيجة على الخادم.",
                        },
                        ttl_seconds=TTL_TERMINAL_SEC,
                    )
                else:
                    data_out: dict = {
                        "kind": "xlsx",
                        "filename": raw["filename"],
                        "storage": "file",
                    }
                    if raw.get("preview"):
                        data_out["preview"] = raw["preview"]
                    await job_save(
                        job_id,
                        {
                            "status": "done",
                            "data": data_out,
                        },
                        ttl_seconds=TTL_TERMINAL_SEC,
                    )
                    schedule_result_file_cleanup(job_id, float(TTL_TERMINAL_SEC))
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
    large_export_cols_json: str = Form(""),
    small_export_cols_json: str = Form(""),
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
        _parse_col_list(large_export_cols_json),
        _parse_col_list(small_export_cols_json),
    )
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/check/status/{job_id}")
async def check_status(job_id: str):
    row = await job_get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(row)


@router.get("/check/result/{job_id}")
async def check_download_result(job_id: str):
    """Download فرز Excel from disk (job JSON only holds metadata + preview)."""
    if not job_id_valid(job_id):
        raise HTTPException(status_code=400, detail="Invalid job id")
    row = await job_get(job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    if row.get("status") != "done":
        raise HTTPException(status_code=409, detail="Result not ready")
    data = row.get("data") or {}
    if data.get("kind") != "xlsx" or data.get("storage") != "file":
        raise HTTPException(status_code=404, detail="No downloadable file for this job")
    path = result_path_for_job(job_id)
    if path is None or not path.is_file():
        raise HTTPException(status_code=404, detail="Result file expired or missing")
    fname = data.get("filename") or "التطابقات.xlsx"
    return FileResponse(
        path,
        filename=fname,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
