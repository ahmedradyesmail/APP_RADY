import io
import logging
from datetime import datetime

import openpyxl
from fastapi import APIRouter, Depends, Form, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse

from dependencies.auth import get_current_user
from services.plate_utils import normalize_plate, auto_detect_plate_col
from services.excel_utils import (
    load_workbook_maybe_encrypted,
    find_best_sheet,
    apply_excel_style,
    workbook_to_bytes,
)
from services.upload_security import MAX_EXCEL_BYTES, read_upload_with_limit

from urllib.parse import quote


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
    password:   str               = Form(""),
):
    result = {}

    if large_file:
        try:
            # SECURITY FIX: file size limit to prevent DoS via large uploads
            content = await read_upload_with_limit(large_file, MAX_EXCEL_BYTES, 30)
            wb      = load_workbook_maybe_encrypted(content, password.strip())
            ws      = find_best_sheet(wb)
            headers = _get_headers(ws)
            result["large"] = {
                "headers":    headers,
                "detected":   auto_detect_plate_col(headers),
                "sheet_name": ws.title,
                "all_sheets": wb.sheetnames,
            }
        except Exception as e:
            result["large"] = {"error": str(e)}

    if small_file:
        try:
            # SECURITY FIX: file size limit to prevent DoS via large uploads
            content = await read_upload_with_limit(small_file, MAX_EXCEL_BYTES, 30)
            wb      = openpyxl.load_workbook(
                io.BytesIO(content), read_only=True, data_only=True
            )
            ws      = find_best_sheet(wb)
            headers = _get_headers(ws)
            result["small"] = {
                "headers":    headers,
                "detected":   auto_detect_plate_col(headers),
                "sheet_name": ws.title,
                "all_sheets": wb.sheetnames,
            }
        except Exception as e:
            result["small"] = {"error": str(e)}

    return JSONResponse(result)


@router.post("/check")
async def check_plates(
    large_file:  UploadFile = File(...),
    small_file:  UploadFile = File(...),
    password:    str        = Form(""),
    large_col:   str        = Form(""),
    small_col:   str        = Form(""),
    large_sheet: str        = Form(""),
    small_sheet: str        = Form(""),
):
    # SECURITY FIX: file size limit to prevent DoS via large uploads
    lc_bytes = await read_upload_with_limit(large_file, MAX_EXCEL_BYTES, 30)
    # SECURITY FIX: file size limit to prevent DoS via large uploads
    sc_bytes = await read_upload_with_limit(small_file, MAX_EXCEL_BYTES, 30)

    try:
        large_wb = load_workbook_maybe_encrypted(lc_bytes, password.strip())
    except ValueError:
        # SECURITY FIX: hiding internal exception details from client
        logger.exception("Failed to open encrypted large workbook")
        raise HTTPException(
            status_code=400,
            detail="An internal error occurred. Please try again.",
        )

    try:
        small_wb = openpyxl.load_workbook(
            io.BytesIO(sc_bytes), read_only=True, data_only=True
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"تعذّر فتح الملف الصغير: {e}")

    large_ws = (
        large_wb[large_sheet]
        if large_sheet and large_sheet in large_wb.sheetnames
        else find_best_sheet(large_wb)
    )
    small_ws = (
        small_wb[small_sheet]
        if small_sheet and small_sheet in small_wb.sheetnames
        else find_best_sheet(small_wb)
    )

    ld = list(large_ws.iter_rows(values_only=True))
    if not ld:
        raise HTTPException(status_code=400, detail="الملف الكبير فارغ")

    lh = [str(h).strip() if h is not None else "" for h in ld[0]]
    lc = large_col.strip() or auto_detect_plate_col(lh)

    if not lc or lc not in lh:
        return JSONResponse(
            status_code=422,
            content={
                "detail":  (
                    f"لم يُعثر على عمود اللوحة في الملف الكبير "
                    f"(شيت: {large_ws.title}). الأعمدة: {lh}"
                ),
                "headers": lh,
                "code":    "COL_NOT_FOUND_LARGE",
            },
        )

    lci    = lh.index(lc)
    lookup = {}
    for row in ld[1:]:
        if all(v is None for v in row):
            continue
        rp   = row[lci] if lci < len(row) else None
        norm = normalize_plate(rp)
        if not norm:
            continue
        rd = {lh[i]: (row[i] if i < len(row) else None) for i in range(len(lh))}
        lookup.setdefault(norm, []).append(rd)

    sd = list(small_ws.iter_rows(values_only=True))
    if not sd:
        raise HTTPException(status_code=400, detail="الملف الصغير فارغ")

    sh = [str(h).strip() if h is not None else "" for h in sd[0]]
    sc = small_col.strip() or auto_detect_plate_col(sh)

    if not sc or sc not in sh:
        return JSONResponse(
            status_code=422,
            content={
                "detail":  f"لم يُعثر على عمود اللوحة في الملف الصغير. الأعمدة: {sh}",
                "headers": sh,
                "code":    "COL_NOT_FOUND_SMALL",
            },
        )

    sci     = sh.index(sc)
    matched = []
    mp      = []
    up      = []

    for row in sd[1:]:
        if all(v is None for v in row):
            continue
        rp   = row[sci] if sci < len(row) else None
        norm = normalize_plate(rp)
        if not norm:
            continue
        if norm in lookup:
            matched.extend(lookup[norm])
            mp.append(str(rp or "").strip())
        else:
            up.append(str(rp or "").strip())

    if not matched:
        return JSONResponse({
            "detail":        "لا توجد تطابقات بين الملفين",
            "matched":       0,
            "unmatched":     len(up),
            "large_col_used": lc,
            "small_col_used": sc,
        })

    # Build output workbook
    wb_out = openpyxl.Workbook()
    ws_m   = wb_out.active
    ws_m.title = "التطابقات"
    apply_excel_style(ws_m, lh, matched)

    ws_s = wb_out.create_sheet("ملخص")
    apply_excel_style(ws_s, ["البند", "القيمة"], [
        {"البند": "إجمالي صفوف مُطابَقة", "القيمة": len(matched)},
        {"البند": "لوحات مطابَقة",         "القيمة": len(mp)},
        {"البند": "لوحات غير مطابَقة",     "القيمة": len(up)},
        {"البند": "عمود الملف الكبير",      "القيمة": lc},
        {"البند": "عمود الملف الصغير",      "القيمة": sc},
    ])

    if up:
        ws_u = wb_out.create_sheet("غير مطابَقة")
        apply_excel_style(
            ws_u,
            ["رقم اللوحة (غير مطابق)"],
            [{"رقم اللوحة (غير مطابق)": p} for p in up],
        )

    content  = workbook_to_bytes(wb_out)
    ts       = datetime.now().strftime("%Y%m%d_%H%M")
    





    filename = f"التطابقات_{ts}.xlsx"
    encoded_filename = quote(filename, safe='')

    return StreamingResponse(
        io.BytesIO(content),
        media_type=(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
         headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"},
 )