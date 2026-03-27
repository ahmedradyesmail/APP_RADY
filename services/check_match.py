"""Synchronous plate check logic (runs in thread pool / background)."""

from __future__ import annotations

import io
import logging
from datetime import datetime

import openpyxl

from services.plate_utils import normalize_plate, auto_detect_plate_col
from services.excel_utils import (
    load_workbook_maybe_encrypted,
    find_best_sheet,
    apply_excel_style,
    workbook_to_bytes,
)

logger = logging.getLogger(__name__)


def run_check_plates_sync(
    lc_bytes: bytes,
    sc_bytes: bytes,
    password: str,
    large_col: str,
    small_col: str,
    large_sheet: str,
    small_sheet: str,
) -> dict:
    """
    Returns one of:
      {"kind": "xlsx", "content": bytes, "filename": str}
      {"kind": "json", "status_code": int, "body": dict}
    """
    try:
        large_wb = load_workbook_maybe_encrypted(lc_bytes, password)
    except ValueError:
        logger.exception("Failed to open encrypted large workbook")
        raise

    try:
        small_wb = openpyxl.load_workbook(
            io.BytesIO(sc_bytes), read_only=True, data_only=True
        )
    except Exception as e:
        raise ValueError(f"تعذّر فتح الملف الصغير: {e}") from e

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
        raise ValueError("الملف الكبير فارغ")

    lh = [str(h).strip() if h is not None else "" for h in ld[0]]
    lc = large_col.strip() or auto_detect_plate_col(lh)

    if not lc or lc not in lh:
        return {
            "kind": "json",
            "status_code": 422,
            "body": {
                "detail": (
                    f"لم يُعثر على عمود اللوحة في الملف الكبير "
                    f"(شيت: {large_ws.title}). الأعمدة: {lh}"
                ),
                "headers": lh,
                "code": "COL_NOT_FOUND_LARGE",
            },
        }

    lci = lh.index(lc)
    lookup: dict = {}
    for row in ld[1:]:
        if all(v is None for v in row):
            continue
        rp = row[lci] if lci < len(row) else None
        norm = normalize_plate(rp)
        if not norm:
            continue
        rd = {lh[i]: (row[i] if i < len(row) else None) for i in range(len(lh))}
        lookup.setdefault(norm, []).append(rd)

    sd = list(small_ws.iter_rows(values_only=True))
    if not sd:
        raise ValueError("الملف الصغير فارغ")

    sh = [str(h).strip() if h is not None else "" for h in sd[0]]
    sc = small_col.strip() or auto_detect_plate_col(sh)

    if not sc or sc not in sh:
        return {
            "kind": "json",
            "status_code": 422,
            "body": {
                "detail": f"لم يُعثر على عمود اللوحة في الملف الصغير. الأعمدة: {sh}",
                "headers": sh,
                "code": "COL_NOT_FOUND_SMALL",
            },
        }

    sci = sh.index(sc)
    matched: list = []
    mp: list = []
    up: list = []

    for row in sd[1:]:
        if all(v is None for v in row):
            continue
        rp = row[sci] if sci < len(row) else None
        norm = normalize_plate(rp)
        if not norm:
            continue
        if norm in lookup:
            matched.extend(lookup[norm])
            mp.append(str(rp or "").strip())
        else:
            up.append(str(rp or "").strip())

    if not matched:
        return {
            "kind": "json",
            "status_code": 200,
            "body": {
                "detail": "لا توجد تطابقات بين الملفين",
                "matched": 0,
                "unmatched": len(up),
                "large_col_used": lc,
                "small_col_used": sc,
            },
        }

    wb_out = openpyxl.Workbook()
    ws_m = wb_out.active
    ws_m.title = "التطابقات"
    apply_excel_style(ws_m, lh, matched)

    ws_s = wb_out.create_sheet("ملخص")
    apply_excel_style(
        ws_s,
        ["البند", "القيمة"],
        [
            {"البند": "إجمالي صفوف مُطابَقة", "القيمة": len(matched)},
            {"البند": "لوحات مطابَقة", "القيمة": len(mp)},
            {"البند": "لوحات غير مطابَقة", "القيمة": len(up)},
            {"البند": "عمود الملف الكبير", "القيمة": lc},
            {"البند": "عمود الملف الصغير", "القيمة": sc},
        ],
    )

    if up:
        ws_u = wb_out.create_sheet("غير مطابَقة")
        apply_excel_style(
            ws_u,
            ["رقم اللوحة (غير مطابق)"],
            [{"رقم اللوحة (غير مطابق)": p} for p in up],
        )

    content = workbook_to_bytes(wb_out)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"التطابقات_{ts}.xlsx"

    return {"kind": "xlsx", "content": content, "filename": filename}
