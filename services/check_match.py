"""Synchronous plate check logic (runs in thread pool / background)."""

from __future__ import annotations

import io
import logging
import re
from datetime import datetime

import openpyxl

from services.plate_utils import normalize_plate, auto_detect_plate_col
from services.excel_utils import (
    load_workbook_maybe_encrypted,
    find_best_sheet,
    apply_excel_style_matched_merge,
    workbook_to_bytes,
)

logger = logging.getLogger(__name__)

PREVIEW_MAX_ROWS = 300

_SMALL_HDR_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"^\s*صغير\s*[—–\-]\s*", re.IGNORECASE), ""),
    (re.compile(r"^\s*صغير\s+", re.IGNORECASE), ""),
    (re.compile(r"\s+صغير\s*$", re.IGNORECASE), ""),
    (re.compile(r"\s*[—–\-]\s*صغير\s*$", re.IGNORECASE), ""),
)


def _strip_small_word_from_header_title(title: str) -> str:
    """Remove «صغير» and common prefixes from header text in the exported sheet."""
    raw = (title or "").strip()
    if not raw:
        return raw
    s = raw
    for pat, rep in _SMALL_HDR_PATTERNS:
        s = pat.sub(rep, s).strip()
    s = re.sub(r"\s*صغير\s*", " ", s)
    s = " ".join(s.split()).strip()
    return s if s else raw


def _close_wb(wb) -> None:
    if wb is None:
        return
    try:
        wb.close()
    except Exception:
        pass


def _norm_large_export_cols(requested: list[str], available: list[str]) -> list[str]:
    """Empty selection → export all large columns."""
    if not requested:
        return [h for h in available if h]
    out = [c for c in requested if c in available]
    return out if out else [h for h in available if h]


def _norm_small_export_cols(requested: list[str], available: list[str]) -> list[str]:
    """Empty selection → no small columns (user must tick columns explicitly)."""
    if not requested:
        return []
    return [c for c in requested if c in available]


def run_check_plates_sync(
    lc_bytes: bytes,
    sc_bytes: bytes,
    password: str,
    large_col: str,
    small_col: str,
    large_sheet: str,
    small_sheet: str,
    large_export_cols: list[str] | None = None,
    small_export_cols: list[str] | None = None,
) -> dict:
    """
    Returns one of:
      {"kind": "xlsx", "content": bytes, "filename": str}
      {"kind": "json", "status_code": int, "body": dict}

    Reads each sheet in a single streamed pass (no full ``list(sheet)`` copy)
    to cut RAM on large workbooks; closes read_only workbooks when done.
    """
    large_wb = None
    small_wb = None
    try:
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

        lrows = large_ws.iter_rows(values_only=True)
        header_l = next(lrows, None)
        if header_l is None:
            raise ValueError("الملف الكبير فارغ")

        lh = [str(h).strip() if h is not None else "" for h in header_l]
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
        for row in lrows:
            if all(v is None for v in row):
                continue
            rp = row[lci] if lci < len(row) else None
            norm = normalize_plate(rp)
            if not norm:
                continue
            rd = {lh[i]: (row[i] if i < len(row) else None) for i in range(len(lh))}
            lookup.setdefault(norm, []).append(rd)

        srows = small_ws.iter_rows(values_only=True)
        header_s = next(srows, None)
        if header_s is None:
            raise ValueError("الملف الصغير فارغ")

        sh = [str(h).strip() if h is not None else "" for h in header_s]
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
        matched_pairs: list[tuple[dict, dict]] = []
        mp: list = []
        up: list = []

        for row in srows:
            if all(v is None for v in row):
                continue
            rp = row[sci] if sci < len(row) else None
            norm = normalize_plate(rp)
            if not norm:
                continue
            small_rd = {sh[i]: (row[i] if i < len(row) else None) for i in range(len(sh))}
            if norm in lookup:
                for large_rd in lookup[norm]:
                    matched_pairs.append((large_rd, small_rd))
                mp.append(str(rp or "").strip())
            else:
                up.append(str(rp or "").strip())

        if not matched_pairs:
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

        le_cols = _norm_large_export_cols(list(large_export_cols or []), lh)
        se_cols = _norm_small_export_cols(list(small_export_cols or []), sh)

        display_headers: list[str] = [
            _strip_small_word_from_header_title(c) for c in le_cols
        ] + [_strip_small_word_from_header_title(c) for c in se_cols]
        col_sources: list[str] = ["large"] * len(le_cols) + ["small"] * len(se_cols)

        matched_rows: list[list] = []
        for large_rd, small_rd in matched_pairs:
            row_out: list = []
            for c in le_cols:
                v = large_rd.get(c, "")
                row_out.append("" if v is None else v)
            for c in se_cols:
                v = small_rd.get(c, "")
                row_out.append("" if v is None else v)
            matched_rows.append(row_out)

        wb_out = openpyxl.Workbook()
        ws_m = wb_out.active
        ws_m.title = "التطابقات"
        apply_excel_style_matched_merge(ws_m, display_headers, matched_rows, col_sources)

        content = workbook_to_bytes(wb_out)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"التطابقات_{ts}.xlsx"

        preview_rows: list[list[str]] = []
        for row_vals in matched_rows[:PREVIEW_MAX_ROWS]:
            preview_rows.append(
                ["" if v is None else str(v).strip() for v in row_vals]
            )

        return {
            "kind": "xlsx",
            "content": content,
            "filename": filename,
            "preview": {
                "headers": display_headers,
                "col_sources": col_sources,
                "rows": preview_rows,
                "total_rows": len(matched_rows),
                "truncated": len(matched_rows) > PREVIEW_MAX_ROWS,
                "stats": {
                    "matched_rows": len(matched_pairs),
                    "matched_plate_hits": len(mp),
                    "unmatched_plates": len(up),
                },
            },
        }
    finally:
        _close_wb(small_wb)
        _close_wb(large_wb)
