"""Excel parsing and plate index for Live WebSocket checker."""
from __future__ import annotations

import re
from typing import Any

from services.excel_utils import load_workbook_maybe_encrypted
from services.plate_utils import format_plate_display, normalize_plate


def plate_candidates_from_text(text: str) -> list[str]:
    t = (text or "").strip()
    if len(t) < 3:
        return []
    out: list[str] = []
    for m in re.finditer(
        r"[\u0600-\u06FFA-Za-z]{2,5}\s*[\d\u0660-\u0669\u06F0-\u06F9]{3,5}",
        t,
    ):
        frag = m.group(0).strip()
        if frag and frag not in out:
            out.append(frag)
        if len(out) >= 8:
            break
    return out


def parse_excel_workbook_from_path(
    file_path: str, password: str = ""
) -> tuple[dict[str, tuple], list[str]]:
    with open(file_path, "rb") as fh:
        raw = fh.read()
    wb = load_workbook_maybe_encrypted(raw, (password or "").strip())
    try:
        sheets_map: dict[str, tuple] = {}
        sheet_names = list(wb.sheetnames)
        for name in sheet_names:
            ws = wb[name]
            rows_iter = ws.iter_rows(values_only=True)
            header_row = next(rows_iter, None)
            if header_row is None:
                headers: list[str] = []
                body: list[tuple[Any, ...]] = []
            else:
                headers = [str(h).strip() if h is not None else "" for h in header_row]
                body = [tuple(r) for r in rows_iter]
            sheets_map[name] = (body, headers)
        return sheets_map, sheet_names
    finally:
        try:
            wb.close()
        except Exception:
            pass


def union_column_headers(sheets_map: dict) -> list[str]:
    seen: list[str] = []
    for _name, (_body, headers) in sheets_map.items():
        for h in headers:
            hs = str(h).strip() if h is not None else ""
            if hs and hs not in seen:
                seen.append(hs)
    return seen


def merge_workbook_plate_column(sheets_map: dict, col: str) -> dict[str, dict]:
    col = (col or "").strip()
    if not col:
        raise ValueError("عمود اللوحة فارغ")
    merged: dict[str, dict] = {}
    for sheet_name, (body, headers) in sheets_map.items():
        if col not in headers:
            continue
        idx = headers.index(col)
        for row in body:
            if row is None or all(v is None for v in row):
                continue
            cell = row[idx] if idx < len(row) else None
            key = normalize_plate(cell)
            if not key or len(key) < 2:
                continue
            rd = {
                headers[i]: (row[i] if i < len(row) else None)
                for i in range(len(headers))
            }
            rd["_sheet"] = sheet_name
            rd["_matched_column"] = col
            merged[key] = rd
    return merged


def lookup_plate(excel_plates: dict, raw_plate: str) -> tuple[bool, dict]:
    key = normalize_plate(raw_plate)
    if not key:
        return False, {}
    row = excel_plates.get(key)
    if row:
        return True, dict(row)
    return False, {}
