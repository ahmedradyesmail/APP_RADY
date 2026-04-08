"""WebSocket message handling for Live plate checker (FastAPI WebSocket)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import traceback
from typing import Any, Optional

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from config import settings
from core.gemini_client import create_gemini_session
from core.session import SessionState, create_session, remove_session
from core.excel_loader import (
    format_plate_display,
    lookup_plate,
    merge_workbook_plate_column,
    normalize_plate,
    parse_excel_workbook,
    plate_candidates_from_text,
    union_column_headers,
)

logger = logging.getLogger(__name__)

_live_sem = asyncio.Semaphore(max(1, int(settings.gemini_live_max_concurrent)))


async def _send(websocket: WebSocket, payload: dict) -> None:
    try:
        await websocket.send_text(json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        logger.error("Failed to send WS: %s", e)


async def _send_error(
    websocket: WebSocket, message: str, error_type: str = "general"
) -> None:
    await _send(
        websocket,
        {"type": "error", "data": {"message": message, "error_type": error_type}},
    )


async def _maybe_live_sheet_check(websocket: WebSocket, session: SessionState) -> None:
    """While user speaks: infer plate from STT and lookup in Excel (real-time)."""
    if not session.excel_loaded or not (session.excel_plate_column or "").strip():
        return
    t = (session.input_transcript or "").strip()
    if len(t) < 3:
        return
    cands = plate_candidates_from_text(t)
    if not cands:
        return
    best = cands[0]
    key = normalize_plate(best)
    if len(key) < 3:
        return
    if key == session.last_live_check_key:
        return
    session.last_live_check_key = key
    found, row_data = lookup_plate(session.excel_plates, best)
    safe_row = {
        k: (str(v) if v is not None else None)
        for k, v in row_data.items()
        if not str(k).startswith("_")
    }
    plate_show = format_plate_display(best) or best
    matched = row_data.get("_matched_column", "") if found else ""
    hit_sheet = row_data.get("_sheet", "") if found else ""
    await _send(
        websocket,
        {
            "type": "live_plate",
            "data": {
                "plate": plate_show,
                "found": found,
                "details": safe_row,
                "transcript": t,
                "compare_column": matched,
                "sheet": hit_sheet,
            },
        },
    )


def _strip_markdown_json_fence(text: str) -> str:
    t = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", t, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return t


def _parse_plate_payload(blob: str) -> list[Any]:
    """
    Parse Gemini reply: single object {"plate": ...}, array of objects,
    or {"plates": [...]}. Returns list of plate values (str or None).
    """
    blob = blob.strip()
    if not blob:
        return []

    def collect_from_obj(obj: dict) -> list[Any]:
        out: list[Any] = []
        if "plate" in obj:
            out.append(obj.get("plate"))
        inner = obj.get("plates")
        if isinstance(inner, list):
            for el in inner:
                if isinstance(el, dict) and "plate" in el:
                    out.append(el.get("plate"))
                elif isinstance(el, str):
                    out.append(el)
        return out

    try:
        data = json.loads(blob)
    except json.JSONDecodeError:
        lb, rb = blob.find("["), blob.rfind("]")
        if lb >= 0 and rb > lb:
            try:
                data = json.loads(blob[lb : rb + 1])
            except json.JSONDecodeError:
                data = None
        else:
            data = None
        if data is None:
            lo, hi = blob.find("{"), blob.rfind("}") + 1
            if lo >= 0 and hi > lo:
                try:
                    data = json.loads(blob[lo:hi])
                except json.JSONDecodeError:
                    return []
            else:
                return []

    if isinstance(data, list):
        plates: list[Any] = []
        for item in data:
            if isinstance(item, dict):
                plates.append(item.get("plate"))
            elif isinstance(item, str):
                plates.append(item)
        return plates
    if isinstance(data, dict):
        return collect_from_obj(data)
    return []


async def _emit_plate_result(
    websocket: WebSocket, session: SessionState, plate: Any
) -> None:
    if plate is None or (isinstance(plate, str) and not plate.strip()):
        await _send(websocket, {"type": "no_plate", "data": {}})
        return
    raw = str(plate).strip()
    plate_str = format_plate_display(raw) or raw
    if session.excel_loaded:
        if not (session.excel_plate_column or "").strip():
            await _send(
                websocket,
                {
                    "type": "plate_result",
                    "data": {
                        "plate": plate_str,
                        "found": None,
                        "details": {},
                        "compare_column": "",
                        "sheet": "",
                        "index_count": 0,
                        "needs_plate_column": True,
                    },
                },
            )
            return
        found, row_data = lookup_plate(session.excel_plates, raw)
        safe_row = {
            k: (str(v) if v is not None else None)
            for k, v in row_data.items()
            if not str(k).startswith("_")
        }
        matched = row_data.get("_matched_column", "") if found else ""
        hit_sheet = row_data.get("_sheet", "") if found else ""
        await _send(
            websocket,
            {
                "type": "plate_result",
                "data": {
                    "plate": plate_str,
                    "found": bool(found),
                    "details": safe_row,
                    "compare_column": matched,
                    "sheet": hit_sheet,
                    "index_count": len(session.excel_plates),
                },
            },
        )
    else:
        await _send(
            websocket,
            {
                "type": "plate_result",
                "data": {
                    "plate": plate_str,
                    "found": None,
                    "details": {},
                    "compare_column": "",
                    "sheet": "",
                    "needs_plate_column": False,
                },
            },
        )


async def _emit_model_plate_if_new(
    websocket: WebSocket, session: SessionState, plate: Any
) -> bool:
    """Deduplicate partial/final model emissions for the same normalized plate."""
    if plate is None:
        return False
    raw = str(plate).strip()
    if not raw:
        return False
    key = normalize_plate(raw)
    if key and key == session.last_model_plate_key:
        return False
    if key:
        session.last_model_plate_key = key
    await _emit_plate_result(websocket, session, raw)
    return True


async def _process_plate_text(
    websocket: WebSocket, session: SessionState, text: str
) -> None:
    text = text.strip()
    logger.info("Gemini text: %s", text[:500])
    blob = _strip_markdown_json_fence(text)
    plates = _parse_plate_payload(blob)
    if not plates:
        logger.warning("No plates parsed from Gemini: %s", text[:500])
        await _send(websocket, {"type": "raw_text", "data": text})
        return

    valid = [p for p in plates if p is not None and str(p).strip()]
    if not valid:
        await _send(websocket, {"type": "no_plate", "data": {}})
        return

    for plate in valid:
        await _emit_model_plate_if_new(websocket, session, plate)


async def handle_client_messages(websocket: WebSocket, session: SessionState) -> None:
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
                msg_type = data.get("type")

                if msg_type == "excel_upload":
                    logger.info("Excel upload received")
                    try:
                        pw = (data.get("password") or "").strip()
                        sheets_map, sheet_names = parse_excel_workbook(
                            data["data"], pw
                        )
                        if not sheet_names:
                            await _send_error(
                                websocket,
                                "الملف لا يحتوي صفحات.",
                                "excel_error",
                            )
                            continue
                        session.excel_sheets = sheets_map
                        session.excel_loaded = True
                        union_headers = union_column_headers(sheets_map)
                        logger.info(
                            "Excel parsed: %s sheets, %s union column titles",
                            len(sheet_names),
                            len(union_headers),
                        )
                        session.excel_plates = {}
                        session.excel_columns = union_headers
                        session.excel_rows = []
                        session.excel_plate_column = ""
                        session.excel_active_sheet = ""
                        session.last_live_check_key = ""
                        columns_by_sheet = {n: sheets_map[n][1] for n in sheet_names}
                        await _send(
                            websocket,
                            {
                                "type": "excel_loaded",
                                "data": {
                                    "columns": union_headers,
                                    "columns_by_sheet": columns_by_sheet,
                                    "sheets_scanned": sheet_names,
                                    "count": 0,
                                    "needs_plate_column": True,
                                },
                            },
                        )
                    except Exception as e:
                        await _send_error(
                            websocket, f"خطأ في تحميل الملف: {e}", "excel_error"
                        )

                elif msg_type == "set_plate_column":
                    col = (data.get("column") or "").strip()
                    if not session.excel_loaded or not session.excel_sheets:
                        await _send_error(
                            websocket, "ارفع ملف Excel أولاً.", "excel_error"
                        )
                        continue
                    sheets_map = session.excel_sheets
                    if not col or not any(
                        col in sheets_map[n][1] for n in sheets_map
                    ):
                        await _send_error(
                            websocket,
                            "اختر عنوان عمود موجود في الملف.",
                            "excel_error",
                        )
                        continue
                    try:
                        merged = merge_workbook_plate_column(sheets_map, col)
                        session.excel_plates = merged
                        session.excel_plate_column = col
                        session.last_live_check_key = ""
                        await _send(
                            websocket,
                            {
                                "type": "plate_column_ready",
                                "data": {
                                    "plate_column": col,
                                    "count": len(merged),
                                },
                            },
                        )
                    except ValueError as e:
                        await _send_error(websocket, str(e), "excel_error")

                elif msg_type == "manual_lookup":
                    plate_raw = data.get("plate", "")
                    await _emit_plate_result(websocket, session, plate_raw)

                elif msg_type == "audio":
                    if session.genai_session:
                        await session.genai_session.send_audio(data.get("data", ""))

                elif msg_type == "end_of_turn":
                    if session.genai_session:
                        await session.genai_session.send_end_of_turn()

                elif msg_type == "text":
                    if session.genai_session:
                        await session.genai_session.send_text(data.get("data", ""))

            except Exception as e:
                logger.error(
                    "Client message error: %s\n%s", e, traceback.format_exc()
                )
    except WebSocketDisconnect:
        raise


async def handle_gemini_responses(websocket: WebSocket, session: SessionState) -> None:
    if not session.genai_session:
        return
    try:
        async for msg in session.genai_session:
            try:
                if msg.get("serverContent", {}).get("interrupted"):
                    await _send(websocket, {"type": "interrupted", "data": {}})
                    session.text_buffer = ""
                    session.input_transcript = ""
                    session.last_live_check_key = ""
                    session.last_model_plate_key = ""
                    continue

                sc = msg.get("serverContent", {})
                it = sc.get("inputTranscription") or {}
                if isinstance(it, dict) and it.get("text"):
                    session.input_transcript = it["text"]
                    await _send(
                        websocket,
                        {
                            "type": "live_transcript",
                            "data": session.input_transcript,
                        },
                    )
                    await _maybe_live_sheet_check(websocket, session)

                ot = sc.get("outputTranscription") or {}
                if isinstance(ot, dict) and ot.get("text"):
                    session.text_buffer += ot["text"]
                model_turn = sc.get("modelTurn", {})
                for part in model_turn.get("parts", []):
                    if "text" in part:
                        session.text_buffer += part["text"]

                # Try early parse on partial chunks (low latency), even before turnComplete.
                if session.text_buffer.strip():
                    blob_now = _strip_markdown_json_fence(session.text_buffer)
                    partial_plates = _parse_plate_payload(blob_now)
                    for plate in partial_plates:
                        await _emit_model_plate_if_new(websocket, session, plate)

                if sc.get("turnComplete"):
                    if session.text_buffer.strip():
                        await _process_plate_text(
                            websocket, session, session.text_buffer
                        )
                    session.text_buffer = ""
                    session.input_transcript = ""
                    session.last_live_check_key = ""
                    session.last_model_plate_key = ""
                    await _send(websocket, {"type": "turn_complete"})

            except Exception as e:
                logger.error(
                    "Gemini response error: %s\n%s", e, traceback.format_exc()
                )
    except Exception as e:
        if "connection closed" not in str(e).lower():
            logger.error("Gemini receive error: %s", e)
        raise


async def cleanup_session(session: Optional[SessionState], session_id: str) -> None:
    try:
        if session and session.genai_session:
            await session.genai_session.close()
        remove_session(session_id)
        logger.info("Session %s cleaned up", session_id)
    except Exception as e:
        logger.error("Cleanup error: %s", e)


async def handle_plate_checker_client(websocket: WebSocket) -> None:
    session_id = str(id(websocket))
    session = create_session(session_id)
    logger.info("New Live check connection: %s", session_id)

    try:
        raw_init = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
        init = json.loads(raw_init)
        if init.get("type") != "init":
            await _send_error(
                websocket,
                "الرسالة الأولى يجب أن تكون init مع api_key",
                "general",
            )
            await cleanup_session(session, session_id)
            return
        api_key = (init.get("api_key") or "").strip() or os.getenv(
            "GEMINI_API_KEY", ""
        )
        if not api_key:
            await _send_error(
                websocket,
                "مفتاح Gemini غير موجود — أضفه في الإعدادات أو GEMINI_API_KEY",
                "general",
            )
            await cleanup_session(session, session_id)
            return
    except (TimeoutError, json.JSONDecodeError, WebSocketDisconnect) as e:
        logger.info("Live check init failed: %s", e)
        try:
            await websocket.close(code=4408)
        except Exception:
            pass
        await cleanup_session(session, session_id)
        return

    try:
        async with _live_sem:
            async with create_gemini_session(api_key) as gemini_session:
                session.genai_session = gemini_session
                await _send(websocket, {"type": "ready"})

                client_task = asyncio.create_task(
                    handle_client_messages(websocket, session)
                )
                gemini_task = asyncio.create_task(
                    handle_gemini_responses(websocket, session)
                )

                done, pending = await asyncio.wait(
                    [client_task, gemini_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
                for t in done:
                    exc = t.exception()
                    if exc is not None and not isinstance(
                        exc, (WebSocketDisconnect, asyncio.CancelledError)
                    ):
                        logger.error("Live check task ended with error: %s", exc)

    except Exception as e:
        err = str(e)
        if "Quota" in err:
            await _send_error(
                websocket, "تم تجاوز الحصة، انتظر قليلاً.", "quota_exceeded"
            )
        elif "connection closed" not in err.lower():
            logger.error("Session error: %s\n%s", e, traceback.format_exc())
            await _send_error(websocket, "حدث خطأ، حاول مرة أخرى.", "general")
    finally:
        await cleanup_session(session, session_id)
