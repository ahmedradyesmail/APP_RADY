"""WebSocket message handling for Live plate checker (FastAPI WebSocket)."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import traceback
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from config import settings
from core.gemini_client import create_gemini_session
from core.session import (
    SessionState,
    get_session,
    get_or_create_session,
    remove_session,
    touch_session,
)
from core.excel_loader import (
    format_plate_display,
    lookup_plate,
    merge_workbook_plate_column,
    normalize_plate,
    parse_excel_workbook,
    plate_candidates_from_text,
    union_column_headers,
)
from services.check_temp_storage import temp_plate_exists_sync
from services.gemini_catalog import is_gemini_model_allowed_sync
from services.provider_keys import get_gemini_api_key_sync

logger = logging.getLogger(__name__)

_live_sem = asyncio.Semaphore(max(1, int(settings.gemini_live_max_concurrent)))
_live_idle_ttl = max(60, int(settings.check_live_idle_ttl_seconds))
_live_hard_ttl = max(_live_idle_ttl, int(settings.check_live_hard_ttl_seconds))
_live_cleanup_tasks: dict[str, asyncio.Task] = {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cancel_cleanup_task(session_key: str) -> None:
    task = _live_cleanup_tasks.pop(session_key, None)
    if task and not task.done():
        task.cancel()
        logger.info("Session %s cleanup cancelled - User returned.", session_key)


async def _schedule_idle_cleanup(session_key: str) -> None:
    try:
        await asyncio.sleep(_live_idle_ttl)
        session = get_session(session_key)
        if session is None:
            return
        if session.connected:
            return
        idle_sec = (_utc_now() - session.last_activity_at).total_seconds()
        age_sec = (_utc_now() - session.created_at).total_seconds()
        if idle_sec >= _live_idle_ttl or age_sec >= _live_hard_ttl:
            if session.genai_session:
                try:
                    await session.genai_session.close()
                except Exception:
                    logger.debug("Failed to close genai session for %s", session_key)
                session.genai_session = None
            remove_session(session_key)
            logger.info("Session %s cleaned up after 30m idl", session_key)
    except asyncio.CancelledError:
        pass
    finally:
        _live_cleanup_tasks.pop(session_key, None)


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


def _segment_for_current_turn_transcript(session: SessionState) -> str:
    """Only the STT segment since the last model turnComplete (ignore prior turns)."""
    full = (session.input_transcript or "").strip()
    anchor = max(0, int(session.transcript_turn_anchor))
    if anchor >= len(full):
        return full
    return full[anchor:].strip()


async def _maybe_live_sheet_check(websocket: WebSocket, session: SessionState) -> None:
    """While user speaks: infer plate from STT and lookup in Excel (real-time)."""
    if (
        not session.check_temp_enabled
        and (not session.excel_loaded or not (session.excel_plate_column or "").strip())
    ):
        return
    t = _segment_for_current_turn_transcript(session)
    if len(t) < 3:
        return
    cands = plate_candidates_from_text(t)
    if not cands:
        return
    best = cands[-1]
    key = normalize_plate(best)
    if len(key) < 3:
        return
    if key == session.last_live_check_key:
        return
    session.last_live_check_key = key
    if session.check_temp_enabled:
        found = await asyncio.to_thread(
            temp_plate_exists_sync,
            session.check_temp_dsn,
            session.user_id,
            session.is_admin,
            session_token=session.check_temp_session_token,
            plate_text=best,
        )
        safe_row = {}
    else:
        found, row_data = lookup_plate(session.excel_plates, best)
        safe_row = {
            k: (str(v) if v is not None else None)
            for k, v in row_data.items()
            if not str(k).startswith("_")
        }
    plate_show = format_plate_display(best) or best
    matched = (session.excel_plate_column or "") if (found and session.check_temp_enabled) else ""
    hit_sheet = "postgres_temp" if (found and session.check_temp_enabled) else ""
    if not session.check_temp_enabled:
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


def _sanitize_live_plate_text(plate: Any) -> str | None:
    """
    Enforce Live plate constraints:
    - max 3 Arabic letters
    - max 4 digits
    - normalize common over-expanded letter names (e.g. عين/عن -> ع)
    """
    if plate is None:
        return None
    raw = str(plate).strip()
    if not raw:
        return None

    t = raw
    # Normalize common letter-name expansions to single letters.
    t = re.sub(r"(?:\b|^)(عين|عاين|عن)(?:\b|$)", "ع", t)
    t = re.sub(r"(?:\b|^)(غين|غاين)(?:\b|$)", "غ", t)
    t = re.sub(r"(?:\b|^)(حاء|حا|حه)(?:\b|$)", "ح", t)
    t = re.sub(r"(?:\b|^)(هاء|ها|هه)(?:\b|$)", "ه", t)

    letters = "".join(re.findall(r"[\u0621-\u064A]", t))
    digits = "".join(re.findall(r"\d", t))

    if not letters or not digits:
        return None
    if len(letters) > 3 or len(digits) > 4:
        return None

    return f"{letters} {digits}"


async def _emit_plate_result(
    websocket: WebSocket, session: SessionState, plate: Any
) -> None:
    normalized_plate = _sanitize_live_plate_text(plate)
    if not normalized_plate:
        await _send(websocket, {"type": "no_plate", "data": {}})
        return
    raw = normalized_plate
    plate_str = format_plate_display(raw) or raw
    if session.check_temp_enabled:
        found = await asyncio.to_thread(
            temp_plate_exists_sync,
            session.check_temp_dsn,
            session.user_id,
            session.is_admin,
            session_token=session.check_temp_session_token,
            plate_text=raw,
        )
        await _send(
            websocket,
            {
                "type": "plate_result",
                "data": {
                    "plate": plate_str,
                    "found": bool(found),
                    "details": {},
                    "compare_column": session.excel_plate_column or "",
                    "sheet": "postgres_temp",
                    "index_count": 0,
                },
            },
        )
    elif session.excel_loaded:
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
    """Deduplicate partial/final model emissions for the same normalized plate(s) per turn."""
    normalized_plate = _sanitize_live_plate_text(plate)
    if not normalized_plate:
        return False
    key = normalize_plate(normalized_plate)
    if key and key in session.model_plate_norm_keys:
        return False
    if key:
        session.model_plate_norm_keys.add(key)
    await _emit_plate_result(websocket, session, normalized_plate)
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


async def handle_client_messages(
    websocket: WebSocket, session: SessionState, session_key: str
) -> None:
    try:
        while True:
            raw = await websocket.receive_text()
            touch_session(session_key)
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
                        session.transcript_turn_anchor = 0
                        session.model_plate_norm_keys.clear()
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
                        session.transcript_turn_anchor = 0
                        session.model_plate_norm_keys.clear()
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

                elif msg_type == "audio":
                    if session.genai_session:
                        await session.genai_session.send_audio(data.get("data", ""))

                elif msg_type == "end_of_turn":
                    if session.genai_session:
                        await session.genai_session.send_end_of_turn()

                elif msg_type == "text":
                    if session.genai_session:
                        await session.genai_session.send_text(data.get("data", ""))
                elif msg_type == "ping":
                    await _send(websocket, {"type": "pong", "data": {}})

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
                    session.transcript_turn_anchor = 0
                    session.last_live_check_key = ""
                    session.model_plate_norm_keys.clear()
                    continue

                sc = msg.get("serverContent", {})
                it = sc.get("inputTranscription") or {}
                if isinstance(it, dict) and it.get("text"):
                    full_text = it["text"]
                    session.input_transcript = full_text
                    if session.transcript_turn_anchor > len(full_text):
                        session.transcript_turn_anchor = 0
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
                    # Slice future STT at end of this turn so cumulative transcripts
                    # cannot re-match a plate from a previous user utterance.
                    session.transcript_turn_anchor = len(session.input_transcript or "")
                    # Start next model turn from a clean slate.
                    session.text_buffer = ""
                    session.input_transcript = ""
                    session.last_live_check_key = ""
                    session.model_plate_norm_keys.clear()
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


async def handle_plate_checker_client(websocket: WebSocket, user_id: int, is_admin: bool = False) -> None:
    session_key = ""
    session: SessionState | None = None
    logger.info("New Live check connection user_id=%s", user_id)

    try:
        raw_init = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
        init = json.loads(raw_init)
        if init.get("type") != "init":
            await _send_error(
                websocket,
                "الرسالة الأولى يجب أن تكون init (مع client_id و live_model).",
                "general",
            )
            if session:
                await cleanup_session(session, session_key or str(user_id))
            return
        client_id = (init.get("client_id") or "").strip()
        if not client_id:
            await _send_error(
                websocket,
                "Missing client_id",
                "general",
            )
            if session:
                await cleanup_session(session, session_key or str(user_id))
            return
        session_key = f"{user_id}:{client_id}"
        _cancel_cleanup_task(session_key)
        session = get_or_create_session(session_key)
        session.connected = True
        session.user_id = int(user_id)
        session.is_admin = bool(is_admin)
        temp_session_token = (init.get("temp_session_token") or "").strip()
        dsn_pg = (settings.check_postgres_dsn or "").strip()
        session.check_temp_enabled = bool(temp_session_token and dsn_pg)
        session.check_temp_session_token = temp_session_token if session.check_temp_enabled else ""
        session.check_temp_dsn = dsn_pg if session.check_temp_enabled else ""
        touch_session(session_key)
        live_model = (init.get("live_model") or "").strip()
        if not live_model:
            await _send_error(
                websocket,
                "اختر موديل Live من القائمة أعلى الصفحة (مُدار من الخادم).",
                "general",
            )
            session.connected = False
            _live_cleanup_tasks[session_key] = asyncio.create_task(
                _schedule_idle_cleanup(session_key)
            )
            return
        if not await asyncio.to_thread(
            is_gemini_model_allowed_sync, "live", live_model
        ):
            await _send_error(
                websocket,
                "موديل Live غير مسموح أو غير مفعّل.",
                "general",
            )
            session.connected = False
            _live_cleanup_tasks[session_key] = asyncio.create_task(
                _schedule_idle_cleanup(session_key)
            )
            return
    except (TimeoutError, json.JSONDecodeError, WebSocketDisconnect) as e:
        logger.info("Live check init failed: %s", e)
        try:
            await websocket.close(code=4408)
        except Exception:
            pass
        if session_key and session:
            session.connected = False
            _live_cleanup_tasks[session_key] = asyncio.create_task(
                _schedule_idle_cleanup(session_key)
            )
        return

    try:
        async with _live_sem:
            if session is None:
                raise RuntimeError("Session not initialized")
            connected_live = False
            api_key = get_gemini_api_key_sync()
            if api_key:
                try:
                    async with create_gemini_session(
                        api_key, live_model=live_model
                    ) as gemini_session:
                        session.genai_session = gemini_session
                        await _send(websocket, {"type": "ready"})
                        connected_live = True

                        client_task = asyncio.create_task(
                            handle_client_messages(websocket, session, session_key)
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
                    logger.warning("Live Gemini connect/session failed: %s", e)
            if not connected_live:
                await _send_error(
                    websocket,
                    "خدمة التشيك المباشر غير متاحة مؤقتاً (503).",
                    "general",
                )

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
        if session_key and session:
            session.connected = False
            session.genai_session = None
            touch_session(session_key)
            _cancel_cleanup_task(session_key)
            _live_cleanup_tasks[session_key] = asyncio.create_task(
                _schedule_idle_cleanup(session_key)
            )
