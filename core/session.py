from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any


@dataclass
class SessionState:
    genai_session: Any | None = None
    excel_sheets: dict = field(default_factory=dict)
    excel_loaded: bool = False
    excel_plates: dict = field(default_factory=dict)
    excel_columns: list = field(default_factory=list)
    excel_rows: list = field(default_factory=list)
    excel_plate_column: str = ""
    excel_active_sheet: str = ""
    last_live_check_key: str = ""
    # Normalized plate keys already emitted this model turn (partial + final dedupe).
    model_plate_norm_keys: set[str] = field(default_factory=set)
    text_buffer: str = ""
    input_transcript: str = ""
    # Index into cumulative inputTranscription: only suffix after this is used for live plate regex.
    transcript_turn_anchor: int = 0
    check_temp_enabled: bool = False
    check_temp_session_token: str = ""
    check_temp_dsn: str = ""
    user_id: int = 0
    is_admin: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    connected: bool = False


_sessions: dict[str, SessionState] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def create_session(session_id: str) -> SessionState:
    s = SessionState()
    s.last_activity_at = _now_utc()
    _sessions[session_id] = s
    return s


def get_session(session_id: str) -> SessionState | None:
    return _sessions.get(session_id)


def get_or_create_session(session_id: str) -> SessionState:
    s = _sessions.get(session_id)
    if s is None:
        s = create_session(session_id)
    return s


def touch_session(session_id: str) -> None:
    s = _sessions.get(session_id)
    if s is not None:
        s.last_activity_at = _now_utc()


def remove_session(session_id: str) -> None:
    _sessions.pop(session_id, None)
