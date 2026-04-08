from __future__ import annotations

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
    last_model_plate_key: str = ""
    text_buffer: str = ""
    input_transcript: str = ""


_sessions: dict[str, SessionState] = {}


def create_session(session_id: str) -> SessionState:
    s = SessionState()
    _sessions[session_id] = s
    return s


def remove_session(session_id: str) -> None:
    _sessions.pop(session_id, None)
