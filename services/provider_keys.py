"""Provider API keys persisted to data/provider_keys.json.

Keys survive restarts and only change when updated manually (file/admin API).
"""

from __future__ import annotations

import threading
import json
from pathlib import Path

_lock = threading.Lock()
_KEY_FIELDS = ("gemini_api_key", "ors_api_key", "gmaps_api_key")
_mem: dict[str, str] = {}
_KEYS_FILE = Path(__file__).resolve().parents[1] / "data" / "provider_keys.json"


def _normalize_payload(data: dict | None) -> dict[str, str]:
    src = data or {}
    out: dict[str, str] = {}
    for k in _KEY_FIELDS:
        v = src.get(k, "")
        s = v.strip() if isinstance(v, str) else ""
        if s:
            out[k] = s
    return out


def _load_from_disk_unlocked() -> None:
    try:
        if not _KEYS_FILE.exists():
            return
        raw = json.loads(_KEYS_FILE.read_text(encoding="utf-8") or "{}")
        _mem.clear()
        _mem.update(_normalize_payload(raw))
    except Exception:
        # Keep in-memory values if file read fails.
        pass


def _save_to_disk_unlocked() -> None:
    try:
        _KEYS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: _mem.get(k, "") for k in _KEY_FIELDS}
        _KEYS_FILE.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        # Do not crash requests on filesystem errors.
        pass


def snapshot_for_admin() -> dict[str, str]:
    """Current in-memory values (admin GET only)."""
    with _lock:
        return {
            "gemini_api_key": _mem.get("gemini_api_key") or "",
            "ors_api_key": _mem.get("ors_api_key") or "",
            "gmaps_api_key": _mem.get("gmaps_api_key") or "",
        }


def set_all_keys(gemini_api_key: str, ors_api_key: str, gmaps_api_key: str) -> None:
    """Replace all three slots; non-empty after strip is stored, empty removes that key."""
    with _lock:
        pairs = zip(
            _KEY_FIELDS,
            (gemini_api_key, ors_api_key, gmaps_api_key),
        )
        for k, raw in pairs:
            s = (raw or "").strip()
            if s:
                _mem[k] = s
            else:
                _mem.pop(k, None)
        _save_to_disk_unlocked()


def get_gemini_api_key_sync() -> str | None:
    with _lock:
        v = _mem.get("gemini_api_key")
    return v.strip() if isinstance(v, str) and v.strip() else None


def get_ors_api_key_sync() -> str | None:
    with _lock:
        v = _mem.get("ors_api_key")
    return v.strip() if isinstance(v, str) and v.strip() else None


def get_gmaps_api_key_sync() -> str | None:
    with _lock:
        v = _mem.get("gmaps_api_key")
    return v.strip() if isinstance(v, str) and v.strip() else None


with _lock:
    _load_from_disk_unlocked()
