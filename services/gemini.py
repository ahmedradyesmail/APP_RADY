import asyncio
import json
import os
import tempfile
from pathlib import Path
from datetime import datetime

import aiofiles
import httpx
from google import genai
from google.genai import types

from .plate_utils import normalize_plate_value


SYSTEM_INSTRUCTION = """
You are an expert Arabic data clerk specializing in Saudi/Gulf license plates.
Never translate letters to English; always use Arabic script.
If the speaker mentions a vehicle type (Taxi, Dyna, Transport), include it.
Carefully identify Street Names and specific Landmarks mentioned by the speaker.
"""

USER_PROMPT = """
Listen to the attached audio. Extract every license plate mentioned.
Output ONLY a JSON array where each object has:
- "street_name": The current street.
- "location_details": Specific landmarks (e.g. 'سلخه', 'جراج','معدي اول يمين' ,'بعد المسجد', 'أول برحة').
- "plate_letters": Arabic letters SEPARATED BY SPACES (e.g. "ح أ أ" or "ر س م").
- "plate_numbers": Numeric part only (e.g. "3108").
- "vehicle_type": Vehicle description if mentioned, otherwise null.
"""

_http_client: httpx.AsyncClient | None = None


async def init_http_client() -> None:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=120.0)


async def close_http_client() -> None:
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None


def _get_http_client() -> httpx.AsyncClient:
    if _http_client is None:
        raise RuntimeError("Gemini HTTP client not initialized")
    return _http_client


def _detect_mime(filename: str) -> str:
    n = (filename or "").lower()
    if n.endswith(".mp3"):
        return "audio/mpeg"
    if n.endswith(".opus"):
        return "audio/ogg"
    if n.endswith(".ogg"):
        return "audio/ogg"
    if n.endswith((".m4a", ".mp4")):
        return "audio/mp4"
    if n.endswith(".wav"):
        return "audio/wav"
    if n.endswith(".flac"):
        return "audio/flac"
    return "audio/webm"


def _upload_file_sync(tmp_path: str, api_key: str, gemini_mime: str):
    client = genai.Client(api_key=api_key)
    upload_config = types.UploadFileConfig(mime_type=gemini_mime)
    uploaded = client.files.upload(file=tmp_path, config=upload_config)
    return client, uploaded


def _generate_content_sync(client: genai.Client, model_name: str, uploaded) -> str:
    response = client.models.generate_content(
        model=model_name,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0.0,
            response_mime_type="application/json",
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                disable=True
            ),
        ),
        contents=[USER_PROMPT, uploaded],
    )
    return response.text


async def _wait_for_active(client: genai.Client, uploaded, api_key: str) -> None:
    """Poll until file is ACTIVE or raise."""
    http = _get_http_client()
    for attempt in range(40):
        try:
            file_info = await asyncio.to_thread(client.files.get, uploaded.name)
            state_name = (
                file_info.state.name
                if hasattr(file_info.state, "name")
                else str(file_info.state)
            )
            print(f"[Gemini] Poll {attempt + 1}: state={state_name}")
            if state_name == "ACTIVE":
                print("[Gemini] File is ACTIVE ✅")
                return
            if state_name == "FAILED":
                raise RuntimeError("فشل Gemini في معالجة الملف الصوتي (FAILED)")
        except RuntimeError:
            raise
        except Exception:
            rest_url = (
                f"https://generativelanguage.googleapis.com"
                f"/v1beta/{uploaded.name}?key={api_key}"
            )
            try:
                resp = await http.get(rest_url, timeout=10.0)
                resp.raise_for_status()
                fj = resp.json()
                sn = fj.get("state", "UNKNOWN")
                print(f"[Gemini] Poll {attempt + 1} (REST): state={sn}")
                if sn == "ACTIVE":
                    print("[Gemini] File is ACTIVE ✅ (REST)")
                    return
                if sn == "FAILED":
                    raise RuntimeError(
                        "فشل Gemini في معالجة الملف الصوتي (FAILED)"
                    )
            except RuntimeError:
                raise
            except Exception as e2:
                print(f"[Gemini] REST error: {e2}")
        await asyncio.sleep(3)
    raise RuntimeError("انتهت مهلة الانتظار (120s) — الملف لم يصبح ACTIVE")


def _parse_gemini_response(raw: str) -> list[dict]:
    raw = (raw or "[]").strip()
    if raw.startswith("```"):
        raw = raw.split("```", 1)[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.rstrip("`").strip()
    s, e = raw.find("["), raw.rfind("]")
    raw = raw[s : e + 1] if s != -1 and e != -1 else "[]"
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def _enrich_plates(
    plates: list[dict],
    recorder_name: str,
    sheet_name: str,
    gps_points: list[dict],
) -> list[dict]:
    last_street = "غير محدد"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    for i, p in enumerate(plates):
        if not p.get("vehicle_type"):
            p["vehicle_type"] = "ملاكى"

        s = p.get("street_name")
        if isinstance(s, str) and s.strip():
            last_street = s.strip()
        p["street_name"] = last_street

        letters = " ".join(str(p.get("plate_letters", "")).split())
        numbers = str(p.get("plate_numbers", "")).strip()
        normalized, _ = normalize_plate_value(
            letters_raw=letters,
            numbers_raw=numbers,
            full_raw=f"{letters}{numbers}",
        )
        p["full_plate"] = normalized or f"{letters} {numbers}".strip()

        if gps_points and i < len(gps_points):
            pt = gps_points[i]
        elif gps_points:
            pt = gps_points[-1]
        else:
            pt = None
        p["gps"] = f"{pt.get('lat','')},{pt.get('lng','')}" if pt else ""

        p["recorder_name"] = recorder_name
        p["recording_date"] = now_str
        p["sheet_name"] = sheet_name

    return plates


def _deduplicate(plates: list[dict]) -> list[dict]:
    seen, unique = set(), []
    for p in plates:
        key = (
            p.get("full_plate", ""),
            p.get("street_name", ""),
            p.get("location_details", ""),
        )
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


async def process_audio(
    file_content: bytes,
    filename: str,
    api_key: str,
    model_name: str,
    recorder_name: str,
    sheet_name: str,
    gps_points: list[dict],
) -> list[dict]:
    """Upload audio to Gemini, extract plates, enrich, deduplicate."""
    suffix = Path(filename).suffix if filename else ".mp3"
    suffix = (suffix or ".mp3").encode("ascii", "ignore").decode("ascii") or ".mp3"
    gemini_mime = _detect_mime(filename)

    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    try:
        async with aiofiles.open(tmp_path, "wb") as af:
            await af.write(file_content)

        client, uploaded = await asyncio.to_thread(
            _upload_file_sync, tmp_path, api_key, gemini_mime
        )
        print(f"[Gemini] Uploaded as: {uploaded.name}")

        await _wait_for_active(client, uploaded, api_key)

        raw_text = await asyncio.to_thread(
            _generate_content_sync, client, model_name, uploaded
        )

        plates = _parse_gemini_response(raw_text)
        plates = _enrich_plates(plates, recorder_name, sheet_name, gps_points)
        plates = _deduplicate(plates)
        return plates

    finally:
        try:
            await asyncio.to_thread(os.unlink, tmp_path)
        except Exception:
            pass
