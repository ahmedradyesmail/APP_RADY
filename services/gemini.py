import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from datetime import datetime

import aiofiles
import httpx
from google import genai
from google.genai import types

from .plate_utils import normalize_plate_value


SYSTEM_INSTRUCTION = """
    You are a robust Vehicle License Plate Extractor.
    Your primary goal is to identify and extract license plates from speech, even in noisy environments or street backgrounds.
    STRICT RULE: Ignore any background noise, side conversations, or non-plate words.
    Output MUST be raw JSON only. NEVER return explanations or greetings.

    Association (Arabic cues): Treat vehicle_type and location_details as belonging only to the plate spoken immediately before them in the audio—never copy one car's details onto another unless the speaker clearly links them (e.g. shared place for «السيارات دي» / «كل السيارات»).
    Single car: If the speaker gives one plate, then location, then a return-to-street cue like «ونرجع للشارع», keep location_details for «السيارة دي» only in that segment.
    Batch until end: While a batch is active, you may repeat the same location_details for each following plate; when you hear an end cue such as «انتهي», «انتهي السيارات اللي في …», or «نرجع للشارع», stop filling location_details for any plate spoken after that phrase (use null or empty).
    Vehicle type: Put vehicle_type on the same JSON object as that plate's plate_letters / plate_numbers, matching speech where the type usually comes right after the plate for «السيارة دي».
"""

USER_PROMPT = """
Listen to the attached audio. Extract every license plate mentioned.
Apply the same four rules from the system instruction:
1) vehicle_type and location_details attach only to the plate spoken immediately before them; do not copy across plates unless «السيارات دي» / «كل السيارات» (or similar) clearly shares one place.
2) One plate + location + «ونرجع للشارع» ⇒ location_details for «السيارة دي» only in that segment.
3) Batch: repeat the same location_details for following plates until «انتهي», «انتهي السيارات اللي في …», or «نرجع للشارع»; after that, leave location_details empty/null for later plates.
4) vehicle_type belongs on the same object as that plate; type usually follows the plate for «السيارة دي».

Output ONLY a JSON array where each object has:
- "street_name": The current street.
- "location_details": Specific landmarks (e.g. 'سلخه', 'جراج','معدي اول يمين' ,'بعد المسجد', 'أول برحة').
- "plate_letters": Arabic letters SEPARATED BY SPACES (e.g. "ح أ أ" or "ر س م").
- "plate_numbers": Numeric part only (e.g. "3108").
- "vehicle_type": Vehicle description if mentioned, otherwise null.
"""

_http_client: httpx.AsyncClient | None = None


async def init_http_client() -> None:
    """Shared httpx client for Gemini Files REST polling; called from FastAPI lifespan (see main.py)."""
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
    if n.endswith((".webm", ".weba")):
        return "audio/webm"
    if n.endswith(".aac"):
        return "audio/aac"
    return "audio/webm"


def _sniff_audio_mime(file_content: bytes) -> str | None:
    """
    Infer MIME from magic bytes so uploads match real container (extension often wrong for recordings).
    Returns None if unknown.
    """
    b = file_content
    if len(b) < 12:
        return None
    if b[:3] == b"ID3" or (b[0] == 0xFF and (b[1] & 0xE0) == 0xE0):
        return "audio/mpeg"
    if b[:4] == b"OggS":
        return "audio/ogg"
    if b[:4] == b"RIFF" and len(b) >= 12 and b[8:12] == b"WAVE":
        return "audio/wav"
    if b[:4] == b"fLaC":
        return "audio/flac"
    if b[:4] == b"\x1a\x45\xdf\xa3":
        return "audio/webm"
    if len(b) >= 12 and b[4:8] == b"ftyp":
        return "audio/mp4"
    return None


def _upload_file_sync(tmp_path: str, api_key: str, gemini_mime: str):
    client = genai.Client(api_key=api_key)
    upload_config = types.UploadFileConfig(mime_type=gemini_mime)
    uploaded = client.files.upload(file=tmp_path, config=upload_config)
    return client, uploaded


def _generate_content_sync(client: genai.Client, model_name: str, uploaded) -> str:
    """Call generate_content; retry a few times on transient 503 UNAVAILABLE from Google."""
    max_attempts = 4
    for attempt in range(max_attempts):
        try:
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
        except Exception as e:
            t = str(e).lower()
            transient = "503" in t or "unavailable" in t
            if not transient or attempt == max_attempts - 1:
                raise
            time.sleep(min(2**attempt, 8))


async def _wait_for_active(client: genai.Client, uploaded, api_key: str) -> None:
    """Poll until file is ACTIVE or raise. Exponential backoff between polls (less API chatter)."""
    http = _get_http_client()
    deadline = time.time() + 120.0
    next_sleep = 3.0
    max_sleep = 15.0
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            file_info = await asyncio.to_thread(client.files.get, uploaded.name)
            state_name = (
                file_info.state.name
                if hasattr(file_info.state, "name")
                else str(file_info.state)
            )
            print(f"[Gemini] Poll {attempt}: state={state_name}")
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
                print(f"[Gemini] Poll {attempt} (REST): state={sn}")
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
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        sleep_for = min(next_sleep, remaining, max_sleep)
        await asyncio.sleep(sleep_for)
        next_sleep = min(next_sleep * 2, max_sleep)
    raise RuntimeError("انتهت مهلة الانتظار (120s) — الملف لم يصبح ACTIVE")


def _mid_gps_from_points(gps_points: list[dict]) -> str:
    """Representative GPS for the recording: middle sample of the points sent with this request."""
    if not gps_points:
        return ""
    mid_idx = (len(gps_points) - 1) // 2
    pt = gps_points[mid_idx]
    return f"{pt.get('lat', '')},{pt.get('lng', '')}"


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
    mid_street_gps = _mid_gps_from_points(gps_points)

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
        p["street_location"] = mid_street_gps

        p["recorder_name"] = recorder_name
        p["recording_date"] = now_str
        p["sheet_name"] = sheet_name

    return plates


async def process_audio(
    file_content: bytes,
    filename: str,
    api_key: str,
    model_name: str,
    recorder_name: str,
    sheet_name: str,
    gps_points: list[dict],
) -> list[dict]:
    """Upload audio to Gemini, extract plates, enrich (لوحات متكررة تُحفظ كما هي)."""
    suffix = Path(filename).suffix if filename else ".mp3"
    suffix = (suffix or ".mp3").encode("ascii", "ignore").decode("ascii") or ".mp3"
    sniffed = _sniff_audio_mime(file_content)
    gemini_mime = sniffed or _detect_mime(filename)

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
        return plates

    finally:
        try:
            await asyncio.to_thread(os.unlink, tmp_path)
        except Exception:
            pass
