"""
Gemini Live API — direct WebSocket connection (no SDK).
Bypasses all SDK/version issues entirely.
"""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

import websockets

logger = logging.getLogger(__name__)

# Live / Bidi API is documented on v1beta (v1alpha rejects current models with 1008).
# https://ai.google.dev/api/live
GEMINI_WS_URL = (
    "wss://generativelanguage.googleapis.com"
    "/ws/google.ai.generativelanguage.v1beta"
    ".GenerativeService.BidiGenerateContent"
)

# English-only instructions for the Live model (systemInstruction.parts).
SYSTEM_INSTRUCTION = (
    "You extract Egyptian/Arabic license plates from spoken Arabic audio. "
    "Return JSON ONLY (per SYSTEM_PROMPT). No markdown, no prose, no extra keys. "
    "The user usually spells letters one-by-one, then digits, e.g. 'و ص ر 3424'. "
    "If you hear Arabic letter names (واو/صاد/راء/حاء/هاء/عين/غين/قاف/كاف/...), map each to exactly ONE Arabic plate letter. "
    "Never output Latin letters in JSON (no 'h' etc.). If audio sounds like an English letter name, treat it as an Arabic letter-name cue, not Latin text. "
    "Do NOT expand a spoken letter into a full Arabic word in the output (e.g. never output 'الف' instead of 'ا'). "
    "If audio is ambiguous/low confidence, return {\"plate\":null} instead of guessing. "
    "Each user/model turn is independent: do not carry over or repeat a plate from an earlier turn unless clearly re-spoken in the current turn. "
    "Plate constraints are strict: letters must never exceed 3 Arabic letters, digits must never exceed 4 numbers. "
    "Preferred format is 3 letters + 4 digits when clearly heard. "
    "Never output letter-name words for plate letters (e.g. do not output 'عين' or 'عن'; output single letter 'ع'). "
    "\n"
    "تثبيت أخطاء شائعة في النطق (لو سمعت الاسم، اختر الحرف الصحيح للوحة): "
    "حاء/حا/حه => ح (وليس ه). "
    "هاء/ها/هه => ه (وليس ح). "
    "عين/عاين => ع (وليس غ ولا ق). "
    "غين/غاين => غ (وليس ع). "
    "قاف => ق (وليس ك). "
    "كاف => ك (وليس ق)."
)

SYSTEM_PROMPT = """Output must be ONLY one of:
{"plate":"<letters> <digits>"}
{"plates":[{"plate":"<letters> <digits>"}, ...]}
{"plate":null}

Rules:
1) Keep Arabic letters exactly as spoken for plate letters.
2) Do NOT expand letters into words (e.g., never convert 'ا' to 'الف').
3) Do NOT merge extra letters; keep only plate letters.
4) Letters block then one ASCII space then digits block.
5) Digits must be Western 0-9 only.
6) No markdown, no extra keys, no extra text.
7) The letters block must contain Arabic letters ONLY (Unicode Arabic letters). Never output Latin letters.
8) Letters count must be 1..3 (preferred 3). Never output >3 letters.
9) Digits count must be 1..4 (preferred 4). Never output >4 digits.
10) If you hear a letter name, convert it to one Arabic character only (e.g. عين/عن -> ع).

Valid example: {"plate":"وصر 4923"}"""

# Live-capable model IDs come only from the admin Gemini catalog (channel=live);
# the WebSocket client sends the chosen model_id after /api/config/gemini-models.
_LIVE_VOICE = os.getenv("GEMINI_LIVE_VOICE", "Kore")


class GeminiLiveSession:
    """Thin wrapper around a raw WebSocket to Gemini Live."""

    def __init__(self, ws):
        self._ws = ws

    async def send_audio(self, base64_data: str, end_of_turn: bool = False):
        # Prefer `audio` blob (mediaChunks deprecated per Live API reference).
        payload = {
            "realtimeInput": {
                "audio": {
                    "data": base64_data,
                    "mimeType": "audio/pcm;rate=16000",
                }
            }
        }
        await self._ws.send(json.dumps(payload))
        if end_of_turn:
            await self._ws.send(json.dumps({"realtimeInput": {"audioStreamEnd": True}}))

    async def send_end_of_turn(self):
        # Empty clientContent.turnComplete alone is invalid on Gemini 3.1 Live (1007).
        await self._ws.send(json.dumps({"realtimeInput": {"audioStreamEnd": True}}))

    async def send_text(self, text: str):
        await self._ws.send(json.dumps({"realtimeInput": {"text": text}}))

    async def receive_one(self):
        raw = await self._ws.recv()
        return json.loads(raw)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.receive_one()
        except websockets.exceptions.ConnectionClosed:
            raise StopAsyncIteration

    async def close(self):
        try:
            await self._ws.close()
        except Exception:
            pass


async def _try_connect_one(api_key: str, model: str) -> GeminiLiveSession:
    url = f"{GEMINI_WS_URL}?key={api_key}"
    ws = await websockets.connect(
        url,
        additional_headers={"Content-Type": "application/json"},
        open_timeout=15,
        ping_interval=20,
        ping_timeout=10,
    )
    setup = {
        "setup": {
            "model": model,
            "generationConfig": {
                # Keep AUDIO modality for protocol compatibility with native-audio model.
                "responseModalities": ["AUDIO"],
                "speechConfig": {
                    "voiceConfig": {
                        "prebuiltVoiceConfig": {"voiceName": _LIVE_VOICE},
                    }
                },
            },
            # We parse JSON from transcription text; generated audio is ignored by frontend.
            "outputAudioTranscription": {},
            "inputAudioTranscription": {},
            "systemInstruction": {
                "parts": [
                    {"text": SYSTEM_INSTRUCTION},
                    {"text": SYSTEM_PROMPT},
                ]
            },
        }
    }
    try:
        await ws.send(json.dumps(setup))
        resp_raw = await asyncio.wait_for(ws.recv(), timeout=15)
        resp = json.loads(resp_raw)
        if "error" in resp:
            await ws.close()
            raise RuntimeError(f"Setup error: {resp['error']}")
        logger.info("Connected Live model: %s", model)
        return GeminiLiveSession(ws)
    except Exception:
        try:
            await ws.close()
        except Exception:
            pass
        raise


@asynccontextmanager
async def create_gemini_session(
    api_key: str | None = None,
    live_model: str | None = None,
):
    """Connect to Gemini Live using the single model id chosen by the client (admin catalog)."""
    key = (api_key or "").strip()
    if not key:
        raise ValueError("GEMINI API key missing")

    primary = (live_model or "").strip()
    if not primary:
        raise ValueError(
            "live_model is required — add enabled Live models in admin and pick one in the UI."
        )

    try:
        session = await _try_connect_one(key, primary)
    except Exception as e:
        logger.warning("Live connect failed model=%s: %s", primary, e)
        raise RuntimeError(
            f"Could not start Gemini Live session for model {primary!r}. "
            f"Error: {e!r}"
        ) from e

    logger.info("Connected Live model=%s", primary)

    try:
        yield session
    finally:
        await session.close()
