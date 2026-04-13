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
    "You extract Arabic car plate text from speech. "
    "Return JSON only. "
    "Do not explain. Do not add words. Do not normalize linguistically. "
    "Assume the user speaks plate letters one-by-one (not words). "
    "Treat any spoken letter name as the single Arabic letter (e.g., 'راء' or 'ره' => 'ر')."
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

Valid example: {"plate":"وصر 4923"}"""

# Live model IDs change; use env to override without code edits.
# If you see WebSocket 1011 on connect, try another ID from https://ai.google.dev/gemini-api/docs/models
_DEFAULT_LIVE_MODELS = (
    "models/gemini-3.1-flash-live",
    "models/gemini-3.1-flash-live-preview",
    "models/gemini-2.5-flash-native-audio-preview-12-2025",
)
_LIVE_VOICE = os.getenv("GEMINI_LIVE_VOICE", "Kore")


def _live_model_candidates() -> list[str]:
    single = (os.getenv("GEMINI_LIVE_MODEL") or "").strip()
    if single:
        return [single]
    return list(_DEFAULT_LIVE_MODELS)

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


async def _try_connect(api_key: str) -> GeminiLiveSession:
    last_err: Exception | None = None
    for model in _live_model_candidates():
        try:
            return await _try_connect_one(api_key, model)
        except websockets.exceptions.ConnectionClosedError as e:
            last_err = e
            logger.warning(
                "Live WS closed during setup model=%s code=%s reason=%s",
                model,
                getattr(e, "code", None),
                getattr(e, "reason", None) or str(e),
            )
        except Exception as e:
            last_err = e
            logger.warning("Live setup failed model=%s: %s", model, e)
    raise RuntimeError(
        "Could not start Gemini Live session. "
        "Set GEMINI_LIVE_MODEL to a valid Live model from "
        "https://ai.google.dev/gemini-api/docs/models — last error: "
        f"{last_err!r}"
    ) from last_err


@asynccontextmanager
async def create_gemini_session(api_key: str | None = None):
    key = (api_key or "").strip() or os.getenv("GEMINI_API_KEY", "")
    if not key:
        raise ValueError("GEMINI API key missing (init message or GEMINI_API_KEY in .env)")

    candidates = _live_model_candidates()
    logger.info("Connecting Live (candidates=%s)", candidates)
    session = await _try_connect(key)
    try:
        yield session
    finally:
        await session.close()
