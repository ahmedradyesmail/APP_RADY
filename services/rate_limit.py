"""
# SECURITY FIX: shared rate limiter and JSON 429 handler.
"""

from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address


# SECURITY FIX: single limiter instance used by auth routes.
limiter = Limiter(key_func=get_remote_address)


# SECURITY FIX: clear 429 response message for clients.
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={"detail": "Too many requests. Please wait and try again."},
    )
