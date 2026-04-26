from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=6, max_length=128)


class RefreshRequest(BaseModel):
    """Optional body: refresh_token; if omitted, cookie tfg_refresh is used."""

    refresh_token: str | None = None


class AuthSessionOut(BaseModel):
    """JWTs are set as HttpOnly cookies; JSON is for UI flags only."""

    is_admin: bool = False
    token_type: str = "bearer"


class TokenResponse(BaseModel):
    """Legacy JSON token response (non-browser clients). Prefer cookies in browsers."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    is_admin: bool = False


class MeOut(BaseModel):
    username: str
    is_admin: bool
    group_id: int | None = None
    group_name: str | None = None
