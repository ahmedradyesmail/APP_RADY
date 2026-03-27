import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Existing app settings
    app_title: str = os.getenv("APP_NAME", "تفريغ API")
    app_version: str = "2.0.0"
    debug: bool = False
    port: int = 5001
    static_dir: str = "static"

    # Auth settings
    sqlite_db_url: str = "sqlite:///./app.db"
    jwt_secret_key: str = "change-this-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    admin_username: str = "admin"
    admin_password: str = "admin123"
    # SECURITY FIX: browser CORS origins loaded from environment.
    allowed_origins: str = ""

    # Optional: shared job status for multi-worker (Gunicorn). If unset, in-memory per worker.
    redis_url: str | None = None

    # SECURITY FIX: parse comma-separated ALLOWED_ORIGINS once.
    @property
    def allowed_origins_list(self) -> list[str]:
        return [x.strip() for x in self.allowed_origins.split(",") if x.strip()]

    class Config:
        extra = "ignore"
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()