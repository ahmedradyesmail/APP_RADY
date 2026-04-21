import os

from pydantic_settings import BaseSettings
from urllib.parse import quote_plus


class Settings(BaseSettings):
    # Existing app settings
    app_title: str = os.getenv("APP_NAME", "تفريغ API")
    app_version: str = "2.0.0"
    debug: bool = False
    port: int = 5001
    static_dir: str = "static"

    # Primary DB for auth/users/tokens. Use PostgreSQL in production.
    database_url: str = ""
    # Legacy local fallback (used when DATABASE_URL is empty).
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

    # Concurrent Gemini Live WebSocket sessions per process (each holds an upstream WS).
    gemini_live_max_concurrent: int = 8
    # Check (فرز) queue limits (in-process workers).
    check_queue_workers: int = 2
    check_queue_max_depth: int = 200
    check_user_rate_limit_per_minute: int = 3
    # Live check workbook cache: keep session alive while active; cleanup after idle.
    check_live_idle_ttl_seconds: int = 30 * 60
    check_live_hard_ttl_seconds: int = 4 * 60 * 60

    # Optional: Postgres for الفرز large-file storage (RLS by JWT user_id). Empty = legacy two-file temp only.
    check_postgres_url: str = ""
    # Optional split config (Railway-friendly): used only when CHECK_POSTGRES_URL is empty.
    check_postgres_host: str = ""
    check_postgres_port: int = 5432
    check_postgres_dbname: str = ""
    check_postgres_user: str = ""
    check_postgres_password: str = ""

    # SECURITY FIX: parse comma-separated ALLOWED_ORIGINS once.
    @property
    def allowed_origins_list(self) -> list[str]:
        return [x.strip() for x in self.allowed_origins.split(",") if x.strip()]

    @property
    def check_postgres_dsn(self) -> str:
        dsn = (self.check_postgres_url or "").strip()
        if dsn:
            return dsn
        host = (self.check_postgres_host or "").strip()
        dbname = (self.check_postgres_dbname or "").strip()
        user = (self.check_postgres_user or "").strip()
        if not (host and dbname and user):
            # Single-DB mode: reuse primary database when check-specific DSN is not set.
            return (self.database_url or "").strip()
        pwd = quote_plus(self.check_postgres_password or "")
        port = int(self.check_postgres_port or 5432)
        return f"postgresql://{quote_plus(user)}:{pwd}@{host}:{port}/{quote_plus(dbname)}"

    @property
    def auth_db_url(self) -> str:
        """SQLAlchemy URL for core auth tables."""
        return (self.database_url or "").strip() or self.sqlite_db_url

    class Config:
        extra = "ignore"
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()