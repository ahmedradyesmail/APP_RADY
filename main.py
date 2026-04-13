import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import aiofiles
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from config import settings
from db import Base, apply_sqlite_migrations, engine
from models import User
from routers.audio import router as audio_router
from routers.excel import router as excel_router
from routers.check import router as check_router
from routers.check_live_ws import router as check_live_ws_router
from routers.gps import router as gps_router
from routers.admin import router as admin_router
from routers.admin_check_storage import router as admin_check_storage_router
from routers.auth import router as auth_router
from services import gemini
from services import job_store as job_store_svc
from services.check_queue import start_check_queue, stop_check_queue
from routers.check import process_check_queue_item
from services.check_postgres import ensure_check_pg_schema
from services.rate_limit import limiter, rate_limit_exceeded_handler
from services.security import hash_password
from slowapi.errors import RateLimitExceeded


logger = logging.getLogger(__name__)


# SECURITY FIX: fail fast when sensitive env vars are left on unsafe defaults.
def _validate_sensitive_settings() -> None:
    blocked_values = {
        "JWT_SECRET_KEY": {"change-this-in-production", "your-secret-here"},
        "ADMIN_USERNAME": {"admin", "your-admin-username-here"},
        "ADMIN_PASSWORD": {"admin123", "your-admin-password-here"},
        "ALLOWED_ORIGINS": {"", "*"},
    }

    current_values = {
        "JWT_SECRET_KEY": settings.jwt_secret_key,
        "ADMIN_USERNAME": settings.admin_username,
        "ADMIN_PASSWORD": settings.admin_password,
        "ALLOWED_ORIGINS": settings.allowed_origins,
    }

    invalid_vars: list[str] = []
    for var_name, bad_set in blocked_values.items():
        if str(current_values[var_name]).strip() in bad_set:
            invalid_vars.append(var_name)

    if invalid_vars:
        details = ", ".join(invalid_vars)
        raise RuntimeError(
            "Security startup check failed. Update these .env variables before running: "
            f"{details}"
        )


# SECURITY FIX: execute sensitive configuration validation at startup import time.
_validate_sensitive_settings()


def bootstrap_admin(db: Session) -> None:
    existing_admin = db.query(User).filter(User.username == settings.admin_username).first()
    if existing_admin:
        return

    user = User(
        username=settings.admin_username,
        password_hash=hash_password(settings.admin_password),
        is_admin=True,
        is_active=True,
        device_id=None,
    )
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        # Another Gunicorn worker inserted the bootstrap admin at the same time.
        db.rollback()


def _sqlite_table_exists_error(exc: OperationalError) -> bool:
    msg = str(getattr(exc, "orig", None) or exc).lower()
    return "already exists" in msg


def _startup_db_sync() -> None:
    # Gunicorn runs multiple workers; each calls lifespan. SQLite DDL is not safe to
    # run concurrently — a second CREATE TABLE can raise "table ... already exists".
    try:
        Base.metadata.create_all(bind=engine, checkfirst=True)
    except OperationalError as e:
        if _sqlite_table_exists_error(e):
            pass
        else:
            raise
    apply_sqlite_migrations()
    with Session(engine) as db:
        bootstrap_admin(db)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await job_store_svc.init_job_store()
    await gemini.init_http_client()
    await start_check_queue(process_check_queue_item)
    dsn_pg = (settings.check_postgres_dsn or "").strip()
    if dsn_pg:
        try:
            await asyncio.to_thread(ensure_check_pg_schema, dsn_pg)
        except Exception:
            # Fail-open: app still runs; check Postgres routes will error until DSN/schema fixed.
            logger.exception("check_postgres schema init failed (CHECK_POSTGRES_URL)")
    await asyncio.to_thread(_startup_db_sync)
    yield
    await stop_check_queue()
    await gemini.close_http_client()
    await job_store_svc.close_job_store()


app = FastAPI(
    title=settings.app_title,
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
# SECURITY FIX: enforce explicit CORS origins from environment configuration.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_methods=["*"],
    allow_headers=["*"],
)
# SECURITY FIX: attach shared rate limiter to application state.
app.state.limiter = limiter
# SECURITY FIX: return controlled JSON for rate-limit errors.
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(audio_router)
app.include_router(excel_router)
app.include_router(check_router)
app.include_router(check_live_ws_router)
app.include_router(gps_router)

app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(admin_check_storage_router)


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}


static_path = Path(settings.static_dir)
if static_path.exists():
    app.mount(
        "/static",
        StaticFiles(directory=str(static_path)),
        name="static",
    )


@app.get("/admin/check-storage", response_class=HTMLResponse, include_in_schema=False)
async def admin_check_storage_page():
    """لوحة إدارة: استيراد الفرز (Postgres) لكل مستخدم."""
    f = static_path / "admin-check-storage.html"
    if f.exists():
        async with aiofiles.open(str(f), mode="r", encoding="utf-8") as fh:
            return HTMLResponse(
                content=await fh.read(),
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )
    return HTMLResponse("<p>admin-check-storage.html not found</p>", status_code=404)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index():
    index_file = static_path / "index.html"
    if index_file.exists():
        async with aiofiles.open(str(index_file), mode="r", encoding="utf-8") as f:
            content = await f.read()
        return HTMLResponse(
            content=content,
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return HTMLResponse("<h1>index.html not found in static/</h1>", status_code=404)


@app.get("/lame.min.js", include_in_schema=False)
async def lame_js():
    lame_file = static_path / "lame.min.js"
    if lame_file.exists():
        return FileResponse(
            str(lame_file),
            media_type="application/javascript",
            headers={"Cache-Control": "public, max-age=604800"},
        )
    return HTMLResponse("lame.min.js not found", status_code=404)


if __name__ == "__main__":
    import uvicorn

    print(f"🚗  التفريغ — Server running → http://localhost:{settings.port}")
    print(f"     Docs: http://localhost:{settings.port}/docs")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.port,
        reload=settings.debug,
    )
