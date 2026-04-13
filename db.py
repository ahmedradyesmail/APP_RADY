from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker

from config import settings


_db_url = settings.auth_db_url
_connect_args = {"check_same_thread": False} if _db_url.startswith("sqlite") else {}
engine = create_engine(_db_url, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def apply_sqlite_migrations() -> None:
    """Add columns missing from older DB files (SQLite only)."""
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(users)")).fetchall()
        col_names = {r[1] for r in rows}
        if "is_active" not in col_names:
            try:
                conn.execute(
                    text("ALTER TABLE users ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT 1")
                )
            except OperationalError as e:
                # Concurrent workers may both attempt ALTER; second sees column already added.
                if "duplicate column" not in str(getattr(e, "orig", None) or e).lower():
                    raise


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
