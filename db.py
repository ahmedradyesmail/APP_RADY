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
        if "group_id" not in col_names:
            try:
                conn.execute(
                    text(
                        "ALTER TABLE users ADD COLUMN group_id INTEGER REFERENCES user_groups(id)"
                    )
                )
            except OperationalError as e:
                if "duplicate column" not in str(getattr(e, "orig", None) or e).lower():
                    raise
        if "max_stored_large_rows" not in col_names:
            try:
                conn.execute(
                    text(
                        "ALTER TABLE users ADD COLUMN max_stored_large_rows INTEGER"
                    )
                )
            except OperationalError as e:
                if "duplicate column" not in str(getattr(e, "orig", None) or e).lower():
                    raise
        g_rows = conn.execute(text("PRAGMA table_info(user_groups)")).fetchall()
        g_col_names = {r[1] for r in g_rows}
        if "max_stored_large_rows" not in g_col_names:
            try:
                conn.execute(
                    text(
                        "ALTER TABLE user_groups ADD COLUMN max_stored_large_rows INTEGER"
                    )
                )
            except OperationalError as e:
                if "duplicate column" not in str(getattr(e, "orig", None) or e).lower():
                    raise
        # Provider keys moved to Redis pools — drop legacy table if present.
        try:
            conn.execute(text("DROP TABLE IF EXISTS provider_api_key_settings"))
        except OperationalError:
            pass


def apply_postgres_auth_migrations() -> None:
    """user_groups + users.group_id when auth DB is PostgreSQL."""
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_groups (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL UNIQUE
                )
                """
            )
        )
        row = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'users'
                  AND column_name = 'group_id'
                """
            )
        ).fetchone()
        if not row:
            try:
                conn.execute(
                    text(
                        """
                        ALTER TABLE users ADD COLUMN group_id INTEGER
                        REFERENCES user_groups(id) ON DELETE SET NULL
                        """
                    )
                )
            except OperationalError as e:
                if "already exists" not in str(getattr(e, "orig", None) or e).lower():
                    raise
        row = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'users'
                  AND column_name = 'max_stored_large_rows'
                """
            )
        ).fetchone()
        if not row:
            conn.execute(
                text(
                    "ALTER TABLE users ADD COLUMN max_stored_large_rows INTEGER"
                )
            )
        row = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'user_groups'
                  AND column_name = 'max_stored_large_rows'
                """
            )
        ).fetchone()
        if not row:
            conn.execute(
                text(
                    "ALTER TABLE user_groups ADD COLUMN max_stored_large_rows INTEGER"
                )
            )


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
