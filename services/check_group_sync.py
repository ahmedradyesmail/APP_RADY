"""Mirror فرز user↔group membership to Postgres for RLS (peer data access)."""

from __future__ import annotations

import logging

import psycopg

logger = logging.getLogger(__name__)


def ensure_check_mirror_user_groups_table(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS check_mirror_user_groups (
                user_id INTEGER NOT NULL PRIMARY KEY,
                group_id INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cmug_group ON check_mirror_user_groups (group_id)"
        )


def sync_user_group_membership_postgres(dsn: str, user_id: int, group_id: int | None) -> None:
    """
    Individual users: no row (only own check_large_* via RLS).
    Group members: one row (user_id, sqlite group id).
    """
    ensure_check_mirror_user_groups_table(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        if group_id is None:
            conn.execute(
                "DELETE FROM check_mirror_user_groups WHERE user_id = %s",
                (user_id,),
            )
        else:
            conn.execute(
                """
                INSERT INTO check_mirror_user_groups (user_id, group_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET group_id = EXCLUDED.group_id
                """,
                (user_id, group_id),
            )


def delete_group_mirrors_postgres(dsn: str, group_id: int) -> None:
    """عند حذف مجموعة من SQLite: إزالة كل صفوف المرآة لهذا group_id."""
    ensure_check_mirror_user_groups_table(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM check_mirror_user_groups WHERE group_id = %s",
            (group_id,),
        )
