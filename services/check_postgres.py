"""Postgres-backed storage for الفرز: عدة ملفات كبيرة لكل مستخدم + RLS."""

from __future__ import annotations

import io
import itertools
import json
import logging
import re
import threading
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import openpyxl
import psycopg
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from psycopg.rows import dict_row
from psycopg.types.json import Json

from services.check_match import (
    PREVIEW_MAX_ROWS,
    _norm_large_export_cols,
    _norm_small_export_cols,
    _strip_small_word_from_header_title,
)
from services.excel_utils import find_best_sheet, load_workbook_maybe_encrypted, workbook_to_bytes
from services.plate_utils import (
    auto_detect_plate_col,
    auto_detect_plate_col_from_row3,
    normalize_plate,
)

logger = logging.getLogger(__name__)

CHECK_PG_MAX_LARGE_BYTES = 15 * 1024 * 1024
CHECK_PG_MAX_ROWS_PER_USER = 3_000_000
GPS_HEADER = "GPS"

# ألوان ترويسة أقسام الملفات الكبيرة (دوّار)
FILE_SECTION_HEADER_FILLS = [
    "4472C4",
    "C65911",
    "70AD47",
    "9F4F96",
    "FFC000",
    "E15759",
    "5B9BD5",
    "264478",
    "ED7D31",
    "A5A5A5",
]

_schema_lock = threading.Lock()
_schema_initialized: set[str] = set()


def _jsonable_cell(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    from datetime import date, time
    from datetime import datetime as dt

    if isinstance(v, (dt, date, time)):
        return str(v)
    return str(v)


def _safe_filename(name: str) -> str:
    s = (name or "").strip() or "upload.xlsx"
    s = re.sub(r"[/\\<>|\":?*]", "_", s)[:240]
    return s


@contextmanager
def check_pg_tx(dsn: str, user_id: int, is_admin: bool):
    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('app.user_id', %s, true)", (str(int(user_id)),))
            cur.execute(
                "SELECT set_config('app.is_admin', %s, true)",
                ("true" if is_admin else "false",),
            )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _thin_border() -> Border:
    t = Side(style="thin", color="BFBFBF")
    return Border(left=t, right=t, top=t, bottom=t)


def _apply_migrations(conn) -> None:
    """Idempotent: check_large_imports + import_id على check_large_rows."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS check_large_imports (
                id BIGSERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                filename TEXT NOT NULL DEFAULT '',
                sheet_name TEXT NOT NULL DEFAULT '',
                plate_column TEXT NOT NULL DEFAULT '',
                column_order JSONB NOT NULL DEFAULT '[]'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = 'public' AND table_name = 'check_large_rows'
            )
            """
        )
        rows_exists = cur.fetchone()[0]
        if not rows_exists:
            cur.execute(
                """
                CREATE TABLE check_large_rows (
                    id BIGSERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    import_id BIGINT NOT NULL REFERENCES check_large_imports(id) ON DELETE CASCADE,
                    plate_normalized TEXT NOT NULL,
                    row_data JSONB NOT NULL,
                    gps TEXT NOT NULL DEFAULT ''
                )
                """
            )
        else:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.columns
                  WHERE table_schema = 'public' AND table_name = 'check_large_rows'
                    AND column_name = 'import_id'
                )
                """
            )
            has_import_id = cur.fetchone()[0]
            if not has_import_id:
                cur.execute(
                    "ALTER TABLE check_large_rows ADD COLUMN import_id BIGINT REFERENCES check_large_imports(id) ON DELETE CASCADE"
                )
                cur.execute(
                    """
                    INSERT INTO check_large_imports (user_id, filename, sheet_name, plate_column, column_order, created_at)
                    SELECT m.user_id, 'legacy-import', COALESCE(m.sheet_name, ''), '', m.column_order, m.updated_at
                    FROM check_large_meta m
                    WHERE NOT EXISTS (
                      SELECT 1 FROM check_large_imports i WHERE i.user_id = m.user_id AND i.filename = 'legacy-import'
                    )
                    """
                )
                cur.execute(
                    """
                    UPDATE check_large_rows r
                    SET import_id = i.id
                    FROM check_large_imports i
                    WHERE r.import_id IS NULL AND i.user_id = r.user_id AND i.filename = 'legacy-import'
                    """
                )
                cur.execute(
                    """
                    INSERT INTO check_large_imports (user_id, filename, sheet_name, plate_column, column_order)
                    SELECT DISTINCT r.user_id, 'migrated', '', '', '[]'::jsonb
                    FROM check_large_rows r
                    WHERE r.import_id IS NULL
                      AND NOT EXISTS (
                        SELECT 1 FROM check_large_imports i
                        WHERE i.user_id = r.user_id AND i.filename = 'migrated'
                      )
                    """
                )
                cur.execute(
                    """
                    UPDATE check_large_rows r
                    SET import_id = i.id
                    FROM check_large_imports i
                    WHERE r.import_id IS NULL AND i.user_id = r.user_id AND i.filename = 'migrated'
                    """
                )
                cur.execute("DELETE FROM check_large_rows WHERE import_id IS NULL")
                cur.execute(
                    "ALTER TABLE check_large_rows ALTER COLUMN import_id SET NOT NULL"
                )
        cur.execute("DROP TABLE IF EXISTS check_large_meta CASCADE")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_clr_import_plate ON check_large_rows (import_id, plate_normalized)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_clr_user ON check_large_rows (user_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_clr_user_plate ON check_large_rows (user_id, plate_normalized)"
        )
        cur.execute(
            """
            DO $p$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_policies WHERE schemaname = 'public'
                  AND tablename = 'check_large_imports' AND policyname = 'check_large_imports_isolation'
              ) THEN
                CREATE POLICY check_large_imports_isolation ON check_large_imports FOR ALL
                USING (
                  COALESCE(current_setting('app.is_admin', true), '') = 'true'
                  OR user_id = (NULLIF(current_setting('app.user_id', true), ''))::integer
                )
                WITH CHECK (
                  COALESCE(current_setting('app.is_admin', true), '') = 'true'
                  OR user_id = (NULLIF(current_setting('app.user_id', true), ''))::integer
                );
              END IF;
            END
            $p$;
            """
        )


def ensure_check_pg_schema(dsn: str) -> None:
    with _schema_lock:
        if dsn in _schema_initialized:
            return
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS check_large_imports (
            id BIGSERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            filename TEXT NOT NULL DEFAULT '',
            sheet_name TEXT NOT NULL DEFAULT '',
            plate_column TEXT NOT NULL DEFAULT '',
            column_order JSONB NOT NULL DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
    ]
    policy_rows = """
    DO $p$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_policies WHERE schemaname = 'public'
          AND tablename = 'check_large_rows' AND policyname = 'check_large_rows_isolation'
      ) THEN
        CREATE POLICY check_large_rows_isolation ON check_large_rows FOR ALL
        USING (
          COALESCE(current_setting('app.is_admin', true), '') = 'true'
          OR user_id = (NULLIF(current_setting('app.user_id', true), ''))::integer
        )
        WITH CHECK (
          COALESCE(current_setting('app.is_admin', true), '') = 'true'
          OR user_id = (NULLIF(current_setting('app.user_id', true), ''))::integer
        );
      END IF;
    END
    $p$;
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        for sql in stmts:
            conn.execute(sql)
        _apply_migrations(conn)
        conn.execute("ALTER TABLE check_large_rows ENABLE ROW LEVEL SECURITY")
        conn.execute("ALTER TABLE check_large_rows FORCE ROW LEVEL SECURITY")
        conn.execute("ALTER TABLE check_large_imports ENABLE ROW LEVEL SECURITY")
        conn.execute("ALTER TABLE check_large_imports FORCE ROW LEVEL SECURITY")
        conn.execute(policy_rows)
        conn.execute(
            """
            DO $p$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_policies WHERE schemaname = 'public'
                  AND tablename = 'check_large_imports' AND policyname = 'check_large_imports_isolation'
              ) THEN
                CREATE POLICY check_large_imports_isolation ON check_large_imports FOR ALL
                USING (
                  COALESCE(current_setting('app.is_admin', true), '') = 'true'
                  OR user_id = (NULLIF(current_setting('app.user_id', true), ''))::integer
                )
                WITH CHECK (
                  COALESCE(current_setting('app.is_admin', true), '') = 'true'
                  OR user_id = (NULLIF(current_setting('app.user_id', true), ''))::integer
                );
              END IF;
            END
            $p$;
            """
        )
    with _schema_lock:
        _schema_initialized.add(dsn)
    logger.info("check_postgres schema ensured (multi-import)")


def count_user_large_rows_sync(dsn: str, user_id: int, is_admin: bool) -> int:
    ensure_check_pg_schema(dsn)
    with check_pg_tx(dsn, user_id, is_admin) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::bigint FROM check_large_rows WHERE user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            return int(row[0] if row else 0)


def list_imports_sync(dsn: str, user_id: int, is_admin: bool) -> list[dict[str, Any]]:
    ensure_check_pg_schema(dsn)
    with check_pg_tx(dsn, user_id, is_admin) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT i.id, i.filename, i.sheet_name, i.plate_column, i.created_at,
                       (SELECT COUNT(*)::int FROM check_large_rows r WHERE r.import_id = i.id) AS row_count
                FROM check_large_imports i
                WHERE i.user_id = %s
                ORDER BY i.created_at ASC, i.id ASC
                """,
                (user_id,),
            )
            out = []
            for r in cur.fetchall():
                out.append(
                    {
                        "id": int(r["id"]),
                        "filename": r.get("filename") or "",
                        "sheet_name": r.get("sheet_name") or "",
                        "plate_column": r.get("plate_column") or "",
                        "row_count": int(r["row_count"] or 0),
                        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                    }
                )
            return out


def delete_import_sync(dsn: str, user_id: int, is_admin: bool, import_id: int) -> bool:
    ensure_check_pg_schema(dsn)
    with check_pg_tx(dsn, user_id, is_admin) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM check_large_imports WHERE id = %s AND user_id = %s",
                (import_id, user_id),
            )
            return cur.rowcount > 0


def get_stored_large_meta_sync(dsn: str, user_id: int, is_admin: bool) -> dict[str, Any] | None:
    """اتحاد أعمدة كل الاستيرادات + إجمالي الصفوف."""
    ensure_check_pg_schema(dsn)
    imports = list_imports_sync(dsn, user_id, is_admin)
    if not imports:
        return None
    total_rows = sum(i["row_count"] for i in imports)
    if total_rows == 0:
        return None
    seen: set[str] = set()
    union_headers: list[str] = []
    with check_pg_tx(dsn, user_id, is_admin) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT column_order FROM check_large_imports
                WHERE user_id = %s
                ORDER BY created_at ASC, id ASC
                """,
                (user_id,),
            )
            for r in cur.fetchall():
                co = r["column_order"]
                arr = co if isinstance(co, list) else json.loads(co) if co else []
                for h in arr:
                    hs = str(h).strip() if h else ""
                    if hs and hs not in seen:
                        seen.add(hs)
                        union_headers.append(hs)
    return {
        "headers": union_headers,
        "sheet_name": "",
        "row_count": total_rows,
        "updated_at": imports[-1].get("created_at") if imports else None,
        "imports": imports,
        "has_data": True,
    }


def admin_list_check_storage_sync(dsn: str, admin_user_id: int, is_admin: bool) -> list[dict[str, Any]]:
    if not is_admin:
        return []
    ensure_check_pg_schema(dsn)
    with check_pg_tx(dsn, admin_user_id, True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT i.user_id AS user_id,
                       COUNT(DISTINCT i.id)::int AS import_count,
                       COALESCE(
                           (SELECT COUNT(*)::bigint FROM check_large_rows r WHERE r.user_id = i.user_id),
                           0
                       ) AS row_count,
                       MAX(i.created_at) AS last_import
                FROM check_large_imports i
                GROUP BY i.user_id
                ORDER BY last_import DESC NULLS LAST
                """
            )
            out = []
            for r in cur.fetchall():
                out.append(
                    {
                        "user_id": r["user_id"],
                        "import_count": int(r["import_count"] or 0),
                        "row_count": int(r["row_count"] or 0),
                        "updated_at": r["last_import"].isoformat() if r.get("last_import") else None,
                    }
                )
            return out


def import_large_workbook_sync(
    dsn: str,
    user_id: int,
    is_admin: bool,
    lc_bytes: bytes,
    password: str,
    large_col: str,
    large_sheet: str,
    source_filename: str,
) -> dict[str, Any]:
    ensure_check_pg_schema(dsn)
    if len(lc_bytes) > CHECK_PG_MAX_LARGE_BYTES:
        raise ValueError("الملف الكبير يتجاوز 15 ميجابايت")

    fname = _safe_filename(source_filename)
    large_wb = load_workbook_maybe_encrypted(lc_bytes, password)
    try:
        if len(large_wb.sheetnames) != 1:
            raise ValueError(
                "يجب أن يحتوي الملف الكبير على ورقة عمل واحدة فقط. "
                f"الموجود: {len(large_wb.sheetnames)} ({', '.join(large_wb.sheetnames)})"
            )
        large_ws = (
            large_wb[large_sheet]
            if large_sheet and large_sheet in large_wb.sheetnames
            else find_best_sheet(large_wb)
        )
        lrows = list(large_ws.iter_rows(values_only=True))
        if not lrows:
            raise ValueError("الملف الكبير فارغ")
        header_l = lrows[0]
        lh = [str(h).strip() if h is not None else "" for h in header_l]
        lc = (large_col or "").strip() or (auto_detect_plate_col(lh) or "")
        if not lc or lc not in lh:
            raise ValueError(
                f"لم يُعثر على عمود اللوحة في الملف الكبير. الأعمدة: {lh}"
            )
        lci = lh.index(lc)
        new_row_count = 0
        for row in lrows[1:]:
            if all(v is None for v in row):
                continue
            rp = row[lci] if lci < len(row) else None
            if normalize_plate(rp):
                new_row_count += 1

        current_total = count_user_large_rows_sync(dsn, user_id, is_admin)
        if current_total + new_row_count > CHECK_PG_MAX_ROWS_PER_USER:
            raise ValueError(
                f"تجاوز الحد الأقصى {CHECK_PG_MAX_ROWS_PER_USER:,} صف لكل مستخدم "
                f"(الحالي {current_total:,} + الجديد {new_row_count:,})."
            )

        batch: list[tuple[int, int, str, Json, str]] = []
        batch_size = 2000
        total = 0
        import_id: int | None = None
        with check_pg_tx(dsn, user_id, is_admin) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM check_large_imports
                    WHERE user_id = %s
                      AND lower(trim(filename)) = lower(trim(%s))
                    LIMIT 1
                    """,
                    (user_id, fname),
                )
                if cur.fetchone():
                    raise ValueError(
                        f"يوجد ملف بنفس الاسم مسبقاً: {fname}. غيّر الاسم أو احذف الاستيراد القديم أولاً."
                    )
                cur.execute(
                    """
                    INSERT INTO check_large_imports (user_id, filename, sheet_name, plate_column, column_order)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    RETURNING id
                    """,
                    (
                        user_id,
                        fname,
                        large_ws.title,
                        lc,
                        json.dumps(lh, ensure_ascii=False),
                    ),
                )
                import_id = int(cur.fetchone()[0])
                for row in lrows[1:]:
                    if all(v is None for v in row):
                        continue
                    rp = row[lci] if lci < len(row) else None
                    pn = normalize_plate(rp)
                    if not pn:
                        continue
                    rd: dict[str, Any] = {}
                    gps_val = ""
                    for hi, h in enumerate(lh):
                        if not h:
                            continue
                        cell = row[hi] if hi < len(row) else None
                        if h == GPS_HEADER:
                            gps_val = str(cell).strip() if cell is not None else ""
                        rd[h] = _jsonable_cell(cell)
                    batch.append((user_id, import_id, pn, Json(rd), gps_val))
                    total += 1
                    if len(batch) >= batch_size:
                        cur.executemany(
                            """
                            INSERT INTO check_large_rows (user_id, import_id, plate_normalized, row_data, gps)
                            VALUES (%s, %s, %s, %s::jsonb, %s)
                            """,
                            batch,
                        )
                        batch.clear()
                if batch:
                    cur.executemany(
                        """
                        INSERT INTO check_large_rows (user_id, import_id, plate_normalized, row_data, gps)
                        VALUES (%s, %s, %s, %s::jsonb, %s)
                        """,
                        batch,
                    )
        if total == 0 and import_id is not None:
            with check_pg_tx(dsn, user_id, is_admin) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM check_large_imports WHERE id = %s AND user_id = %s",
                        (import_id, user_id),
                    )
            raise ValueError("لا توجد صفوف بلوحات صالحة في الملف الكبير")
        return {
            "row_count": total,
            "headers": lh,
            "sheet_name": large_ws.title,
            "large_col_used": lc,
            "import_id": import_id,
            "filename": fname,
        }
    finally:
        try:
            large_wb.close()
        except Exception:
            pass


def _cell_display(v: Any) -> Any:
    if v is None:
        return None
    return v


def _fetch_matches_by_import(
    conn, user_id: int, plate_normalized: str
) -> dict[int, list[dict[str, Any]]]:
    out: dict[int, list[dict[str, Any]]] = defaultdict(list)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT import_id, row_data
            FROM check_large_rows
            WHERE user_id = %s AND plate_normalized = %s AND import_id IS NOT NULL
            ORDER BY import_id ASC, id ASC
            """,
            (user_id, plate_normalized),
        )
        for row in cur.fetchall():
            iid = row.get("import_id")
            rd = row.get("row_data")
            if iid is None:
                continue
            d = rd if isinstance(rd, dict) else json.loads(rd) if isinstance(rd, str) else rd
            out[int(iid)].append(d)
    return out


def _load_imports_ordered(conn, user_id: int) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, filename, sheet_name, plate_column, column_order
            FROM check_large_imports
            WHERE user_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (user_id,),
        )
        rows = []
        for r in cur.fetchall():
            co = r["column_order"]
            if isinstance(co, str):
                co = json.loads(co)
            rows.append(
                {
                    "id": int(r["id"]),
                    "filename": r.get("filename") or "",
                    "sheet_name": r.get("sheet_name") or "",
                    "plate_column": r.get("plate_column") or "",
                    "column_order": list(co) if co else [],
                }
            )
        return rows


def _ws_set_col_widths(ws, max_col: int, width: float = 18.0) -> None:
    for c in range(1, max_col + 1):
        ws.column_dimensions[get_column_letter(c)].width = width


def _write_row(
    ws,
    row_idx: int,
    values: list[Any],
    *,
    header: bool,
    header_fills: list[str] | None,
    body_fill_large: PatternFill,
    body_fill_small: PatternFill,
    body_fill_plate: PatternFill,
    fonts_h: Font,
    fonts_b: Font,
    align_h: Alignment,
    align_b: Alignment,
    border: Border,
    col_sources: list[str],
) -> int:
    for col_idx, val in enumerate(values, start=1):
        cell = ws.cell(row=row_idx, column=col_idx, value=val)
        cell.border = border
        src = col_sources[col_idx - 1] if col_idx - 1 < len(col_sources) else "large"
        if header:
            cell.font = fonts_h
            cell.alignment = align_h
            if header_fills and col_idx - 1 < len(header_fills):
                cell.fill = PatternFill("solid", start_color=header_fills[col_idx - 1])
            else:
                cell.fill = (
                    PatternFill("solid", start_color="1E40AF")
                    if src == "large"
                    else PatternFill("solid", start_color="166534")
                )
        else:
            cell.font = fonts_b
            cell.alignment = align_b
            if src == "plate":
                cell.fill = body_fill_plate
            elif src == "small":
                cell.fill = body_fill_small
            else:
                cell.fill = body_fill_large
    return row_idx + 1


def run_check_plates_postgres_sync(
    dsn: str,
    user_id: int,
    is_admin: bool,
    sc_bytes: bytes,
    password: str,
    small_col: str,
    small_sheet: str,
    large_export_cols: list[str] | None,
    small_export_cols: list[str] | None,
) -> dict:
    ensure_check_pg_schema(dsn)
    meta = get_stored_large_meta_sync(dsn, user_id, is_admin)
    if not meta or meta.get("row_count", 0) == 0:
        return {
            "kind": "json",
            "status_code": 400,
            "body": {
                "detail": "لا توجد بيانات ملفات كبيرة مخزّنة. استورد ملفاً كبيراً أولاً.",
                "code": "NO_STORED_LARGE",
            },
        }

    union_headers: list[str] = list(meta.get("headers") or [])

    small_wb = None
    try:
        small_wb = openpyxl.load_workbook(
            io.BytesIO(sc_bytes), read_only=True, data_only=True
        )
        small_ws = (
            small_wb[small_sheet]
            if small_sheet and small_sheet in small_wb.sheetnames
            else find_best_sheet(small_wb)
        )
        srows = small_ws.iter_rows(values_only=True)
        header_s = next(srows, None)
        if header_s is None:
            raise ValueError("الملف الصغير فارغ")
        sh = [str(h).strip() if h is not None else "" for h in header_s]
        row2 = next(srows, None)
        row3 = next(srows, None)
        detected_small = (auto_detect_plate_col(sh) or auto_detect_plate_col_from_row3(sh, row3) or "")
        sc = small_col.strip() or detected_small
        if not sc or sc not in sh:
            return {
                "kind": "json",
                "status_code": 422,
                "body": {
                    "detail": f"لم يُعثر على عمود اللوحة في الملف الصغير. الأعمدة: {sh}",
                    "headers": sh,
                    "code": "COL_NOT_FOUND_SMALL",
                },
            }
        se_cols = _norm_small_export_cols(list(small_export_cols or []), sh)
        se_idx = [sh.index(c) for c in se_cols]
        sci = sh.index(sc)

        plate_title = "رقم اللوحة (المطابقة)"
        header_font = Font(name="Arial", bold=True, color="FFFFFF", size=11)
        body_font = Font(name="Arial", size=10, color="000000")
        body_fill_large = PatternFill("solid", start_color="DBEAFE")
        body_fill_small = PatternFill("solid", start_color="DCFCE7")
        body_fill_plate = PatternFill("solid", start_color="E0E7FF")
        align_header = Alignment(horizontal="center", vertical="center", wrap_text=True)
        align_cell = Alignment(horizontal="center", vertical="center", wrap_text=True)
        border = _thin_border()

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.title = "التطابقات"
        ws.sheet_view.rightToLeft = True
        row = 1

        preview_rows: list[list[str]] = []
        matched_rows_count = 0
        matched_plate_hits = 0
        unmatched_plates = 0
        section_started: dict[int, bool] = {}
        multi_file_notes: list[tuple[str, str, list[Any], dict[int, list[dict]]]] = []

        with check_pg_tx(dsn, user_id, is_admin) as conn:
            imports = _load_imports_ordered(conn, user_id)
            if not imports:
                return {
                    "kind": "json",
                    "status_code": 400,
                    "body": {"detail": "لا توجد استيرادات.", "code": "NO_IMPORTS"},
                }
            imp_by_id = {i["id"]: i for i in imports}

            for srow in itertools.chain((r for r in (row2, row3) if r is not None), srows):
                if all(v is None for v in srow):
                    continue
                rp = srow[sci] if sci < len(srow) else None
                norm = normalize_plate(rp)
                if not norm:
                    continue
                small_vals = [(srow[i] if i < len(srow) else None) for i in se_idx]
                groups = _fetch_matches_by_import(conn, user_id, norm)
                if not groups:
                    unmatched_plates += 1
                    continue
                matched_plate_hits += 1
                plate_disp = str(rp).strip() if rp is not None else norm
                if len(groups) > 1:
                    multi_file_notes.append((norm, plate_disp, small_vals, dict(groups)))

                for imp in imports:
                    iid = imp["id"]
                    lst = groups.get(iid) or []
                    if not lst:
                        continue
                    lh = imp["column_order"]
                    sec_le = _norm_large_export_cols(list(large_export_cols or []), lh)
                    if not section_started.get(iid):
                        color_hex = FILE_SECTION_HEADER_FILLS[
                            imports.index(imp) % len(FILE_SECTION_HEADER_FILLS)
                        ]
                        title_fill = PatternFill("solid", start_color=color_hex)
                        cell = ws.cell(row=row, column=1, value=f"📁 {imp['filename']}")
                        cell.fill = title_fill
                        cell.font = Font(name="Arial", bold=True, color="FFFFFF", size=11)
                        cell.border = border
                        row += 1
                        hdr_vals = (
                            [plate_title]
                            + [
                                _strip_small_word_from_header_title(c) for c in sec_le
                            ]
                            + [
                                _strip_small_word_from_header_title(c) for c in se_cols
                            ]
                        )
                        hdr_src = (
                            ["plate"]
                            + ["large"] * len(sec_le)
                            + ["small"] * len(se_cols)
                        )
                        hdr_colors = [color_hex] * len(hdr_vals)
                        row = _write_row(
                            ws,
                            row,
                            hdr_vals,
                            header=True,
                            header_fills=hdr_colors,
                            body_fill_large=body_fill_large,
                            body_fill_small=body_fill_small,
                            body_fill_plate=body_fill_plate,
                            fonts_h=header_font,
                            fonts_b=body_font,
                            align_h=align_header,
                            align_b=align_cell,
                            border=border,
                            col_sources=hdr_src,
                        )
                        section_started[iid] = True

                    for ld in lst:
                        large_vals = [_cell_display(ld.get(c)) for c in sec_le]
                        row_vals = [plate_disp] + large_vals + small_vals
                        row_src = ["plate"] + ["large"] * len(sec_le) + ["small"] * len(se_cols)
                        row = _write_row(
                            ws,
                            row,
                            row_vals,
                            header=False,
                            header_fills=None,
                            body_fill_large=body_fill_large,
                            body_fill_small=body_fill_small,
                            body_fill_plate=body_fill_plate,
                            fonts_h=header_font,
                            fonts_b=body_font,
                            align_h=align_header,
                            align_b=align_cell,
                            border=border,
                            col_sources=row_src,
                        )
                        matched_rows_count += 1
                        if len(preview_rows) < PREVIEW_MAX_ROWS:
                            preview_rows.append(
                                ["" if v is None else str(v).strip() for v in row_vals]
                            )

            if multi_file_notes:
                row += 1
                ws.cell(row=row, column=1, value="⚠ لوحات ظهرت في أكثر من ملف كبير")
                ws.cell(row=row, column=1).font = Font(bold=True, size=12, color="9A3412")
                row += 1
                sub_hdr = ["رقم اللوحة", "الملفات", "ملاحظة"]
                row = _write_row(
                    ws,
                    row,
                    sub_hdr,
                    header=True,
                    header_fills=["B91C1C", "B91C1C", "B91C1C"],
                    body_fill_large=body_fill_large,
                    body_fill_small=body_fill_small,
                    body_fill_plate=body_fill_plate,
                    fonts_h=header_font,
                    fonts_b=body_font,
                    align_h=align_header,
                    align_b=align_cell,
                    border=border,
                    col_sources=["plate", "large", "large"],
                )
                seen_norm: set[str] = set()
                for norm, plate_disp, smv, grp in multi_file_notes:
                    if norm in seen_norm:
                        continue
                    seen_norm.add(norm)
                    names = []
                    for iid in sorted(grp.keys()):
                        im = imp_by_id.get(iid)
                        if im:
                            names.append(im["filename"])
                    note = f"تطابق في {len(grp)} ملف(ات)"
                    row = _write_row(
                        ws,
                        row,
                        [plate_disp, "; ".join(names), note],
                        header=False,
                        header_fills=None,
                        body_fill_large=body_fill_large,
                        body_fill_small=body_fill_small,
                        body_fill_plate=body_fill_plate,
                        fonts_h=header_font,
                        fonts_b=body_font,
                        align_h=align_header,
                        align_b=align_cell,
                        border=border,
                        col_sources=["plate", "large", "large"],
                    )

        max_col = 1
        if ws.max_column:
            max_col = ws.max_column
        _ws_set_col_widths(ws, max_col, 20.0)

        if not matched_rows_count:
            return {
                "kind": "json",
                "status_code": 200,
                "body": {
                    "detail": "لا توجد تطابقات",
                    "matched": 0,
                    "unmatched": unmatched_plates,
                    "large_col_used": "(مخزّن — كل الملفات)",
                    "small_col_used": sc,
                },
            }

        display_headers = (
            [plate_title]
            + [_strip_small_word_from_header_title(c) for c in _norm_large_export_cols(list(large_export_cols or []), union_headers)]
            + [_strip_small_word_from_header_title(c) for c in se_cols]
        )
        col_sources_preview = (
            ["plate"]
            + ["large"] * len(_norm_large_export_cols(list(large_export_cols or []), union_headers))
            + ["small"] * len(se_cols)
        )

        content = workbook_to_bytes(wb)
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"التطابقات_{ts}.xlsx"
        return {
            "kind": "xlsx",
            "content": content,
            "filename": filename,
            "preview": {
                "headers": display_headers,
                "col_sources": col_sources_preview,
                "rows": preview_rows,
                "total_rows": matched_rows_count,
                "truncated": matched_rows_count > PREVIEW_MAX_ROWS,
                "stats": {
                    "matched_rows": matched_rows_count,
                    "matched_plate_hits": matched_plate_hits,
                    "unmatched_plates": unmatched_plates,
                    "multi_file_plates": len({m[0] for m in multi_file_notes}),
                },
            },
        }
    finally:
        if small_wb:
            try:
                small_wb.close()
            except Exception:
                pass


def collect_gps_vehicles_stored_sync(
    dsn: str,
    user_id: int,
    is_admin: bool,
    sc_bytes: bytes,
    small_col: str,
    small_sheet: str,
) -> dict[str, Any]:
    ensure_check_pg_schema(dsn)
    meta = get_stored_large_meta_sync(dsn, user_id, is_admin)
    if not meta or not meta.get("headers"):
        return {"detail": "لا توجد بيانات ملف كبير مخزّنة", "vehicles": []}

    union_h: list[str] = list(meta["headers"])
    gps_col = GPS_HEADER if GPS_HEADER in union_h else None
    if not gps_col:
        return {"detail": "لا يوجد عمود GPS في البيانات المخزّنة", "vehicles": []}

    date_col = next((h for h in union_h if "تاريخ" in h), None)
    type_col = next((h for h in union_h if "نوع" in h), None)
    notes_col = next((h for h in union_h if "ملاحظات" in h), None)

    small_wb = openpyxl.load_workbook(io.BytesIO(sc_bytes), read_only=True, data_only=True)
    try:
        small_ws = (
            small_wb[small_sheet]
            if small_sheet and small_sheet in small_wb.sheetnames
            else find_best_sheet(small_wb)
        )
        sd = list(small_ws.iter_rows(values_only=True))
        if not sd:
            return {"detail": "الملف الصغير فارغ", "vehicles": []}
        sh = [str(h).strip() if h is not None else "" for h in sd[0]]
        row3 = sd[2] if len(sd) > 2 else None
        sc = small_col.strip() or (auto_detect_plate_col(sh) or auto_detect_plate_col_from_row3(sh, row3) or "")
        if not sc or sc not in sh:
            return {
                "detail": f"لم يُعثر على عمود اللوحة في الملف الصغير. الأعمدة: {sh}",
                "vehicles": [],
            }
        sci = sh.index(sc)
        matched: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        with check_pg_tx(dsn, user_id, is_admin) as conn:
            imports = _load_imports_ordered(conn, user_id)
            imp_name = {i["id"]: i["filename"] for i in imports}
            for row in sd[1:]:
                if all(v is None for v in row):
                    continue
                rp = row[sci] if sci < len(row) else None
                norm = normalize_plate(rp)
                if not norm:
                    continue
                groups = _fetch_matches_by_import(conn, user_id, norm)
                for iid, rds in groups.items():
                    fn = imp_name.get(iid, "")
                    for ld in rds:
                        gps = str(ld.get(gps_col) or "").strip()
                        plate = str(rp or "").strip()
                        key = (plate, gps, fn)
                        if key in seen:
                            continue
                        seen.add(key)
                        matched.append(
                            {
                                "plate": plate,
                                "gps": gps,
                                "date": str(ld.get(date_col) or "").strip() if date_col else "",
                                "vehicle_type": str(ld.get(type_col) or "").strip() if type_col else "",
                                "notes": (str(ld.get(notes_col) or "").strip() if notes_col else "")
                                + (f" [{fn}]" if fn else ""),
                            }
                        )

        valid = []
        for item in matched:
            gps = item.get("gps", "")
            if not gps or gps == "None" or "," not in gps:
                continue
            parts = gps.split(",")
            try:
                float(parts[0].strip())
                float(parts[1].strip())
                valid.append(item)
            except Exception:
                continue

        if not valid:
            return {"detail": "لا توجد تطابقات", "vehicles": [], "total": 0, "skipped": 0}

        return {
            "vehicles": valid,
            "total": len(valid),
            "skipped": len(matched) - len(valid),
        }
    finally:
        try:
            small_wb.close()
        except Exception:
            pass
