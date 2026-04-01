import sqlite3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

_db_context: dict = {"path": None}


def set_db(path: str):
    _db_context["path"] = path


def _is_pg(path: str) -> bool:
    return bool(path and path.startswith("postgresql://"))

def _is_mysql(path: str) -> bool:
    return bool(path and path.startswith("mysql://"))


def run_query(sql: str):
    """
    Run a SELECT query against the active database (SQLite, PostgreSQL, or MySQL).
    Returns (DataFrame, None) on success, (None, error_message) on failure.
    """
    path = _db_context["path"]
    if not path:
        return None, "No database connected. Select a database first."
    if _is_pg(path):
        return _run_pg_query(sql, path)
    if _is_mysql(path):
        return _run_mysql_query(sql, path)
    return _run_sqlite_query(sql, path)


def _run_sqlite_query(sql: str, path: str):
    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame(), None
            return pd.DataFrame([dict(r) for r in rows]), None
        finally:
            conn.close()
    except Exception as e:
        return None, str(e)


def _run_mysql_query(sql: str, dsn: str):
    try:
        import pymysql
        import pymysql.cursors
        from urllib.parse import urlparse
        u = urlparse(dsn)
        conn = pymysql.connect(
            host=u.hostname, port=u.port or 3306,
            database=u.path.lstrip("/"),
            user=u.username, password=u.password,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            if not rows:
                return pd.DataFrame(), None
            return pd.DataFrame(rows), None
        finally:
            conn.close()
    except Exception as e:
        return None, str(e)


def _run_pg_query(sql: str, dsn: str):
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(dsn)
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(sql)
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame(), None
            return pd.DataFrame([dict(r) for r in rows]), None
        finally:
            conn.close()
    except Exception as e:
        return None, str(e)
