import sqlite3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection pools, keyed by DSN
_pg_pools: dict = {}


def _get_pg_pool(dsn: str):
    import psycopg2.pool
    if dsn not in _pg_pools:
        _pg_pools[dsn] = psycopg2.pool.ThreadedConnectionPool(1, 5, dsn)
    return _pg_pools[dsn]


def _is_pg(path: str) -> bool:
    return bool(path and path.startswith("postgresql://"))

def _is_mysql(path: str) -> bool:
    return bool(path and path.startswith("mysql://"))


def run_query(sql: str, db_path: str):
    """
    Run a SELECT query against the given database (SQLite, PostgreSQL, or MySQL).
    Returns (DataFrame, None) on success, (None, error_message) on failure.
    """
    if not db_path:
        return None, "No database connected. Select a database first."
    if _is_pg(db_path):
        return _run_pg_query(sql, db_path)
    if _is_mysql(db_path):
        return _run_mysql_query(sql, db_path)
    return _run_sqlite_query(sql, db_path)


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
    import psycopg2.extras
    pool = _get_pg_pool(dsn)
    conn = pool.getconn()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.rollback()  # reset transaction state before returning to pool
        if not rows:
            return pd.DataFrame(), None
        return pd.DataFrame([dict(r) for r in rows]), None
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return None, str(e)
    finally:
        pool.putconn(conn)
