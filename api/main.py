import sys
import json
import sqlite3
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

from agent import graph, initial_state
from agent import (
    generate_sql as _gen_sql,
    schema_filter as _schema_filter,
    verify_columns as _verify_cols,
    execute_sql as _exec_sql,
    interpret as _interpret,
    self_correct as _self_correct,
)
from schema_loader import load_schema, build_schema_graph
from sample_db import create_sample_db, SAMPLE_DB_PATH
from utils import format_sql

app = FastAPI(title="Text2SQL Agent API")

# Generate sample DB on startup if it doesn't exist
create_sample_db()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Server-side cache so we don't reload schema on every request
_schema_cache: dict[str, str] = {}
_dot_cache:    dict[str, str] = {}

CONNECTIONS_FILE = Path(__file__).parent.parent / "inquery_connections.json"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _scan_databases() -> list[dict]:
    roots = [
        Path("./minidev/minidev/MINIDEV/dev_databases"),
        Path("./bird_data/dev_databases"),
    ]
    dbs = []
    for root in roots:
        if root.exists():
            for entry in sorted(root.iterdir()):
                sqlite = entry / f"{entry.name}.sqlite"
                if sqlite.exists():
                    dbs.append({"name": entry.name, "path": str(sqlite.resolve())})
    return dbs


def _read_connections() -> list[dict]:
    if not CONNECTIONS_FILE.exists():
        return []
    try:
        return json.loads(CONNECTIONS_FILE.read_text())
    except Exception:
        return []


def _write_connections(conns: list[dict]) -> None:
    CONNECTIONS_FILE.write_text(json.dumps(conns, indent=2))


# ── Existing endpoints ────────────────────────────────────────────────────────

@app.get("/api/databases")
def get_databases():
    return {"databases": _scan_databases()}


@app.get("/api/sample-db")
def get_sample_db():
    return {"path": str(SAMPLE_DB_PATH), "name": "Sample E-Commerce"}


class ConnectRequest(BaseModel):
    db_path: str

@app.post("/api/connect")
def connect(req: ConnectRequest):
    try:
        if req.db_path not in _schema_cache:
            _schema_cache[req.db_path] = load_schema(req.db_path)
            # Only build dot graph for SQLite
            if not req.db_path.startswith("postgresql://") and not req.db_path.startswith("mysql://"):
                _dot_cache[req.db_path] = build_schema_graph(req.db_path)
        # Derive a display name
        if req.db_path.startswith("postgresql://"):
            from urllib.parse import urlparse
            u = urlparse(req.db_path)
            name = u.path.lstrip("/") or u.hostname or "postgres"
        else:
            name = Path(req.db_path).stem
        return {
            "name":       name,
            "schema_dot": _dot_cache.get(req.db_path),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class AskRequest(BaseModel):
    question: str
    db_path:  str

@app.post("/api/ask")
def ask(req: AskRequest):
    try:
        schema = _schema_cache.get(req.db_path) or load_schema(req.db_path)
        result = graph.invoke(initial_state(question=req.question, schema=schema, db_path=req.db_path))
        return {
            "sql":          result.get("sql"),
            "df_json":      result.get("df_json"),
            "chart_config": result.get("chart_config"),
            "answer":       result.get("answer"),
            "error":        result.get("error"),
        }
    except Exception as e:
        return {"error": str(e)}


class ExecuteRequest(BaseModel):
    question: str
    sql:      str
    db_path:  str

@app.post("/api/execute")
def execute(req: ExecuteRequest):
    try:
        schema    = _schema_cache.get(req.db_path, "")
        new_state = initial_state(question=req.question, schema=schema, db_path=req.db_path)
        new_state["is_data_query"] = True
        new_state["sql"]           = req.sql
        result = graph.invoke(new_state)
        return {
            "sql":          result.get("sql"),
            "df_json":      result.get("df_json"),
            "chart_config": result.get("chart_config"),
            "answer":       result.get("answer"),
            "error":        result.get("error"),
        }
    except Exception as e:
        return {"error": str(e)}


# ── New endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/connections")
def get_connections():
    return {"connections": _read_connections()}


class ConnectionBody(BaseModel):
    name: str
    type: str
    path: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

@app.post("/api/connections")
def add_connection(body: ConnectionBody):
    conns = _read_connections()
    conns.append(body.dict())
    _write_connections(conns)
    return {"ok": True}


@app.delete("/api/connections/{name}")
def delete_connection(name: str):
    conns = _read_connections()
    conns = [c for c in conns if c.get("name") != name]
    _write_connections(conns)
    return {"ok": True}


class TestConnectionBody(BaseModel):
    name: Optional[str] = None
    type: str
    path: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

@app.post("/api/test-connection")
def test_connection(body: TestConnectionBody):
    db_type = (body.type or "").lower()
    if db_type == "sqlite":
        if not body.path:
            return {"ok": False, "message": "No file path provided"}
        p = Path(body.path)
        if not p.exists():
            return {"ok": False, "message": f"File not found: {body.path}"}
        try:
            con = sqlite3.connect(str(p))
            con.execute("SELECT 1")
            con.close()
            return {"ok": True, "message": "Connection successful"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
    elif db_type in ("postgresql", "postgres"):
        try:
            import psycopg2
            con = psycopg2.connect(
                host=body.host or "localhost",
                port=body.port or 5432,
                dbname=body.database or "",
                user=body.username or "",
                password=body.password or "",
                connect_timeout=5,
            )
            con.close()
            return {"ok": True, "message": "Connection successful"}
        except ImportError:
            return {"ok": False, "message": "psycopg2 not installed. Run: pip install psycopg2-binary"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
    elif db_type == "mysql":
        try:
            import pymysql
            con = pymysql.connect(
                host=body.host or "localhost",
                port=body.port or 3306,
                database=body.database or "",
                user=body.username or "",
                password=body.password or "",
                connect_timeout=5,
            )
            con.close()
            return {"ok": True, "message": "Connection successful"}
        except ImportError:
            return {"ok": False, "message": "PyMySQL not installed. Run: pip install pymysql"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
    else:
        return {"ok": False, "message": f"Unsupported connection type: {body.type}"}


class GenerateSQLBody(BaseModel):
    question: str
    db_path: str

@app.post("/api/generate-sql")
def generate_sql_endpoint(body: GenerateSQLBody):
    try:
        if body.db_path not in _schema_cache:
            _schema_cache[body.db_path] = load_schema(body.db_path)
        schema = _schema_cache[body.db_path]

        state = initial_state(question=body.question, schema=schema, db_path=body.db_path)
        state["is_data_query"] = True
        state.update(_schema_filter(state))
        state.update(_gen_sql(state))
        if not state.get("error"):
            state.update(_verify_cols(state))

        if state.get("error"):
            return {"error": state["error"]}
        sql = state.get("sql", "")
        if not sql:
            return {"error": "No SQL generated"}

        return {"sql": format_sql(sql)}
    except Exception as e:
        return {"error": str(e)}


class RunQueryBody(BaseModel):
    question: str
    sql: str
    db_path: str

@app.post("/api/run-query")
def run_query_endpoint(body: RunQueryBody):
    try:
        schema = _schema_cache.get(body.db_path, "")

        state = initial_state(question=body.question, schema=schema, db_path=body.db_path)
        state["sql"] = body.sql
        state["is_data_query"] = True

        t0 = time.time()
        state.update(_exec_sql(state))

        retries = 0
        while state.get("exec_error") and retries < 2:
            state.update(_self_correct(state))
            state.update(_exec_sql(state))
            retries += 1

        exec_time = round(time.time() - t0, 3)

        if state.get("exec_error"):
            return {
                "sql": state.get("sql", body.sql),
                "df_json": None,
                "answer": None,
                "error": state.get("exec_error"),
                "exec_time": exec_time,
            }

        state.update(_interpret(state))

        return {
            "sql": format_sql(state.get("sql", body.sql)),
            "df_json": state.get("df_json"),
            "answer": state.get("answer"),
            "error": state.get("error"),
            "exec_time": exec_time,
        }
    except Exception as e:
        return {
            "sql": body.sql,
            "df_json": None,
            "answer": None,
            "error": str(e),
            "exec_time": 0,
        }


@app.get("/api/schema-info")
def get_schema_info(db_path: str):
    try:
        if db_path.startswith("postgresql://"):
            return _schema_info_postgres(db_path)
        if db_path.startswith("mysql://"):
            return _schema_info_mysql(db_path)
        return _schema_info_sqlite(db_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def _schema_info_sqlite(db_path: str):
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    table_names = [r[0] for r in cur.fetchall()]
    result = []
    for tname in table_names:
        cur.execute(f"PRAGMA table_info(`{tname}`)")
        columns = [{"name": r[1], "type": r[2] or "TEXT", "notnull": bool(r[3]), "pk": bool(r[5]), "fk": False}
                   for r in cur.fetchall()]
        cur.execute(f"PRAGMA foreign_key_list(`{tname}`)")
        fk_cols = {r[3] for r in cur.fetchall()}
        for c in columns:
            c["fk"] = c["name"] in fk_cols
        try:
            cur.execute(f"SELECT COUNT(*) FROM `{tname}`")
            row_count = cur.fetchone()[0]
        except Exception:
            row_count = None
        result.append({"name": tname, "columns": columns, "row_count": row_count})
    conn.close()
    return {"tables": result}


def _schema_info_mysql(dsn: str):
    import pymysql
    from urllib.parse import urlparse
    u = urlparse(dsn)
    db_name = u.path.lstrip("/")
    conn = pymysql.connect(host=u.hostname, port=u.port or 3306,
                           database=db_name, user=u.username, password=u.password)
    cur = conn.cursor()
    cur.execute("SHOW TABLES")
    table_names = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL
    """, (db_name,))
    fk_pairs = {(r[0], r[1]) for r in cur.fetchall()}

    cur.execute("""
        SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND CONSTRAINT_NAME = 'PRIMARY'
    """, (db_name,))
    pk_pairs = {(r[0], r[1]) for r in cur.fetchall()}

    result = []
    for tname in table_names:
        cur.execute(f"DESCRIBE `{tname}`")
        columns = [{"name": r[0], "type": r[1].upper(), "notnull": r[2] == "NO",
                    "pk": (tname, r[0]) in pk_pairs, "fk": (tname, r[0]) in fk_pairs}
                   for r in cur.fetchall()]
        try:
            cur.execute(f"SELECT COUNT(*) FROM `{tname}`")
            row_count = cur.fetchone()[0]
        except Exception:
            row_count = None
        result.append({"name": tname, "columns": columns, "row_count": row_count})
    conn.close()
    return {"tables": result}


def _schema_info_postgres(dsn: str):
    import psycopg2
    conn = psycopg2.connect(dsn)
    cur  = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    table_names = [r[0] for r in cur.fetchall()]

    # Build FK set
    cur.execute("""
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
             ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
    """)
    fk_pairs = {(r[0], r[1]) for r in cur.fetchall()}

    # Build PK set
    cur.execute("""
        SELECT tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
             ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
    """)
    pk_pairs = {(r[0], r[1]) for r in cur.fetchall()}

    result = []
    for tname in table_names:
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (tname,))
        columns = [
            {
                "name": r[0], "type": r[1].upper(), "notnull": r[2] == "NO",
                "pk": (tname, r[0]) in pk_pairs,
                "fk": (tname, r[0]) in fk_pairs,
            }
            for r in cur.fetchall()
        ]
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{tname}"')
            row_count = cur.fetchone()[0]
        except Exception:
            row_count = None
        result.append({"name": tname, "columns": columns, "row_count": row_count})
    conn.close()
    return {"tables": result}


@app.get("/")
def serve_frontend():
    html_path = Path(__file__).parent.parent / "inquery.html"
    return FileResponse(str(html_path))
