"""
schema_loader.py — Introspect a database and return a formatted schema string for the LLM prompt.

Public API:
    load_schema(db_path, db_type, sample_rows) -> str
    filter_schema_by_tables(schema, selected)  -> str
"""

import re
import sqlite3
from typing import Optional


# ── Column helpers ─────────────────────────────────────────────────────────────

def _query_text_enum_values(cursor, table: str, col: str, max_distinct: int = 15) -> str:
    """
    Query distinct non-null TEXT values for a column directly from the DB.
    Returns a quoted comma-separated string if ≤ max_distinct values exist, else "".
    """
    try:
        rows = cursor.execute(
            f"SELECT DISTINCT `{col}` FROM `{table}` WHERE `{col}` IS NOT NULL LIMIT {max_distinct + 1}"
        ).fetchall()
        if not rows or len(rows) > max_distinct:
            return ""
        return ", ".join(f"'{r[0]}'" for r in sorted(rows, key=lambda r: str(r[0])))
    except Exception:
        return ""


def _build_column_lines(
    name: str,
    col_type: str,
    is_pk: bool,
    pragma_fk: Optional[tuple],
    cursor,
    table: str,
) -> list[str]:
    """
    Build 1-2 lines for a single column in a CREATE TABLE block.
    Returns [col_line] or [col_line, "  -- Actual values: ..."]
    """
    col_line = f'  "{name}" {col_type or "TEXT"}'
    if is_pk:
        col_line += " PRIMARY KEY"
    if pragma_fk:
        col_line += f"  -- FK → {pragma_fk[0]}.{pragma_fk[1]}"
        return [col_line]

    # For TEXT columns with no FK, show actual enum values if low-cardinality
    if (col_type or "").upper() in ("TEXT", "VARCHAR", "CHAR", ""):
        db_vals = _query_text_enum_values(cursor, table, name)
        if db_vals:
            return [col_line, f'        -- Actual values: {db_vals}']

    return [col_line]


def _build_table_block(cursor, table: str, sample_rows: int) -> str:
    """
    Build the full CREATE TABLE block for one table, including sample rows.
    """
    cursor.execute(f"PRAGMA table_info(`{table}`)")
    columns = cursor.fetchall()

    cursor.execute(f"PRAGMA foreign_key_list(`{table}`)")
    pragma_fk_map = {fk[3]: (fk[2], fk[4]) for fk in cursor.fetchall()}

    col_defs = []
    for cid, name, col_type, notnull, dflt, pk in columns:
        pragma_fk = pragma_fk_map.get(name)
        col_defs.extend(_build_column_lines(name, col_type, bool(pk), pragma_fk, cursor, table))

    lines = [
        f"CREATE TABLE {table} (",
        ",\n".join(col_defs),
        ");",
    ]

    if sample_rows > 0:
        try:
            cursor.execute(f"SELECT * FROM `{table}` LIMIT {sample_rows}")
            rows = cursor.fetchall()
            col_names = [c[1] for c in columns]
            if rows:
                lines.append("/* Sample rows:")
                lines.append(" | ".join(col_names))
                for row in rows:
                    lines.append(" | ".join("NULL" if v is None else str(v) for v in row))
                lines.append("*/")
        except Exception:
            pass

    return "\n".join(lines)


# ── Backend loaders ────────────────────────────────────────────────────────────

def _load_sqlite_schema(db_path: str, sample_rows: int = 3) -> str:
    """
    Input:  path to a SQLite file
    Output: formatted schema string (header + FK relationships + per-table blocks)
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]

        sections = ["Database: SQLite"]

        # TABLE RELATIONSHIPS
        join_lines = []
        for table in tables:
            cursor.execute(f"PRAGMA foreign_key_list(`{table}`)")
            for fk in cursor.fetchall():
                join_lines.append(
                    f"   `{table}` JOIN `{fk[2]}` ON `{table}`.`{fk[3]}` = `{fk[2]}`.`{fk[4]}`"
                )
        if join_lines:
            sections.append("/* TABLE RELATIONSHIPS (use these for JOINs):\n" + "\n".join(join_lines) + "\n*/")

        for table in tables:
            sections.append(_build_table_block(cursor, table, sample_rows))

        return "\n\n".join(sections)
    finally:
        conn.close()


# ── PostgreSQL backend ─────────────────────────────────────────────────────────

def _load_postgres_schema(dsn: str, sample_rows: int = 3) -> str:
    import psycopg2
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]

        sections = ["Database: PostgreSQL"]

        # Foreign key relationships
        cur.execute("""
            SELECT tc.table_name, kcu.column_name,
                   ccu.table_name AS ref_table, ccu.column_name AS ref_col
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                 ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
        """)
        fks = cur.fetchall()
        if fks:
            join_lines = [
                f'   "{r[0]}" JOIN "{r[2]}" ON "{r[0]}"."{r[1]}" = "{r[2]}"."{r[3]}"'
                for r in fks
            ]
            sections.append("/* TABLE RELATIONSHIPS (use these for JOINs):\n" + "\n".join(join_lines) + "\n*/")

        # Per-table blocks
        for table in tables:
            cur.execute("""
                SELECT c.column_name, c.data_type, c.is_nullable,
                       CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN true ELSE false END AS is_pk
                FROM information_schema.columns c
                LEFT JOIN information_schema.key_column_usage kcu
                    ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
                    AND c.table_schema = kcu.table_schema
                LEFT JOIN information_schema.table_constraints tc
                    ON kcu.constraint_name = tc.constraint_name
                    AND tc.constraint_type = 'PRIMARY KEY'
                WHERE c.table_schema = 'public' AND c.table_name = %s
                ORDER BY c.ordinal_position
            """, (table,))
            cols = cur.fetchall()

            fk_set = {r[1] for r in fks if r[0] == table}

            col_defs = []
            for col_name, data_type, nullable, is_pk in cols:
                line = f'  "{col_name}" {data_type.upper()}'
                if is_pk:
                    line += " PRIMARY KEY"
                if col_name in fk_set:
                    ref = next((f"{r[2]}.{r[3]}" for r in fks if r[0] == table and r[1] == col_name), "")
                    line += f"  -- FK → {ref}"
                col_defs.append(line)

            block_lines = [f'CREATE TABLE {table} (', ",\n".join(col_defs), ");"]

            if sample_rows > 0:
                try:
                    cur.execute(f'SELECT * FROM "{table}" LIMIT {sample_rows}')
                    rows = cur.fetchall()
                    if rows:
                        col_names = [c[0] for c in cols]
                        block_lines.append("/* Sample rows:")
                        block_lines.append(" | ".join(col_names))
                        for row in rows:
                            block_lines.append(" | ".join("NULL" if v is None else str(v) for v in row))
                        block_lines.append("*/")
                except Exception:
                    pass

            sections.append("\n".join(block_lines))

        return "\n\n".join(sections)
    finally:
        conn.close()


# ── MySQL backend ──────────────────────────────────────────────────────────────

def _load_mysql_schema(dsn: str, sample_rows: int = 3) -> str:
    import pymysql
    from urllib.parse import urlparse
    u = urlparse(dsn)
    db_name = u.path.lstrip("/")
    conn = pymysql.connect(
        host=u.hostname, port=u.port or 3306,
        database=db_name, user=u.username, password=u.password,
    )
    try:
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [r[0] for r in cur.fetchall()]

        sections = [f"Database: MySQL ({db_name})"]

        # Foreign key relationships
        cur.execute("""
            SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL
        """, (db_name,))
        fks = cur.fetchall()
        if fks:
            join_lines = [
                f'   `{r[0]}` JOIN `{r[2]}` ON `{r[0]}`.`{r[1]}` = `{r[2]}`.`{r[3]}`'
                for r in fks
            ]
            sections.append("/* TABLE RELATIONSHIPS (use these for JOINs):\n" + "\n".join(join_lines) + "\n*/")

        fk_set   = {(r[0], r[1]) for r in fks}
        for table in tables:
            cur.execute(f"DESCRIBE `{table}`")
            cols = cur.fetchall()  # Field, Type, Null, Key, Default, Extra
            col_defs = []
            for field, col_type, nullable, key, *_ in cols:
                line = f'  `{field}` {col_type.upper()}'
                if key == "PRI":
                    line += " PRIMARY KEY"
                if (table, field) in fk_set:
                    ref = next((f"{r[2]}.{r[3]}" for r in fks if r[0] == table and r[1] == field), "")
                    line += f"  -- FK → {ref}"
                col_defs.append(line)

            block = [f"CREATE TABLE {table} (", ",\n".join(col_defs), ");"]
            if sample_rows > 0:
                try:
                    cur.execute(f"SELECT * FROM `{table}` LIMIT {sample_rows}")
                    rows = cur.fetchall()
                    if rows:
                        col_names = [c[0] for c in cols]
                        block.append("/* Sample rows:")
                        block.append(" | ".join(col_names))
                        for row in rows:
                            block.append(" | ".join("NULL" if v is None else str(v) for v in row))
                        block.append("*/")
                except Exception:
                    pass
            sections.append("\n".join(block))

        return "\n\n".join(sections)
    finally:
        conn.close()


# ── Public API ─────────────────────────────────────────────────────────────────

def load_schema(db_path: str, sample_rows: int = 3) -> str:
    """
    Introspect a SQLite, PostgreSQL, or MySQL database and return a formatted schema string.
    """
    if db_path and db_path.startswith("postgresql://"):
        return _load_postgres_schema(db_path, sample_rows)
    if db_path and db_path.startswith("mysql://"):
        return _load_mysql_schema(db_path, sample_rows)
    return _load_sqlite_schema(db_path, sample_rows)



def filter_schema_by_tables(schema: str, selected_tables: list[str]) -> str:
    """
    Return a copy of the schema containing only the selected tables.
    Always preserves the Database header and TABLE RELATIONSHIPS block.
    """
    selected = {t.strip().strip("`") for t in selected_tables}

    raw_sections = re.split(r"\n{2,}", schema)
    kept = []
    for section in raw_sections:
        stripped = section.strip()
        if not stripped:
            continue
        if stripped.startswith("Database:") or stripped.startswith("/*"):
            kept.append(section)
            continue
        m = re.match(r"CREATE TABLE [`\"]?(\w+)[`\"]?\s*\(", stripped)
        if m and m.group(1) in selected:
            kept.append(section)

    return "\n\n".join(kept)


def build_schema_graph(db_path: str) -> str:
    """
    Generate a Graphviz DOT string visualising the database schema.
    - PK columns  : gold  (#F5A623) label
    - FK columns  : blue  (#4A90D9) label
    - Regular cols: dark  (#333333) or muted (#999999) if nullable
    - Arrows point from FK cell → referenced table, with curved/ortho routing.
    """

    # Colour palette
    C_HEADER_BG  = "#2C2C2C"
    C_HEADER_FG  = "#FFFFFF"
    C_PK_BG      = "#FFF8E7"
    C_PK_FG      = "#D4860A"
    C_FK_BG      = "#EBF4FF"
    C_FK_FG      = "#2F6DB5"
    C_ROW_BG     = "#FFFFFF"
    C_COL_FG     = "#333333"
    C_NULL_FG    = "#999999"
    C_TYPE_FG    = "#AAAAAA"
    C_EDGE       = "#4A90D9"

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [r[0] for r in cursor.fetchall()]

        lines = [
            "digraph schema {",
            '  graph [rankdir=LR fontname="Helvetica" bgcolor="#F8F9FA"'
            '         splines=curved nodesep=0.8 ranksep=1.5];',
            '  node  [shape=plaintext fontname="Helvetica" fontsize=11];',
            f'  edge  [color="{C_EDGE}" arrowhead=vee arrowsize=0.8'
            '         fontname="Helvetica" fontsize=9 fontcolor="#4A90D9"'
            '         penwidth=1.5];',
            "",
        ]

        edges = []

        for table in tables:
            cursor.execute(f"PRAGMA table_info(`{table}`)")
            columns = cursor.fetchall()

            cursor.execute(f"PRAGMA foreign_key_list(`{table}`)")
            fk_map = {fk[3]: (fk[2], fk[4]) for fk in cursor.fetchall()}

            # ── Table header ──
            safe = table.replace('"', '')
            rows = [
                f'<TR>'
                f'<TD BGCOLOR="{C_HEADER_BG}" ALIGN="LEFT" BORDER="0" COLSPAN="2">'
                f'<FONT COLOR="{C_HEADER_FG}"><B> {safe} </B></FONT>'
                f'</TD></TR>'
            ]

            for _, col_name, col_type, notnull, _, pk in columns:
                is_pk = bool(pk)
                is_fk = col_name in fk_map
                type_str = (col_type or "TEXT").lower()[:12]

                # Port name for edge anchoring (FK columns)
                port_attr = f' PORT="fk_{col_name}"' if is_fk else ""

                if is_pk:
                    bg, fg, badge = C_PK_BG, C_PK_FG, "PK"
                elif is_fk:
                    bg, fg, badge = C_FK_BG, C_FK_FG, "FK"
                else:
                    bg  = C_ROW_BG
                    fg  = C_COL_FG if notnull else C_NULL_FG
                    badge = ""

                badge_cell = (
                    f'<TD BGCOLOR="{bg}" ALIGN="CENTER" BORDER="0" WIDTH="28">'
                    f'<FONT COLOR="{fg}" POINT-SIZE="8"><B>{badge}</B></FONT></TD>'
                    if badge else
                    f'<TD BGCOLOR="{bg}" BORDER="0" WIDTH="28"></TD>'
                )

                rows.append(
                    f'<TR>'
                    f'{badge_cell}'
                    f'<TD BGCOLOR="{bg}" ALIGN="LEFT" BORDER="0"{port_attr}>'
                    f'<FONT COLOR="{fg}"> {col_name} </FONT></TD>'
                    f'<TD BGCOLOR="{bg}" ALIGN="RIGHT" BORDER="0">'
                    f'<FONT COLOR="{C_TYPE_FG}">{type_str} </FONT></TD>'
                    f'</TR>'
                )

                if is_fk:
                    ref_table, ref_col = fk_map[col_name]
                    edges.append(
                        f'  "{table}":fk_{col_name} -> "{ref_table}" '
                        f'[label=" {col_name}"];'
                    )

            label_body = "\n    ".join(rows)
            lines.append(
                f'  "{table}" [label=<<TABLE BORDER="1" CELLBORDER="0" '
                f'CELLSPACING="0" CELLPADDING="3" BGCOLOR="{C_ROW_BG}"'
                f' STYLE="rounded">\n'
                f'    {label_body}\n'
                f'  </TABLE>>];'
            )

        lines.append("")
        lines.extend(edges)
        lines.append("}")
        return "\n".join(lines)

    finally:
        conn.close()


if __name__ == '__main__':
    db_path = './bird_data/dev_databases/california_schools/california_schools.sqlite'
    schema = load_schema(db_path)
    print(schema[:2000])
