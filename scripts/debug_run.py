"""
debug_run.py — Step-by-step trace of a single question through the agent.

Usage:
    python debug_run.py          # uses ITEM variable below
"""

import json
import random
import sqlite3
from io import StringIO

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

from db import set_db
from schema_loader import load_schema
from agent import graph, initial_state

# ── Config — change this to test different questions ──────────────────────────
ITEM = 24

MINIDEV_JSON = "./minidev/minidev/MINIDEV/mini_dev_sqlite.json"
MINIDEV_DB_ROOT = "./minidev/minidev/MINIDEV/dev_databases"
SEED = 7


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    with open(MINIDEV_JSON) as f:
        items = json.load(f)
    random.seed(SEED)
    random.shuffle(items)
    item = items[ITEM - 1]

    question = item["question"]
    evidence = item.get("evidence", "")
    db_id    = item["db_id"]
    gold_sql = item["SQL"]
    db_path  = f"{MINIDEV_DB_ROOT}/{db_id}/{db_id}.sqlite"

    print("=" * 70)
    print(f"Item:     {ITEM}")
    print(f"DB:       {db_id}  ({db_path})")
    print(f"Question: {question}")
    if evidence:
        print(f"Evidence: {evidence}")
    print(f"Gold SQL: {gold_sql}")
    print("=" * 70)

    # Run gold SQL
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(gold_sql).fetchall()
        conn.close()
        print(f"\n[gold result] {rows[:5]}")
    except Exception as e:
        print(f"\n[gold SQL error] {e}")

    set_db(db_path)
    schema = load_schema(db_path)
    print(f"\n[schema] loaded ({len(schema)} chars, {schema.count('CREATE TABLE')} tables)\n")

    state = initial_state(question, schema=schema, db_id=db_id, evidence=evidence or None)

    for step in graph.stream(state):
        for node_name, updates in step.items():
            print(f"\n{'─'*60}")
            print(f"  NODE: {node_name}")
            print(f"{'─'*60}")
            for key, val in updates.items():
                if val is None:
                    continue
                if key in ("schema", "filtered_schema"):
                    print(f"  {key}: ({len(val)} chars)" if val else f"  {key}: None")
                elif key == "df_json":
                    df = pd.read_json(StringIO(val))
                    print(f"  df_json: {len(df)} rows × {len(df.columns)} cols")
                    print(df.head(3).to_string(index=False))
                elif key == "chart_config":
                    print(f"  chart_config: {val}")
                elif isinstance(val, str) and len(val) > 300:
                    print(f"  {key}: {val[:300]}...")
                else:
                    print(f"  {key}: {val}")

    print("\n" + "=" * 70)
    print("DONE")


if __name__ == "__main__":
    main()
