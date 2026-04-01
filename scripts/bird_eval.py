"""
bird_eval.py — Run the Text2SQL agent on the BIRD dev set and produce predict_dev.json.

Step 1 (this script): generate predictions
    python scripts/bird_eval.py [--limit N] [--random SEED] [--output predict_dev.json]

Step 2 (official eval): score predictions
    python scripts/evaluation.py \
        --predicted_sql_path ./             \
        --ground_truth_path  bird_data/     \
        --data_mode          dev            \
        --db_root_path       bird_data/dev_databases/ \
        --diff_json_path     bird_data/dev.json       \
        --num_cpus           4

Output format (official BIRD):
    {"0": "SELECT ...\t----- bird -----\tdb_id", "1": ...}
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(override=True)

import config
from agent import graph, initial_state
from db import set_db
from schema_loader import load_schema


# ── Data loading ───────────────────────────────────────────────────────────────

def load_bird_dev(data_path: str, json_file: str = "dev.json") -> list:
    dev_json = Path(data_path) / json_file
    if not dev_json.exists():
        raise FileNotFoundError(f"{json_file} not found at {dev_json}")
    with open(dev_json, encoding="utf-8") as f:
        return json.load(f)


def resolve_db_path(data_path: str, db_id: str) -> str:
    path = Path(data_path) / "dev_databases" / db_id / f"{db_id}.sqlite"
    if not path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {path}")
    return str(path)


# ── Prediction generation ──────────────────────────────────────────────────────

def generate_predictions(
    data_path: str,
    json_file: str = "dev.json",
    limit: int = None,
    offset: int = 0,
    db_id_filter: str = None,
    output: str = "predict_dev.json",
    random_seed: int = None,
):
    import random as _random

    items = load_bird_dev(data_path, json_file)

    if db_id_filter:
        items = [it for it in items if it.get("db_id") == db_id_filter]
    if random_seed is not None:
        _random.seed(random_seed)
        _random.shuffle(items)
    if offset:
        items = items[offset:]
    if limit:
        items = items[:limit]

    print(f"Generating predictions for {len(items)} items...")

    schema_cache = {}
    predictions  = {}   # {str(idx): "SQL\t----- bird -----\tdb_id"}
    gold_sqls    = []   # gold SQL lines for the subset: "SQL\tdb_id"

    for i, item in enumerate(items):
        db_id    = item["db_id"]
        question = item["question"]
        evidence = item.get("evidence", "")

        try:
            db_path = resolve_db_path(data_path, db_id)
        except FileNotFoundError as e:
            print(f"  [{i}] SKIP: {e}")
            predictions[str(i)] = f"SELECT 1\t----- bird -----\t{db_id}"
            continue

        set_db(db_path)

        if db_id not in schema_cache:
            schema_cache[db_id] = load_schema(db_path, sample_rows=5)
        schema = schema_cache[db_id]

        t0 = time.time()
        try:
            result = graph.invoke(initial_state(
                question,
                schema=schema,
                db_id=db_id,
                evidence=evidence,
            ))
            predicted_sql = result.get("sql") or "SELECT 1"
        except Exception as e:
            predicted_sql = "SELECT 1"
            print(f"  [{i}] ERROR: {e}")

        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(items)}] {db_id} | {elapsed:.1f}s | {predicted_sql[:80]}")

        predictions[str(i)] = f"{predicted_sql}\t----- bird -----\t{db_id}"
        gold_sqls.append(f"{item['SQL']}\t{db_id}")

    # Save predict_dev.json (official format)
    output_path = Path(output)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(predictions, f, ensure_ascii=False, indent=2)

    # Save matching gold file for this subset
    gold_path = output_path.with_name("dev_gold.sql")
    with open(gold_path, "w", encoding="utf-8") as f:
        f.write("\n".join(gold_sqls) + "\n")

    # Save matching dev.json subset (for --diff_json_path)
    diff_path = output_path.with_name("dev_subset.json")
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(predictions)} predictions → {output_path}")
    print(f"Saved matching gold SQL  → {gold_path}")
    print()
    print("Now run official evaluation:")
    print(f"  python scripts/evaluation.py \\")
    print(f"    --predicted_sql_path {output_path.parent}/ \\")
    print(f"    --ground_truth_path  {output_path.parent}/ \\")
    print(f"    --data_mode          dev \\")
    print(f"    --db_root_path       bird_data/dev_databases/ \\")
    print(f"    --diff_json_path     {diff_path} \\")
    print(f"    --num_cpus           4")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate BIRD predictions with Text2SQL agent")
    parser.add_argument("--data_path", default=config.BIRD_DATA_PATH,   help="Path to BIRD dev directory")
    parser.add_argument("--json_file", default="dev.json",              help="Question JSON filename (e.g. mini_dev_sqlite.json)")
    parser.add_argument("--limit",     type=int,  default=None,         help="Max number of items")
    parser.add_argument("--offset",    type=int,  default=0,            help="Skip first N items")
    parser.add_argument("--db_id",     default=None,                    help="Filter to a specific db_id")
    parser.add_argument("--output",    default="predict_dev.json",      help="Output JSON file path")
    parser.add_argument("--random",    type=int,  default=None,         help="Shuffle with this seed before sampling")
    args = parser.parse_args()

    generate_predictions(
        data_path=args.data_path,
        json_file=args.json_file,
        limit=args.limit,
        offset=args.offset,
        db_id_filter=args.db_id,
        output=args.output,
        random_seed=args.random,
    )
