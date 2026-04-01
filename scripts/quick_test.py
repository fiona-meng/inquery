import json, random, sqlite3, time
from dotenv import load_dotenv
load_dotenv(override=True)

from agent import graph, initial_state
from db import set_db
from schema_loader import load_schema

DATA_PATH = "./minidev/minidev/MINIDEV"
DB_ROOT   = f"{DATA_PATH}/dev_databases"
SEED      = 7

with open(f"{DATA_PATH}/mini_dev_sqlite.json") as f:
    items = json.load(f)

random.seed(SEED)
random.shuffle(items)
items = items[:100]


def run_sql(sql, db_path):
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(sql).fetchall()
        conn.close()
        return set(rows), None
    except Exception as e:
        return None, str(e)


schema_cache = {}
results = []

for i, item in enumerate(items):
    db_id    = item["db_id"]
    question = item["question"]
    evidence = item.get("evidence", "")
    gold_sql = item["SQL"]
    db_path  = f"{DB_ROOT}/{db_id}/{db_id}.sqlite"

    set_db(db_path)
    if db_id not in schema_cache:
        schema_cache[db_id] = load_schema(db_path, sample_rows=5)

    t0 = time.time()
    try:
        result   = graph.invoke(initial_state(
            question, schema=schema_cache[db_id], db_id=db_id, evidence=evidence or None
        ))
        pred_sql = result.get("sql") or ""
    except Exception as e:
        pred_sql = ""
        print(f"[{i+1}] AGENT ERROR: {e}")
    elapsed = time.time() - t0

    pred_rows, pred_err = run_sql(pred_sql, db_path) if pred_sql else (None, "no SQL")
    gold_rows, _        = run_sql(gold_sql, db_path)

    match  = (pred_rows is not None) and (pred_rows == gold_rows)
    status = "✓" if match else "✗"
    print(f"[{i+1}/10] {status} {db_id} | {elapsed:.1f}s")
    if not match:
        print(f"  Q:        {question}")
        print(f"  Gold SQL: {gold_sql[:120]}")
        print(f"  Pred SQL: {pred_sql[:120]}")
        if pred_err:
            print(f"  Error:    {pred_err}")

    results.append(dict(idx=i+1, db_id=db_id, question=question,
                        match=match, pred_sql=pred_sql, gold_sql=gold_sql,
                        pred_err=pred_err or ""))

correct = sum(r["match"] for r in results)
print(f"\n{'='*50}")
print(f"EX: {correct}/100 = {correct}%")
print("\nFailed:")
for r in results:
    if not r["match"]:
        print(f"  [{r['idx']}] {r['db_id']}: {r['question']}")
