import sys
from pathlib import Path
from urllib.parse import quote_plus

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv; load_dotenv(override=True)

from schema_loader import load_schema
from agent import graph, initial_state

# ── Config ────────────────────────────────────────────────────────────────────
DSN = f"postgresql://postgres:{quote_plus('tNfEwFnFsqXI18rP')}@db.rvcgejcwavpyxiqjdbic.supabase.co:5432/postgres"
QUESTIONS = [
    "how's recent selling?",
    "break it down by month",       # follow-up: should understand "selling" from turn 1
]
# ─────────────────────────────────────────────────────────────────────────────

schema = load_schema(DSN)
history = []

for q in QUESTIONS:
    print(f"\n{'─'*60}\nQ: {q}\n")
    result = graph.invoke(initial_state(question=q, schema=schema, db_path=DSN, history=history))
    print("SQL:   ", result.get("sql"))
    print("Answer:", result.get("answer"))

    # Append this turn to history for next round
    history.append({
        "question": q,
        "sql":      result.get("sql"),
        "answer":   result.get("answer"),  # None if DISABLE_CHART_INTERPRET=True, that's OK
    })
print(history)