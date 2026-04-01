import sys
from pathlib import Path
from urllib.parse import quote_plus

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv; load_dotenv(override=True)

from db import set_db
from schema_loader import load_schema
from agent import graph, initial_state

# ── Config ────────────────────────────────────────────────────────────────────
DSN = f"postgresql://postgres:{quote_plus('tNfEwFnFsqXI18rP')}@db.rvcgejcwavpyxiqjdbic.supabase.co:5432/postgres"
QUESTION = "how's recent selling?"
# ─────────────────────────────────────────────────────────────────────────────

set_db(DSN)
schema = load_schema(DSN)
result = graph.invoke(initial_state(question=QUESTION, schema=schema))
print(result)
