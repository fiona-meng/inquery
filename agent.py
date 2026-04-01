import json
import os
from io import StringIO
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

import config
import prompts
from chart_agent import run_chart_agent
from db import run_query
from schema_loader import filter_schema_by_tables
from utils import parse_json, fix_sqlite_backticks

load_dotenv(override=True)

llm = ChatOpenAI(model=config.model, temperature=0)


# ── State ─────────────────────────────────────────────────────────────────────
class State(TypedDict):
    question:      str
    is_data_query: bool
    sql:           Optional[str]
    df_json:       Optional[str]
    exec_error:    Optional[str]
    error:         Optional[str]
    retry_count:   int
    chart_config:  Optional[dict]
    answer:        Optional[str]
    db_path:         Optional[str]   # Active database DSN or file path
    db_id:           Optional[str]   # BIRD db identifier, or None
    schema:          Optional[str]   # Full schema string (loaded once per DB)
    filtered_schema: Optional[str]   # Schema pruned to relevant tables for this question
    evidence:        Optional[str]   # BIRD evidence field (domain knowledge hint)


# ── Nodes ─────────────────────────────────────────────────────────────────────
def classify(state: State) -> dict:
    # In evaluation mode (db_id set), every question is a data query — skip LLM call
    if state.get("db_id"):
        return {"is_data_query": True}
    try:
        response = llm.invoke(prompts.build_classify_prompt(state["question"]))
        return {"is_data_query": "YES" in response.content.strip().upper()}
    except Exception as e:
        return {"error": f"[classify] {e}"}



_SCHEMA_FILTER_THRESHOLD = 10000  # chars; skip filtering for small schemas


def schema_filter(state: State) -> dict:
    """
    Select only the tables relevant to the question.
    Skipped for small schemas (≤ _SCHEMA_FILTER_THRESHOLD chars) — passes full schema through.
    Falls back to full schema if filtering fails.
    """
    try:
        full_schema = state.get("schema") or ""
        if not full_schema:
            return {"filtered_schema": full_schema}

        if len(full_schema) <= _SCHEMA_FILTER_THRESHOLD:
            print(f"[schema_filter] schema small ({len(full_schema)} chars), skipping filter")
            return {"filtered_schema": full_schema}

        question = state["question"]
        evidence = state.get("evidence") or ""
        evidence_block = f"Evidence: {evidence}\n" if evidence else ""

        response = llm.invoke(
            prompts.build_schema_filter_prompt(question, evidence_block, full_schema)
        )

        raw = response.content.strip()
        selected = [t.strip().strip("`") for t in raw.split(",") if t.strip()]

        if not selected:
            return {"filtered_schema": full_schema}

        pruned = filter_schema_by_tables(full_schema, selected)
        return {"filtered_schema": pruned}

    except Exception as e:
        print(f"[schema_filter] non-fatal: {e}")
        return {"filtered_schema": state.get("schema") or ""}





def generate_sql(state: State) -> dict:
    try:
        schema = state.get("filtered_schema") or state.get("schema") or ""
        evidence = state.get("evidence") or ""
        evidence_block = f"\n== EVIDENCE ==\n{evidence}" if evidence else ""

        response = llm.invoke(
            f"""{prompts.build_sql_rules("sqlite")}
== SCHEMA ==
{schema}
== QUESTION ==
{state['question']}{evidence_block}
"""
        )
        sql = parse_json(response.content).get("sql", "").strip()
        if not sql:
            return {"error": "[generate_sql] LLM returned empty SQL"}
        return {"sql": fix_sqlite_backticks(sql), "exec_error": None}
    except Exception as e:
        return {"error": f"[generate_sql] {e}"}




def verify_columns(state: State) -> dict:
    try:
        sql      = state.get("sql") or ""
        question = state["question"]
        evidence = state.get("evidence") or ""

        response = llm.invoke(
            prompts.build_verify_columns_prompt(question, evidence, sql)
        )
        parsed = parse_json(response.content)
        if parsed.get("ok"):
            return {}  # nothing to change
        fixed = parsed.get("sql", "").strip()
        if not fixed:
            return {}
        print(f"[verify_columns] fixed SELECT columns")
        return {"sql": fixed}
    except Exception as e:
        print(f"[verify_columns] non-fatal: {e}")
        return {}


def execute_sql(state: State) -> dict:
    try:
        df, err = run_query(state["sql"], state.get("db_path") or "")
        if err:
            return {"exec_error": err}
        if df.empty:
            return {"df_json": None, "exec_error": None}
        return {"df_json": df.to_json(orient="records"), "exec_error": None}
    except Exception as e:
        return {"exec_error": str(e)}


def self_correct(state: State) -> dict:
    try:
        schema = state.get("filtered_schema") or state.get("schema") or ""
        evidence = state.get("evidence") or ""
        evidence_block = f"== EVIDENCE ==\n{evidence}\n\n" if evidence else ""

        response = llm.invoke(
            prompts.build_self_correct_prompt("sqlite", schema, state["sql"], state["exec_error"], evidence_block)
        )
        parsed = parse_json(response.content)
        diagnosis = parsed.get("diagnosis", "")
        sql = fix_sqlite_backticks(parsed.get("sql", "").strip())
        if diagnosis:
            print(f"[self_correct] diagnosis: {diagnosis}")
        return {"sql": sql, "exec_error": None, "retry_count": (state.get("retry_count") or 0) + 1}
    except Exception as e:
        return {"error": f"[self_correct] {e}"}


def chart(state: State) -> dict:
    if config.DISABLE_CHART_INTERPRET:
        return {"chart_config": None}
    try:
        if not state.get("df_json"):
            return {"chart_config": None}
        return {"chart_config": run_chart_agent(state["question"], state["df_json"])}
    except Exception as e:
        print(f"[chart] non-fatal: {e}")
        return {"chart_config": None}


def interpret(state: State) -> dict:
    if config.DISABLE_CHART_INTERPRET:
        return {"answer": None}
    try:
        if not state.get("df_json"):
            return {"answer": "No data found for your question."}

        sample = pd.read_json(StringIO(state["df_json"])).head(5).to_dict(orient="records")
        response = llm.invoke(
            prompts.build_interpret_prompt(state["question"], sample)
        )
        return {"answer": response.content.strip()}
    except Exception as e:
        return {"error": f"[interpret] {e}"}


def handle_error(state: State) -> dict:
    msg = state.get("error") or f"Query failed after retries: {state.get('exec_error')}"
    return {"answer": f"❌ {msg}"}


# ── Routing ───────────────────────────────────────────────────────────────────
def route_after_classify(state: State) -> str:
    if state.get("error"):     return "handle_error"
    if state["is_data_query"]: return "schema_filter"
    return END



def route_after_generate(state: State) -> str:
    if state.get("error"): return "handle_error"
    return "execute_sql"


def route_after_execute(state: State) -> str:
    if state.get("error"):                  return "handle_error"
    if not state.get("exec_error"):         return "success"
    if (state.get("retry_count") or 0) < 2: return "retry"
    return "give_up"


def route_after_correct(state: State) -> str:
    if state.get("error"): return "handle_error"
    return "execute_sql"


# ── Graph ─────────────────────────────────────────────────────────────────────
_builder = StateGraph(State)

_builder.add_node("classify",        classify)
_builder.add_node("schema_filter",   schema_filter)
_builder.add_node("generate_sql",    generate_sql)
_builder.add_node("verify_columns",  verify_columns)
_builder.add_node("execute_sql",     execute_sql)
_builder.add_node("self_correct",    self_correct)
_builder.add_node("chart",           chart)
_builder.add_node("interpret",       interpret)
_builder.add_node("handle_error",    handle_error)

_builder.set_entry_point("classify")

_builder.add_conditional_edges("classify",     route_after_classify, {
    "schema_filter": "schema_filter",
    "handle_error":  "handle_error",
    END:             END,
})
_builder.add_edge("schema_filter",  "generate_sql")
_builder.add_conditional_edges("generate_sql", route_after_generate, {
    "execute_sql":  "verify_columns",
    "handle_error": "handle_error",
})
_builder.add_edge("verify_columns", "execute_sql")
_builder.add_conditional_edges("execute_sql",  route_after_execute, {
    "success":      "chart",
    "retry":        "self_correct",
    "give_up":      "handle_error",
    "handle_error": "handle_error",
})
_builder.add_conditional_edges("self_correct", route_after_correct, {
    "execute_sql":  "execute_sql",
    "handle_error": "handle_error",
})

_builder.add_edge("chart",        "interpret")
_builder.add_edge("interpret",    END)
_builder.add_edge("handle_error", END)

graph = _builder.compile()


# ── Initial state helper ───────────────────────────────────────────────────────
def initial_state(
    question: str,
    schema: Optional[str] = None,
    db_path: Optional[str] = None,
    db_id: Optional[str] = None,
    evidence: Optional[str] = None,
) -> dict:
    return {
        "question":        question,
        "is_data_query":   False,
        "sql":             None,
        "df_json":         None,
        "exec_error":      None,
        "error":           None,
        "retry_count":     0,
        "chart_config":    None,
        "answer":          None,
        "db_path":         db_path,
        "db_id":           db_id,
        "schema":          schema,
        "filtered_schema": None,
        "evidence":        evidence,
    }
