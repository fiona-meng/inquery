from io import StringIO

import pandas as pd
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

import config
import prompts

_llm = None  # initialized lazily


def _get_llm():
    global _llm
    if _llm is None:
        _llm = ChatOpenAI(model=config.model, temperature=0).bind_tools(TOOLS)
    return _llm


# ── Tools ────────────────────────────────────────
@tool
def draw_line_chart(x_column: str, y_columns: list[str], title: str) -> dict:
    """Use when data shows a trend over time. x_column must be a date or time column."""
    return {"type": "line_chart", "x": x_column, "y": y_columns, "title": title}


@tool
def draw_bar_chart(x_column: str, y_columns: list[str], title: str) -> dict:
    """Use when comparing multiple items side by side."""
    return {"type": "bar_chart", "x": x_column, "y": y_columns, "title": title}


@tool
def draw_single_value(label: str, value: str) -> dict:
    """Use when the answer is just one product or one number."""
    return {"type": "single", "label": label, "value": value}


@tool
def draw_table(title: str) -> dict:
    """Use when showing a list of items with multiple columns."""
    return {"type": "table", "title": title}


TOOLS = [draw_line_chart, draw_bar_chart, draw_single_value, draw_table]


# ── Chart Agent ──────────────────────────────────
def run_chart_agent(question: str, df_json: str) -> dict:
    df = pd.read_json(StringIO(df_json))
    columns = df.columns.tolist()

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")

    sample = df.head(3).to_dict(orient="records")

    response = _get_llm().invoke(
        prompts.build_chart_prompt(question, columns, sample)
    )

    for tool_call in response.tool_calls:
        for t in TOOLS:
            if t.name == tool_call["name"]:
                result = t.invoke(tool_call["args"])
                print(f"[chart_agent] {tool_call['name']} → {result}")
                return result

    return {"type": "table", "title": question}
