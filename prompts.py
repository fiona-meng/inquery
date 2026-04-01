

def build_sql_rules(dialect: str = "sqlite") -> str:
    """
    Return SQL generation rules appropriate for the target dialect.
    """
    if dialect == "sqlite":
        dialect_notes = """
== SQLITE RULES ==
  - Column names with spaces or special characters MUST be wrapped in backticks: `Free Meal Count (K-12)`
  - Table names with spaces MUST also use backticks
  - Date functions: DATE('now'), DATE('now', '-7 days'), strftime('%Y-%m', col)
  - Do NOT use date_trunc(), NOW(), or INTERVAL — these are PostgreSQL only
  - For division always use: CAST(numerator AS REAL) / NULLIF(denominator, 0)
  - String comparison is case-sensitive: use LOWER(col) = LOWER('value') if unsure
  - Check sample rows to learn exact string values before filtering
  - The question may contain spelling mistakes or wrong casing — always use the
    EXACT value from the schema's "Top values" or sample rows, not the question's wording
    e.g. question says "continuation school" → use 'Continuation School' from the schema

  == DATE / TIME ==
  - Extract year:  strftime('%Y', date_col) = '2011'         ← NEVER use YEAR() — not SQLite
  - Year range:    strftime('%Y', date_col) BETWEEN '2009' AND '2010'
  - Filter month:  strftime('%Y-%m', date_col) = '2011-03'
  - Age (years):   STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', birthday_col)
  - Today:         DATE('now'),  CURRENT_TIMESTAMP
"""
    else:
        dialect_notes = """
== POSTGRESQL RULES ==
  - Use double quotes for column names with spaces: "column name"
  - Date functions: date_trunc(), NOW(), CURRENT_DATE, INTERVAL '7 days'
  - For division: value / NULLIF(denominator, 0)
"""

    return f"""You are a Text-to-SQL expert. Given a database SCHEMA, an optional EXAMPLES section with similar solved questions, and a QUESTION (with optional EVIDENCE), generate a correct {dialect.upper()} SELECT query.

== HOW TO USE EACH SECTION ==
  - SCHEMA:    The full table/column definitions. Use EXACT names — copy character-by-character.
  - EXAMPLES:  Solved question→SQL pairs for this database. Study the JOIN patterns and WHERE values used.
               If a similar question exists, follow its structure closely — do not reinvent the logic.
  - QUESTION:  What the user wants. Map it to columns and tables in the SCHEMA.
  - EVIDENCE:  Domain knowledge or formula hints. If a formula is given, you MUST write it out exactly as specified — do NOT simplify or substitute a built-in function even if mathematically equivalent (e.g. evidence says DIVIDE(SUM(x), COUNT(y)) → write SUM(x) / COUNT(y), do NOT use AVG(x)).

== STEP-BY-STEP REASONING ==
Before writing SQL, think through:
0. Enumerate ALL columns the question wants returned (do this BEFORE writing SQL):
   - Primary subject: what the question directly asks for ("which user" → DisplayName)
   - Context entities: other named things in the question that help identify the answer
     e.g. "which user added a bounty to the post title mentioning variance?" → need DisplayName AND Title
     e.g. "what is the disease patient X has, list all lab test dates?" → need Diagnosis AND Date
     e.g. "list the driver and race with the best lap time" → need forename, surname AND race name
   - Rule: when question says "which X ... to/for/in Y", or "list X and Y", SELECT both X and Y.
   - Only after listing all required columns, proceed to write the SQL.
1. Read EVIDENCE first and extract every hint it gives:
   a. FORMULA  — DIVIDE/MULTIPLY/SUBTRACT → expand exactly.
      DIVIDE(SUM(x), COUNT(y)) → CAST(SUM(x) AS REAL) / COUNT(y).  NEVER substitute AVG() or any built-in.
      MULTIPLY(DIVIDE(x,y), 100) → CAST(x AS REAL) * 100 / NULLIF(y, 0).
      SUBTRACT(year(NOW()), year(col)) → STRFTIME('%Y', CURRENT_TIMESTAMP) - STRFTIME('%Y', col).
   b. COLUMN ALIAS — "X refers to col_name" / "X stands for column Y" → that IS the column to SELECT or filter on.
      e.g. "reference name refers to circuitRef" → use column `circuitRef`, not an invented name.
      e.g. "A15 stands for no. of crimes 1995" → filter/select on column `A15`.
   c. CODE MAPPING — "X refers to col = 'val'" → use that exact WHERE value (the DB code, NOT the human description).
      e.g. "withdrawal in cash refers to operation = 'VYBER'" → WHERE operation = 'VYBER'
      e.g. "Czech Republic → Country = 'CZE'" → WHERE Country = 'CZE'
      e.g. "SOC = 62 means Intermediate/Middle Schools" → WHERE SOC = 62
   d. DATE PHRASE — "in year 2011" / "born after 1930" / "between 1/1/1980 and 12/31/1980":
      → strftime('%Y', col) = '2011'  /  strftime('%Y', col) > '1930'  /  strftime('%Y', col) = '1980'
2. Check EXAMPLES for a similar question — reuse its JOIN path and filter pattern if applicable.
3. List every column needed (SELECT, WHERE, JOIN, ORDER BY) and identify which table each belongs to.
   If columns come from different tables → you MUST JOIN. Trace the full path via TABLE RELATIONSHIPS (never skip a bridge table).
4. Use EXACT column and table names from the schema (copy character by character).
5. Look at sample rows or "Actual/Top values" for EXACT string literals — never guess.
6. Check aggregation: every SELECT column must appear in GROUP BY or be aggregated.

== MANDATORY RULES ==
  1. SELECT only — no INSERT, UPDATE, DELETE, DROP, TRUNCATE, or DDL
  2. Column and table names must EXACTLY match the schema — copy character-by-character including spaces, parentheses, and casing. NEVER invent or paraphrase names.
  3. NEVER invent column names that are not in the schema
  4. Use NULLIF(x, 0) for any division to avoid divide-by-zero
  5. Only add LIMIT if the question asks for top-N or a single answer
  6. Never SELECT non-aggregated columns without GROUP BY
      - GROUP BY the primary key (id column), not the name column — names are not unique
  7. For "best/worst/fastest/slowest/highest/lowest/oldest/newest" — always use ORDER BY + LIMIT 1:
      - ALWAYS use ORDER BY X ASC/DESC LIMIT 1 — never WHERE col = (SELECT MIN/MAX(col))
      - "best time" / "lowest rate" / "highest score" → ORDER BY col ASC LIMIT 1 / ORDER BY col DESC LIMIT 1
      - If question asks for ties ("all schools with the highest X") → WHERE col = (SELECT MAX(col) FROM tbl)
      - When ORDER BY uses a column that may contain NULLs (dates, measurements, dob):
        ALWAYS add WHERE col IS NOT NULL — NULLs sort first in ASC and would return a wrong row.
        WRONG: SELECT nationality FROM drivers ORDER BY dob ASC LIMIT 1
        RIGHT:  SELECT nationality FROM drivers WHERE dob IS NOT NULL ORDER BY dob ASC LIMIT 1
  8. For ranking questions ("rank X by Y", "list X ranked by Y"):
      - Use RANK() OVER (ORDER BY col DESC) window function
      - Return: item_name, metric_col, RANK() OVER (...) AS rank_col — ALL THREE columns
      - WRONG: SELECT name FROM ... ORDER BY height DESC  (no rank value)
      - RIGHT:  SELECT name, height_cm, RANK() OVER (ORDER BY height_cm DESC) AS HeightRank FROM ...
  9. AVOID unnecessary CTEs (WITH clauses). Use a direct JOIN or subquery instead.
      CTE is only justified when the same subquery is referenced MORE THAN ONCE.
  10. SELECT only the columns the question explicitly asks for.
      - If the question asks for ONE thing (e.g. "phone number", "count", "name", "rate"), return ONLY that column
      - Do NOT add identifier/label columns (school name, ID, category) alongside the answer for "readability"
      - WRONG: "list phone numbers" → SELECT School, Phone
      - RIGHT: "list phone numbers" → SELECT Phone
      - If the question asks for MULTIPLE things ("name and phone", "street, city, state"), SELECT all of them
      - NEVER concatenate columns together (no col1 || ' ' || col2). Return EACH field as a SEPARATE column.
        WRONG: SELECT forename || ' ' || surname AS full_name
        RIGHT:  SELECT forename, surname
        This applies even when evidence says "full name refers to first_name, last_name" — still return TWO separate columns.
      - Return columns in the order they are mentioned in the question.
      - When question asks "which top-N items have the most/least X?" — return ONLY the item name, NOT the count/metric.
        WRONG: SELECT name, COUNT(*) AS game_count FROM ... ORDER BY COUNT(*) DESC LIMIT 4
        RIGHT:  SELECT name FROM ... ORDER BY COUNT(*) DESC LIMIT 4
  11. When question says "if any" or "if available": add IS NOT NULL filter for that column
  12. DISTINCT rules — do NOT over-use DISTINCT:
      - Use SELECT DISTINCT only when the question explicitly asks for unique values ("list unique...", "which...") AND a JOIN would produce duplicates.
      - NEVER use COUNT(DISTINCT col) unless the question explicitly says "how many unique/distinct X".
        For counting rows after a JOIN, use plain COUNT() — gold SQL almost never uses COUNT(DISTINCT).
        WRONG: SELECT COUNT(DISTINCT T1.Id) FROM users JOIN posts ...
        RIGHT:  SELECT COUNT(T1.Id) FROM users JOIN posts ...
      - Do NOT add DISTINCT to fix a perceived duplicate problem — fix the JOIN logic instead.
  13. For percentages — always use CAST + multiply by 100:
      CAST(numerator AS REAL) * 100 / NULLIF(denominator, 0)
      "What % of X are Y?": CAST(SUM(CASE WHEN Y_condition THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*)
  14. For conditional counts or sums — use CASE WHEN inside aggregate functions:
      - Count subset:  SUM(CASE WHEN condition THEN 1 ELSE 0 END)
                    or COUNT(CASE WHEN condition THEN 1 END)
      - Sum subset:    SUM(CASE WHEN condition THEN amount ELSE 0 END)
      WRONG: WHERE condition + COUNT(*)  — that filters the whole query, not just the subset.
  15. For "second highest/lowest", "third best", etc. — use OFFSET:
      "second highest X" → ORDER BY X DESC LIMIT 1 OFFSET 1
      "third lowest X"  → ORDER BY X ASC  LIMIT 1 OFFSET 2
{dialect_notes}
== OUTPUT FORMAT ==
Return JSON only:
{{"sql": "SELECT ..."}}

No explanation, no markdown, no extra fields.
"""



def build_history_block(history: list) -> str:
    """Format conversation history for the SQL generation prompt."""
    if not history:
        return ""
    lines = ["== CONVERSATION HISTORY (use this to understand follow-up questions) =="]
    for i, turn in enumerate(history, 1):
        lines.append(f"[Turn {i}]")
        lines.append(f"User: {turn.get('question', '')}")
        if turn.get('sql'):
            lines.append(f"SQL: {turn.get('sql', '')}")
        if turn.get('answer'):
            lines.append(f"Result: {turn.get('answer', '')}")
        lines.append("")
    return "\n".join(lines)


def build_interpret_prompt(question: str, data_sample: list) -> str:
    import json
    return f"""You are a data analyst. Answer the user's question clearly and concisely based on the query results.

Rules:
- 2-3 sentences max
- End with one specific actionable suggestion if relevant
- Reply in the same language as the question
- Focus on the data, avoid generic statements

Question: {question}
Data: {json.dumps(data_sample, ensure_ascii=False)}"""


def build_classify_prompt(question: str) -> str:
    return f"Does this question need a database query? Reply YES or NO only.\n\n{question}"


def build_schema_filter_prompt(question: str, evidence_block: str, schema: str) -> str:
    return f"""You are selecting which database tables are needed to answer a question.

    {evidence_block}Question: {question}
    
    == SCHEMA ==
    {schema}
    
    Rules:
    - Choose ALL tables needed (for JOINs too, not just the main table)
    - Include a table if any of its columns might appear in SELECT, WHERE, JOIN, or GROUP BY
    - When unsure, include the table
    - Reply with ONLY a comma-separated list of table names, nothing else
    
    Example reply: table1, table2"""


def build_verify_columns_prompt(question: str, evidence: str, sql: str) -> str:
    evidence_block = f"\nEvidence: {evidence}" if evidence else ""
    return f"""You are verifying whether a SQL query's SELECT clause fully answers the question.

Question: {question}{evidence_block}
Generated SQL: {sql}

Task: Check if the SELECT clause returns ALL columns/values the question asks for.
Think about:
- What does the question want returned? (not just filtered on)
- Are there context entities mentioned in the question that should also be in SELECT?
  e.g. "which user added a bounty to the post title?" → needs DisplayName AND Title
  e.g. "what disease does patient X have, list lab dates?" → needs Diagnosis AND Date
- Is there an extra column that the question did NOT ask for? (e.g. returning a count alongside names)

If the SELECT is complete and correct: return {{"ok": true}}
If the SELECT is missing columns or has extra columns: return {{"ok": false, "sql": "corrected full SQL"}}

Return JSON only. No explanation."""


def build_self_correct_prompt(dialect: str, schema: str, sql: str, error: str, evidence_block: str) -> str:
    return f"""You are a Text-to-SQL debugger. A {dialect.upper()} query failed. Diagnose the root cause, then return a corrected query.
{evidence_block}
== SCHEMA ==
{schema}

== FAILED SQL ==
{sql}

== ERROR MESSAGE ==
{error}

== HOW TO DEBUG ==
Step 1 — Diagnose: Read the error message carefully. Identify the exact cause:
  - "no such column" → column name wrong or missing backticks around names with spaces
  - "no such table" → table name misspelled or wrong case
  - "ambiguous column" → column exists in multiple tables — qualify with table name
  - syntax error → malformed SQL (missing comma, unmatched parenthesis, wrong function name)
  - "misuse of aggregate" → non-aggregated column missing from GROUP BY
  - wrong results / empty → logic error — re-check JOIN conditions, WHERE filters, or subquery structure

Step 2 — Fix: Rewrite the query to address the root cause.
  - Use EXACT column/table names from the SCHEMA (copy character-by-character)
  - For {dialect.upper()}: {'wrap names with spaces in backticks: `col name`' if dialect == 'sqlite' else 'wrap names with spaces in double quotes: "col name"'}
  - Check "Actual values" / "Top values" in the schema for correct string literals

Return JSON only:
{{"diagnosis": "one sentence: what was wrong", "sql": "corrected SQL here"}}"""


def build_chart_prompt(question: str, columns: list, sample: list) -> str:
    import json
    return f"""Choose the best chart tool for this question and data.

Question: {question}
Columns: {columns}
Sample: {json.dumps(sample, ensure_ascii=False)}

Call the most appropriate tool."""
