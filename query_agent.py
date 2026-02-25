from dataclasses import dataclass

from pydantic import Field
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from urllib.parse import quote
from pathlib import Path
import duckdb

from process_data import get_csv_schema

import logfire

logfire.configure()
logfire.instrument_pydantic_ai()

ollama_model = OpenAIChatModel(
    model_name='granite4:7b-a1b-h',
    provider=OllamaProvider(),  
)

@dataclass
class DatabaseInfo():
    csv_path: Path
    db_schema: dict

sql_agent = Agent(
    model=ollama_model,
    deps_type=DatabaseInfo,
    output_type=str,
)

@sql_agent.system_prompt
async def get_system_prompt(ctx: RunContext[DatabaseInfo]) -> str:
    db_schema = None
    try:
        relation = duckdb.read_csv(ctx.deps.csv_path)
        db_schema = relation.columns
    except Exception as e:
        db_schema = ctx.deps.db_schema

    system_prompt = f"""\
You are an agent that generates SQL queries from user question.
The table name is read_csv_auto('{str(ctx.deps.csv_path).replace('\\', '/')}'
The database schema is the following:

{db_schema}

Use the user question provided to generate SQL queries to query a database.
Respond with only the SQL query. Do not include any other text.
"""
    return system_prompt


execute_sql_agent = Agent(
    model=ollama_model,
    deps_type=DatabaseInfo,
    output_type=str,
    system_prompt="""\
You are an agent that generates, validates and executes SQL queries.
Every time an SQL query is generated, validate it and execute it.
If the generated SQL query fails to validate or fails to execute, try to modify the question and generate a new SQL query.

Use the result of the SQL query to answer the user question.
"""
)

@execute_sql_agent.tool
async def get_sql_query(ctx: RunContext[DatabaseInfo], question: str) -> str:
    """
    Runs the SQL agent to generate an SQL query based on the user question and database information.

    Args:
        ctx (RunContext[DatabaseInfo]): The context containing the database information.
        question (str): The user question.

    Returns:
        str: The generated SQL query.
    """
    result = await sql_agent.run(question, deps=ctx.deps)
    sql_query = result.output
    return sql_query


@execute_sql_agent.tool_plain
async def execute_sql_query(sql_query: str) -> str|None:
    """
    Executes the SQL query and returns the result.

    Args:
        sql_query (str): The SQL query to execute.

    Returns:
        str: The stringified result of the SQL query.
    """
    sql_query.replace("\\", "") # sometimes read_csv_auto('tablename') escapes the '
    try:
        result = duckdb.sql(sql_query).fetchall()
        return str(result)
    except duckdb.Error as e:
        logfire.error(f"SQL error: {e}")
        return None