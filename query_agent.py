from dataclasses import dataclass

from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from typing import List
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