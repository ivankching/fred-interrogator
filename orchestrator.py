from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider

from search_agent import get_seriess_from_question, pick_series
from pull_fred import pull_observations
from process_data import zipfile_to_csv, get_csv_schema
from query_agent import DatabaseInfo, get_sql_query, execute_sql_query

import logfire

logfire.configure()
logfire.instrument_pydantic_ai()

ollama_model = OpenAIChatModel(
    model_name='granite4:7b-a1b-h',
    provider=OllamaProvider(),  
)

orchestrator_agent = Agent(
    model=ollama_model,
    output_type=str,
    system_prompt="""\
You are an agent that orchestrates the execution of agents.
You are given a question and a list of agents.
You should use the agents to get the DatabaseInfo required to answer the question.
Use the DatabaseInfo to execute the SQL agent to answer the question.
"""
)

@orchestrator_agent.tool_plain
async def get_data_from_question(question: str) -> DatabaseInfo | None:
    seriess = await get_seriess_from_question(question)
    series = await pick_series(question, seriess)
    if series is None:
        logfire.error("No series chosen")
        return None
    
    observations_results = pull_observations(series["id"])
    if observations_results["success"] is False:
        return None
    
    csv_path = zipfile_to_csv(observations_results["zip_path"])
    if csv_path is None:
        return None
    
    db_schema = get_csv_schema(csv_path[0])
    return DatabaseInfo(csv_path=csv_path[0], db_schema=db_schema)

# @orchestrator_agent.tool_plain
# async def generate_and_execute_sql(database_info: DatabaseInfo, question: str) -> str | None:
#     sql_query = await get_sql_query(database_info, question)
#     result = await execute_sql_query(sql_query)
#     return result