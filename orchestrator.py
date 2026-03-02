from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider

from search_agent import get_seriess_from_question, pick_series
from pull_fred import pull_observations
from process_data import zipfile_to_csv, get_csv_schema
from query_agent import DatabaseInfo, get_sql_query, execute_sql_agent

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
You are given a user question.
You should use the 'get_data_from_question' tool to get the DatabaseInfo required to answer the question.
Use the DatabaseInfo in the 'generate_and_execute_sql' tool to answer the question.
"""
)

@orchestrator_agent.tool_plain
async def get_data_from_question(question: str) -> DatabaseInfo | None:
    """
    This function takes a user question and uses it to pull the relevant data from FRED and return the DatabaseInfo
    where the data is stored.
    
    Args:
        question (str): The user question
    
    Returns:
        DatabaseInfo: The database information where the data is stored. Used in the 'generate_and_execute_sql' tool
    """
    
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

@orchestrator_agent.tool_plain
async def generate_and_execute_sql(database_info: DatabaseInfo, question: str) -> str | None:
    """
    This function takes a DatabaseInfo object and a user question and uses it to generate and execute an SQL query.
    
    Args:
        database_info (DatabaseInfo): The database information where the data is stored. Generated from 'get_data_from_question' tool
        question (str): The user question.
    
    Returns:
        str: The answer to the user question. Returns None if there if the SQL query fails.
    """
    answer = await execute_sql_agent.run(question, deps=database_info)
    logfire.info(answer.output)
    return answer.output