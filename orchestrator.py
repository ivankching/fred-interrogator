from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider

from search_agent import get_seriess_from_question, pick_series
from pull_fred import pull_observations
from process_data import zipfile_to_csv, get_csv_schema
from query_agent import DatabaseInfo, execute_sql_agent, Answer

import logfire

logfire.configure()
logfire.instrument_pydantic_ai()

ollama_model = OpenAIChatModel(
    model_name="qwen3.5:9b",
    provider=OllamaProvider(),  
)

orchestrator_agent = Agent(
    model=ollama_model,
    output_type=str,
    deps_type=DatabaseInfo,
    system_prompt="""\
You are an agent that orchestrates the execution of agents.
You are given a user question.
You should use the 'get_data_from_question' tool to pull data required to answer the question.
You must use the 'generate_and_execute_sql' tool to answer the question.
"""
)

@orchestrator_agent.tool
async def get_data_from_question(ctx: RunContext[DatabaseInfo], question: str) -> dict | None:
    """
    This function takes a user question and uses it to pull the relevant data from FRED and return the DatabaseInfo
    where the data is stored.
    
    Args:
        ctx (RunContext[DatabaseInfo]): The context containing the DatabaseInfo object.
            Gets overriden if new data is pulled
        question (str): The user question
    
    Returns:
        dict: Success or failure of downloading and saving data
            e.g. {"success": True, "csv_path": "path/to/csv/file", "db_schema": "schema"}
    """
    
    seriess = await get_seriess_from_question(question)
    series = await pick_series(question, seriess)
    if series is None:
        logfire.error("No series chosen")
        return {"success": False, "error": "No series chosen"}
    
    observations_results = pull_observations(series["id"])
    if observations_results["success"] is False:
        return {"success": False, "error": "No observations found or no zip file saved"}
    
    csv_path = zipfile_to_csv(observations_results["zip_path"])
    if csv_path is None or len(csv_path) == 0:
        return {"success": False, "error": "Failed to unzip zip file"}
    
    db_schema = get_csv_schema(csv_path[0])
    ctx.deps = DatabaseInfo(csv_path=csv_path[0], db_schema=db_schema)
    return {"success": True, "csv_path": csv_path[0], "db_schema": db_schema}

@orchestrator_agent.tool
async def generate_and_execute_sql(ctx: RunContext[DatabaseInfo], question: str) -> Answer | None:
    """
    This function takes a DatabaseInfo object and a user question and uses it to generate and execute an SQL query.
    
    Args:
        ctx (RunContext[DatabaseInfo]): The context containing the DatabaseInfo object.
        question (str): The user question.
    
    Returns:
        str: The answer to the user question. Returns None if there if the SQL query fails.
    """
    database_info = ctx.deps
    if not database_info.csv_path.exists():
        get_data_result = await get_data_from_question(ctx, question)
        if get_data_result["success"] is False:
            logfire.error(get_data_result["error"])
            return None
        csv_path = get_data_result["csv_path"]
        db_schema = get_data_result["db_schema"]
        database_info = DatabaseInfo(csv_path=csv_path[0], db_schema=db_schema)
    answer = await execute_sql_agent.run(question, deps=database_info)
    # logfire.info(answer.output.answer)
    return answer.output