from orchestrator import orchestrator_agent, get_data_from_question, generate_and_execute_sql
from query_agent import DatabaseInfo, get_csv_schema
from pathlib import Path
import pytest
import logfire

logfire.configure(send_to_logfire=True)

@pytest.mark.asyncio
async def test_get_data_from_question():
    question = "What is the unemployment rate in the US in 2023?"
    database_info = await get_data_from_question(question)
    assert database_info

@pytest.mark.asyncio
async def test_generate_and_execute_sql():
    question = "What is the unemployment rate in the US in 2023?"
    csv_path = Path('tests/obs._by_real-time_period_LNS14000024.csv')
    db_schema = get_csv_schema(Path('tests/obs._by_real-time_period_LNS14000024.csv'))
    database_info = DatabaseInfo(csv_path=csv_path, db_schema=db_schema)
    answer = await generate_and_execute_sql(database_info, question)
    assert answer