from query_agent import DatabaseInfo, sql_agent, execute_sql_agent
from process_data import get_csv_schema
import logfire
import pytest
import duckdb

from pathlib import Path

logfire.configure(send_to_logfire=True)

@pytest.mark.asyncio
async def test_sql_agent():
    csv_path = Path('tests/obs._by_real-time_period.csv')
    db_schema = get_csv_schema(Path('tests/obs._by_real-time_period.csv'))
    database_info = DatabaseInfo(csv_path=csv_path, db_schema=db_schema)
    result = await sql_agent.run("What is the average value of the MSIM2 series?", deps=database_info)
    logfire.info(result.output)
    duckdb.execute(result.output)
    assert result
    assert result.output.find("SELECT AVG(MSIM2)") >= 0
    assert result.output.find("FROM read_csv_auto('tests/obs._by_real-time_period.csv')") >= 0

@pytest.mark.asyncio
async def test_execute_sql_agent():
    csv_path = Path('tests/obs._by_real-time_period.csv')
    db_schema = get_csv_schema(Path('tests/obs._by_real-time_period.csv'))
    database_info = DatabaseInfo(csv_path=csv_path, db_schema=db_schema)
    result = await execute_sql_agent.run("What is the average value of the MSIM2 series?", deps=database_info)
    logfire.info(result.output)
    assert result
    assert result.output.find("4015") >= 0