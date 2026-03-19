"""
Tests for the get_data_from_question orchestrator agent tool.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock
from pydantic_ai import RunContext
from orchestrator import get_data_from_question, generate_and_execute_sql, orchestrator_agent
from process_data import get_csv_schema
from query_agent import DatabaseInfo

import logfire

logfire.configure(send_to_logfire=True)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_database_info():
    """Return a minimal DatabaseInfo mock."""
    db_info = MagicMock()
    db_info.csv_path = None
    db_info.db_schema = None
    return db_info

@pytest.fixture
def mock_ctx(mock_database_info):
    """Return a mock RunContext with DatabaseInfo deps."""
    ctx = MagicMock(spec=RunContext)
    ctx.deps = mock_database_info
    return ctx

@pytest.fixture
def mock_database_info_UNRATE():
    """Return a DatabaseInfo mock with UNRATE series."""
    db_info = MagicMock()
    db_info.csv_path = Path('tests/obs._by_real-time_period_UNRATE.csv')
    db_info.db_schema = get_csv_schema(Path('tests/obs._by_real-time_period_UNRATE.csv'))
    return db_info

@pytest.fixture
def mock_ctx_UNRATE(mock_database_info_UNRATE):
    """Return a mock RunContext with DatabaseInfo deps."""
    ctx = MagicMock(spec=RunContext)
    ctx.deps = mock_database_info_UNRATE
    return ctx

@pytest.mark.asyncio
async def test_get_data_from_question(mock_ctx):
    question = "What is the unemployment rate in the US in 2023?"
    database_info = await get_data_from_question(mock_ctx, question)
    assert database_info

@pytest.mark.asyncio
async def test_generate_and_execute_sql(mock_ctx_UNRATE):
    question = "What is the unemployment rate in the US in 2023?"
    answer = await generate_and_execute_sql(mock_ctx_UNRATE, question)
    assert answer

@pytest.mark.asyncio
async def test_orchestrator_agent():
    question = "What is the unemployment rate in the US in 2023?"
    csv_schema = get_csv_schema(Path('tests/obs._by_real-time_period_UNRATE.csv'))
    db_info = DatabaseInfo(csv_path=Path('tests/obs._by_real-time_period_UNRATE.csv'), db_schema=csv_schema)
    result = await orchestrator_agent.run(question, deps=db_info)
    assert result