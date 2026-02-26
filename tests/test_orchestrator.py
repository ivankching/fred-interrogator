from orchestrator import orchestrator_agent, get_data_from_question
import pytest
import logfire

logfire.configure(send_to_logfire=True)

@pytest.mark.asyncio
async def test_get_data_from_question():
    question = "What is the total unemployment rate in the US in 2023?"
    database_info = await get_data_from_question(question)
    assert database_info