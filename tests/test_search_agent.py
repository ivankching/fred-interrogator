import pytest
from search_agent import search_series, keyword_agent
import logfire

logfire.configure(send_to_logfire=True)

def test_search_series():
    keywords = "monetary service index"
    result = search_series(keywords)
    assert result

@pytest.mark.asyncio
async def test_keyword_agent():
    query = "What is the monthly monetary service index?"
    result = await keyword_agent.run(query)
    assert result