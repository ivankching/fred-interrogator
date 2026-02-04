import pytest
from search_agent import search_series, keyword_agent, get_seriess_from_query, sanitize_keywords
import logfire
from pathlib import Path

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

def test_sanitize_keywords():
    keywords = ["Monetary Service Index (MSI)", "Monthly"]
    result = sanitize_keywords(keywords)
    expected_result = "Monetary+Service+Index+%28MSI%29|Monthly"
    assert result == expected_result

@pytest.mark.asyncio
async def test_search_series_multiple():
    keywords = ["Monetary Service Index (MSI)", "Monthly"]
    result = sanitize_keywords(keywords)
    result = await search_series(result)
    assert result

@pytest.mark.asyncio
async def test_get_seriess_from_query():
    query = "What is the monthly monetary service index?"
    result = await get_seriess_from_query(query)
    expected_md_out = Path("md_output/seriess.md")
    assert result
    assert expected_md_out.is_file()
