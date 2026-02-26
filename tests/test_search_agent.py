import pytest
from search_agent import search_series, keyword_agent, get_seriess_from_question, sanitize_keywords, pick_series
import logfire
from pathlib import Path
import asyncio

logfire.configure(send_to_logfire=True)

@pytest.mark.asyncio
async def test_search_series():
    keywords = "monetary service index"
    result = await search_series(keywords)
    assert result


@pytest.mark.asyncio
async def test_keyword_agent():
    question = "What is the monthly monetary service index?"
    result = await keyword_agent.run(question)
    assert result

def test_sanitize_keywords():
    keywords = ["Monetary Service Index (MSI)", "Monthly"]
    result = sanitize_keywords(keywords)
    expected_result = ["Monetary+Service+Index+%28MSI%29", "Monthly"]
    assert result == expected_result

@pytest.mark.asyncio
async def test_search_series_multiple():
    keywords = ["Monetary Service Index (MSI)", "Monthly"]
    keywords_sanitized = sanitize_keywords(keywords)
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(search_series(keyword)) for keyword in keywords_sanitized]
    result = [task.result() for task in tasks]
    assert result

@pytest.mark.asyncio
async def test_get_seriess_from_question():
    question = "What is the monthly monetary service index?"
    result = await get_seriess_from_question(question)
    expected_md_out = Path("md_output/seriess.md")
    assert result
    assert expected_md_out.is_file()

@pytest.mark.asyncio
async def test_pick_series():
    question = "What is the monthly monetary service index?"
    with open("tests/seriess.md", "r") as f:
        seriess_md = f.read()
    result = await pick_series(question, seriess_md)
    assert result

@pytest.mark.asyncio
async def test_pick_series_unemployment():
    question = "What is the total unemployment rate in the US in 2022?"
    with open("tests/seriess_unemployment.md", "r") as f:
        seriess_md = f.read()
    result = await pick_series(question, seriess_md)
    assert result
    assert result == {"title": "Unemployment Rate", "id": "UNRATE"}
