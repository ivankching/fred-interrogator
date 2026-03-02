from dataclasses import dataclass

from pydantic import BaseModel, Field
from pydantic_ai import Agent, ModelRetry
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from typing import List
from urllib.parse import quote
import asyncio

from pull_fred import search_keywords

import logfire

logfire.configure()
logfire.instrument_pydantic_ai()


FRED_MAX_SERIES_REQUESTS = 5
MAX_SERIES_TO_PICK = 20         # Can increase if context size allows

class Keywords(BaseModel):
    """The overall structure representing a list of keywords"""
    keywords: List[str] = Field(description="The keywords to search for in the FRED API.")

ollama_model = OpenAIChatModel(
    model_name='granite4:7b-a1b-h',
    provider=OllamaProvider(),  
)

keyword_agent = Agent(
    model=ollama_model,
    output_type=Keywords,
    system_prompt="""\
You are an agent that generates keywords from user input.
Use the user question provided to generate keywords to search for in the FRED API.
Order the keywords in the list by relevance to the user question.
Respond ONLY with a JSON object containing 'keywords' list. No other text.
"""
)

@keyword_agent.output_validator
async def validate_keywords(output: Keywords) -> Keywords:
    if not output.keywords and type(output.keywords) != list:
        raise ModelRetry("Please respond with the list of keywords")
    return output

async def search_series(keywords: str) -> List[dict]:
    """
    This function takes a set of keywords and uses them to search the FRED API.
    It then takes the results from the FRED API and formats them into markdown text.
    The markdown text is a numbered list of the series found, with each item including the title and id of the series found.
    If no results are found, it returns "No results found"

    Args:
        keywords (str): The keywords to search for in the FRED API.

    Returns:
        List[dict] | None: A list of the series as dicts, with each item including the title and id of the series found.
    """
    logfire.info(f"Question String: {keywords}")
    json_response = search_keywords(keywords)
    if json_response:
        seriess = json_response["seriess"]
        output = []
        for i in range(len(seriess)):
            item = {"title": seriess[i]["title"], "id": seriess[i]["id"]}
            output.append(item)
        logfire.info(f"Tool Results: {output}")
        return output
    logfire.error("No results found")
    return []

def seriess_to_md(seriess: List[dict]) -> str:
    md_output = ""
    for i in range(len(seriess)):
        item = f"{i+1}. title: {seriess[i]['title']}\n\t- id: {seriess[i]['id']}\n"
        md_output += item
    return md_output

def sanitize_keywords(keywords: List[str]) -> List[str]:
    output = []
    for keyword in keywords:
        output.append(quote(keyword.replace(" ", "+"), safe="+"))
    return output

async def get_seriess_from_question(question: str) -> str:
    """
    This function takes a user question and uses it to search the FRED API.
    It takes the question and uses it to generate keywords, which are then used to search the FRED API.
    The results from the FRED API are then formatted into markdown text.
    The markdown text is a numbered list of the series found, with each item including the title and id of the series found.
    If no results are found, it returns "No results found"

    Args:
        question (str): The user question to search for in the FRED API.

    Returns:
        str: A numbered list of the series in markdown format, with each item including the title and id of the series found.
    """
    logfire.info(f"Question: {question}")
    result = await keyword_agent.run(question)
    logfire.info(f"Keywords: {result.output.keywords}")
    keywords = sanitize_keywords(result.output.keywords)
    async with asyncio.TaskGroup() as tg:
        keywords = keywords[:FRED_MAX_SERIES_REQUESTS]
        tasks = [tg.create_task(search_series(keyword)) for keyword in keywords]
    series_lists = [task.result() for task in tasks] # This is a list of lists
    series_list = [x for sublist in series_lists for x in sublist]
    seriess_md = seriess_to_md(series_list[:MAX_SERIES_TO_PICK])
    with open("md_output/seriess.md", "w") as f:
        f.write(seriess_md)
    return seriess_md

class Series(BaseModel):
    title: str = Field(description="The title of the series.")
    id: str = Field(description="The id of the series.")

series_picker_agent = Agent(
    model=ollama_model,
    output_type=Series,
    system_prompt="""\
You are an agent that picks one series from a list of series that best matches the question provided by the user.
Prefer series at the higher up on the list.
Respond ONLY with a JSON object containing 'title' and 'id' fields. No other text.
"""
)

@series_picker_agent.output_validator
async def validate_series(output: Series) -> Series:
    if not output.title or not output.id:
        raise ModelRetry("Please respond with both a title and id.")
    return output

async def pick_series(question: str, seriess_md: str) -> dict | None:
    prompt = f"""\
Given a question {question}, pick one series that is most relevant to the question from the following markdown list:

{seriess_md}

Prefer series at the higher up on the list.
Respond with ONLY a JSON object in this exact format, nothing else:
{{"title": "Series Title Here", "id": "series-id-here"}}
"""
    logfire.info(f"Prompt: {prompt}")
    result = await series_picker_agent.run(prompt)
    if result.output is None:
        logfire.error("No series chosen")
        return None
    logfire.info(f"Series: {result.output}")
    series = {}
    series["title"] = result.output.title
    series["id"] = result.output.id
    return series