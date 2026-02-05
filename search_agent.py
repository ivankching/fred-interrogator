from dataclasses import dataclass

from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from typing import List
from urllib.parse import quote

from pull_fred import search_keywords

import logfire

logfire.configure()
logfire.instrument_pydantic_ai()


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
Use the user query provided to generate keywords to search for in the FRED API.
Pick one set of keywords and respond with only the keywords in a format sanitized for inserting into a HTTP request query parameter. Do not include any other text.
"""
)

async def search_series(keywords: str) -> str:
    """
    This function takes a set of keywords and uses them to search the FRED API.
    It then takes the results from the FRED API and formats them into markdown text.
    The markdown text is a numbered list of the series found, with each item including the title and id of the series found.
    If no results are found, it returns "No results found"

    Args:
        keywords (str): The keywords to search for in the FRED API.

    Returns:
        str: A numbered list of the series in markdown format, with each item including the title and id of the series found.
    """
    logfire.info(f"Query String: {keywords}")
    json_response = search_keywords(keywords)
    if json_response:
        seriess = json_response["seriess"]
        md_output = ""
        for i in range(len(seriess)):
            item = f"{i+1}. title: {seriess[i]["title"]}\n\t- id: {seriess[i]['id']}\n"
            md_output += item
        logfire.info(f"Tool Results: {md_output}")
        return md_output
    return "No results found"

def sanitize_keywords(keywords: List[str]) -> str:
    output = []
    for keyword in keywords:
        output.append(keyword.replace(" ", "+"))
    output = "|".join(output)
    return quote(output, safe="+|")

async def get_seriess_from_query(query: str) -> str:
    """
    This function takes a user query and uses it to search the FRED API.
    It takes the query and uses it to generate keywords, which are then used to search the FRED API.
    The results from the FRED API are then formatted into markdown text.
    The markdown text is a numbered list of the series found, with each item including the title and id of the series found.
    If no results are found, it returns "No results found"

    Args:
        query (str): The user query to search for in the FRED API.

    Returns:
        str: A numbered list of the series in markdown format, with each item including the title and id of the series found.
    """
    logfire.info(f"Query: {query}")
    result = await keyword_agent.run(query)
    logfire.info(f"Keywords: {result.output.keywords}")
    keywords = sanitize_keywords(result.output.keywords)
    seriess_md = await search_series(keywords)
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
You are an agent that picks one series from a list of series that best matches the query provided by the user.
"""
)

async def pick_series(query: str, seriess_md: str) -> dict | None:
    prompt = f"""\
Given a query {query}, pick one series that best answers the query from the following markdown list:

{seriess_md}

Respond with only the series title and id chosen.
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