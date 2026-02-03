from dataclasses import dataclass

from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from typing import List

from pull_fred import search_keywords

import logfire

logfire.configure()
logfire.instrument_pydantic_ai()


class Series(BaseModel):
    """Represents a single series from FRED API."""
    id: str = Field(description="The ID of the series.")
    title: str = Field(description="The title of the series.")

class SeriesList(BaseModel):
    """The overall structure representing a list of Series"""
    results: List[Series] = Field(description="A list of series from FRED API.")

class Keywords(BaseModel):
    keywords: str = Field(description="The keywords to search for in the FRED API.")

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
Pick one set of keywords and respond with only the keywords.
"""
)

def search_series(keywords: str) -> str:
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
    logfire.info(f"Keywords: {keywords}")
    json_response = search_keywords(keywords)
    if json_response:
        seriess = json_response["seriess"]
        md_output = ""
        for i in range(len(seriess)):
            item = f"{i}. title: {seriess[i]["title"]}\n\t- id: {seriess[i]['id']}\n"
            md_output += item
        logfire.info(f"Tool Results: {md_output}")
        return md_output
    return "No results found"
