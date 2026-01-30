from dotenv import load_dotenv
import requests as r
import os

load_dotenv()

def search_keywords(keywords: str) -> dict | None:
    """
    Search for keywords in the Federal Reserve Economic Data (FRED) API

    Parameters
    ----------
    keywords : str
        Keywords to search for

    Returns
    -------
    dict | None
        JSON response from the FRED API, or None if the request fails
    """
    keywords = keywords.replace(" ", "+")
    api_key = str(os.getenv("FRED_API_KEY"))
    url = f"https://api.stlouisfed.org/fred/series/search?search_text={keywords}&api_key={api_key}&file_type=json"
    response = r.get(url)
    return response.json()