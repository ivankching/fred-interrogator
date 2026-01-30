from dotenv import load_dotenv
import requests as r
import os

load_dotenv()

def search_keywords(keywords: str) -> dict | None:
    keywords = keywords.replace(" ", "+")
    api_key = str(os.getenv("FRED_API_KEY"))
    url = f"https://api.stlouisfed.org/fred/series/search?search_text={keywords}&api_key={api_key}&file_type=json"
    response = r.get(url)
    return response.json()