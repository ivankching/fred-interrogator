from dotenv import load_dotenv
import requests as r
import os
import logfire

load_dotenv()
logfire.configure(send_to_logfire=True)

FRED_SEARCH_LIMIT = 20
FRED_API_URL = "https://api.stlouisfed.org/fred"

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
    url = f"{FRED_API_URL}/series/search?search_text={keywords}&api_key={api_key}&file_type=json&limit={FRED_SEARCH_LIMIT}"
    response = r.get(url).json()
    logfire.info(f"Response: {response}")
    return response

def pull_observations(series_id: str) -> dict:
    """
    Pull observations from the Federal Reserve Economic Data (FRED) API

    Parameters
    ----------
    series_id : str
        Series ID to pull observations for

    Returns
    -------
    dict | None
        success or failure of downloading and saving data, with path to zip file.
    """

    api_key = str(os.getenv("FRED_API_KEY"))
    url = f"{FRED_API_URL}/series/observations?series_id={series_id}&api_key={api_key}&file_type=csv"
    response = r.get(url)
    zip_path = f"data/{series_id}.zip"
    try:
        with open(zip_path, "wb") as f:
            f.write(response.content)
        logfire.info(f"Zip file saved: {zip_path}")
    except Exception as e:
        logfire.error(f"Error saving zip file: {e}")
        return {"success": False, "error": e}
    return {"success": True, "zip_path": zip_path}