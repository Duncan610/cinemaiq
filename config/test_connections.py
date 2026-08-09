"""
test_connections.py
─────────────────
Quick standalone check — confirms old API keys still work
before running the full pipeline. Run this first.
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()


def check_tmdb():
    key = os.getenv("TMDB_API_KEY")
    if not key:
        print("TMDB     : SKIP (no key in .env)")
        return
    r = requests.get("https://api.themoviedb.org/3/movie/popular",
                      params={"api_key": key, "page": 1}, timeout=10)
    if r.status_code == 200:
        print(f"TMDB     : OK  — {r.json()['results'][0]['title']}")
    else:
        print(f"TMDB     : FAIL ({r.status_code}) — {r.json().get('status_message')}")


def check_omdb():
    key = os.getenv("OMDB_API_KEY")
    if not key:
        print("OMDB     : SKIP (no key in .env)")
        return
    r = requests.get("http://www.omdbapi.com/", params={"apikey": key, "t": "Oppenheimer"}, timeout=10)
    data = r.json()
    if data.get("Response") == "True":
        print(f"OMDB     : OK  — {data['Title']} ({data.get('imdbRating')})")
    else:
        print(f"OMDB     : FAIL — {data.get('Error')}")


def check_newsapi():
    key = os.getenv("NEWSAPI_KEY")
    if not key:
        print("NEWSAPI  : SKIP (no key in .env)")
        return
    r = requests.get("https://newsapi.org/v2/everything",
                      params={"apiKey": key, "q": "movie", "pageSize": 1}, timeout=10)
    data = r.json()
    if data.get("status") == "ok":
        print(f"NEWSAPI  : OK  — {data['totalResults']} results available")
    else:
        print(f"NEWSAPI  : FAIL — {data.get('message')}")


if __name__ == "__main__":
    print("Testing stored API keys...\n")
    check_tmdb()
    check_omdb()
    check_newsapi()