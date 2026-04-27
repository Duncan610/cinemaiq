"""
tmdb_ingestor.py
Pulls movie data from The Movie Database (TMDB) API.

What it fetches:
  - Popular movies (what people are watching NOW — current demand signal)
  - Top rated movies (historical quality benchmark)

Why both? Popular = current buzz. Top rated = quality baseline.
Comparing the two gives us "overhyped vs underhyped" signals.

Returns: a pandas DataFrame with one row per movie.
The DataFrame is then handed to snowflake_loader.py — this script
knows nothing about Snowflake. Separation of concerns.
"""

import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"


def fetch_movies(endpoint: str, pages: int = 5) -> list[dict]:
    """
    Fetches multiple pages from a TMDB endpoint.

    Args:
        endpoint: e.g. '/movie/popular' or '/movie/top_rated'
        pages: how many pages to pull (each page = 20 movies)

    Returns:
        List of raw movie dicts from the API
    """
    all_movies = []

    for page in range(1, pages + 1):
        url = f"{BASE_URL}{endpoint}"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "en-US",
            "page": page,
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()   # raises if status != 200

        data = response.json()
        all_movies.extend(data.get("results", []))
        print(f"  TMDB {endpoint} — page {page}/{pages} fetched ({len(data.get('results', []))} movies)")

    return all_movies


def fetch_movie_details(tmdb_id: int) -> dict:
    """
    Fetches extended details for one movie: budget, revenue, runtime, genres.
    The list endpoints don't include budget/revenue — need a separate call per movie.
    We only do this for a sample to avoid hitting rate limits.
    """
    url = f"{BASE_URL}/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    response = requests.get(url, params=params, timeout=10)
    if response.status_code == 200:
        return response.json()
    return {}


def ingest() -> pd.DataFrame:
    """
    Main function called by the Airflow DAG.
    Returns a clean DataFrame ready for Snowflake loading.
    """
    print("Starting TMDB ingestion...")

    # Fetching both endpoints
    popular = fetch_movies("/movie/popular", pages=5)       # 100 movies
    top_rated = fetch_movies("/movie/top_rated", pages=5)   # 100 movies

    # Tagging each with its source so we can filter later
    for m in popular:
        m["source_list"] = "popular"
    for m in top_rated:
        m["source_list"] = "top_rated"

    # Combine and deduplicate by tmdb_id
    all_movies = popular + top_rated
    seen = set()
    unique_movies = []
    for m in all_movies:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique_movies.append(m)

    print(f"  Total unique movies: {len(unique_movies)}")

    # Build DataFrame — pick only the columns we care about
    rows = []
    for m in unique_movies:
        rows.append({
            "tmdb_id":            m.get("id"),
            "title":              m.get("title"),
            "original_title":     m.get("original_title"),
            "release_date":       m.get("release_date"),
            "overview":           m.get("overview"),
            "popularity":         m.get("popularity"),
            "vote_average":       m.get("vote_average"),
            "vote_count":         m.get("vote_count"),
            "genre_ids":          str(m.get("genre_ids", [])),   # store as string
            "original_language":  m.get("original_language"),
            "adult":              m.get("adult", False),
            "source_list":        m.get("source_list"),
            "ingested_at":        datetime.utcnow(),
        })

    df = pd.DataFrame(rows)
    print(f"  TMDB ingestion complete — {len(df)} rows")
    return df


if __name__ == "__main__":
    df = ingest()
    print(df.head())
    print(f"\nShape: {df.shape}")
    print(f"Columns: {list(df.columns)}")