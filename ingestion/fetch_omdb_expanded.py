"""
fetch_omdb_expanded.py

WHY THIS EXISTS:
The original OMDB load only covered 12 hardcoded movies (MOVIE_LIST
in the DAG), while TMDB pulled hundreds. That left critic_audience_gap —
our standout signal — populated for almost nothing.

This script fixes that: it pulls every imdb_id already matched in
int_movies_unified (528 movies), queries OMDB by imdb_id directly
(more reliable than title search — no ambiguity), and reloads
RAW_OMDB_RATINGS with full coverage.

WHY QUERY SNOWFLAKE FOR THE IMDB LIST:
We want OMDB data for movies we ALREADY know exist and matched IMDb —
querying int_movies_unified for those imdb_ids means we're not wasting
API calls on movies OMDB won't find anyway.

"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

import requests
import pandas as pd
from datetime import datetime
from loguru import logger

from loader import get_connection, load_to_snowflake, log_audit
from omdb import get_api_key, parse_rating, flatten_to_row


def fetch_by_imdb_id(imdb_id: str) -> dict:
    """Fetch OMDB data by IMDb ID — precise, no title ambiguity."""
    try:
        resp = requests.get(
            "http://www.omdbapi.com/",
            params={"apikey": get_api_key(), "i": imdb_id, "plot": "short"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("Response") == "False":
            logger.warning(f"OMDB no result for imdb_id={imdb_id}: {data.get('Error')}")
            return {}
        return data
    except Exception as e:
        logger.error(f"OMDB fetch failed imdb_id={imdb_id}: {e}")
        return {}


def get_imdb_ids_to_fetch() -> list:
    """Pull every distinct imdb_id already matched in int_movies_unified."""
    conn = get_connection(schema="DEV_INTERMEDIATE")
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT IMDB_ID
        FROM CINEMAIQ.DEV_INTERMEDIATE.INT_MOVIES_UNIFIED
        WHERE IMDB_ID IS NOT NULL
    """)
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return ids


def run(delay: float = 0.3) -> pd.DataFrame:
    imdb_ids = get_imdb_ids_to_fetch()
    logger.info(f"Fetching OMDB data for {len(imdb_ids)} movies...")

    rows = []
    for i, imdb_id in enumerate(imdb_ids, 1):
        raw = fetch_by_imdb_id(imdb_id)
        if raw:
            rows.append(flatten_to_row(raw))
        if i % 50 == 0:
            logger.info(f"  progress: {i}/{len(imdb_ids)}")
        time.sleep(delay)  # stay well under OMDB's 1,000/day free tier

    df = pd.DataFrame(rows)
    logger.info(f"OMDB expanded fetch complete: {len(df)} rows")
    return df


if __name__ == "__main__":
    df = run()
    if df.empty:
        print("No data returned.")
    else:
        n = load_to_snowflake(df, "RAW_OMDB_RATINGS", truncate=True)
        log_audit("RAW_OMDB_RATINGS", n, "OMDB_API_EXPANDED")
        print(f"Loaded {n} rows into RAW_OMDB_RATINGS")
        print(df[["title", "imdb_rating", "rt_score", "critic_audience_gap"]].head(10).to_string())