"""
omdb_ingestor.py
────────────────
Pulls movie ratings from the Open Movie Database (OMDB) API.

Why OMDB and not just TMDB for ratings?
  OMDB aggregates scores from THREE sources in one call:
    - IMDb rating (general audience)
    - Rotten Tomatoes % (critics)
    - Metacritic score (critics weighted)

  The GAP between critic score and audience score is one of the most
  analytically interesting signals in this project. A movie with
  90% RT but 6.0 IMDb is very different from one with 60% RT and 8.5 IMDb.

  This divergence score is our "critic_audience_gap" feature.

What it fetches:
  We use a list of popular movie titles seeded from TMDB — so OMDB
  enriches the movies we already know about.

Returns: a pandas DataFrame with one row per movie.
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
BASE_URL = "http://www.omdbapi.com/"

# Seed list of movies to look up — drawn from consistently popular titles.
# In production this would be driven by what's in your TMDB RAW table.
# For the initial load, we use a manually curated list of 50+ titles
# that gives good coverage across genres and years.
SEED_TITLES = [
    "Oppenheimer", "Barbie", "Dune Part Two", "Poor Things",
    "The Holdovers", "Killers of the Flower Moon", "Past Lives",
    "May December", "Saltburn", "Priscilla",
    "Mission Impossible Dead Reckoning", "Indiana Jones Dial of Destiny",
    "Guardians of the Galaxy Vol 3", "Ant-Man Quantumania",
    "Spider-Man Across the Spider-Verse", "The Little Mermaid",
    "Elemental", "Wish", "The Creator", "Blue Beetle",
    "Aquaman Lost Kingdom", "The Flash", "Shazam Fury of the Gods",
    "Fast X", "Transformers Rise of the Beasts",
    "John Wick Chapter 4", "Extraction 2", "Heart of Stone",
    "The Killer", "All Quiet on the Western Front",
    "Avatar The Way of Water", "Top Gun Maverick", "Elvis",
    "The Batman", "Doctor Strange Multiverse of Madness",
    "Thor Love and Thunder", "Black Panther Wakanda Forever",
    "Glass Onion", "Babylon", "Tar", "Women Talking",
    "Everything Everywhere All at Once", "CODA", "Belfast",
    "Drive My Car", "The Power of the Dog", "Dune",
    "No Time to Die", "Shang-Chi", "Eternals",
    "Wonka", "Napoleon", "The Marvels",
]


def fetch_omdb(title: str) -> dict | None:
    """
    Fetches full movie data for one title from OMDB.
    Returns None if the movie isn't found.
    """
    params = {
        "apikey": OMDB_API_KEY,
        "t": title,        # search by title
        "type": "movie",
        "tomatoes": "true",  # include Rotten Tomatoes data
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("Response") == "True":
            return data
        else:
            print(f"  OMDB: '{title}' not found — {data.get('Error', 'unknown')}")
            return None
    except Exception as e:
        print(f"  OMDB error for '{title}': {e}")
        return None


def parse_rating(value: str) -> float | None:
    """
    Converts rating strings to floats.
    '8.5/10' → 8.5
    '94%'    → 94.0
    '78/100' → 78.0
    'N/A'    → None
    """
    if not value or value == "N/A":
        return None
    try:
        if "/" in value:
            num, denom = value.split("/")
            # Normalise to 0–10 scale
            return round(float(num.strip()) / float(denom.strip()) * 10, 2)
        elif "%" in value:
            return float(value.replace("%", "").strip())
        else:
            return float(value.strip())
    except Exception:
        return None


def ingest() -> pd.DataFrame:
    """
    Main function called by the Airflow DAG.
    Returns a clean DataFrame ready for Snowflake loading.
    """
    print("Starting OMDB ingestion...")
    rows = []

    for i, title in enumerate(SEED_TITLES):
        data = fetch_omdb(title)
        if data is None:
            continue

        # Extract ratings from the Ratings array
        ratings = {r["Source"]: r["Value"] for r in data.get("Ratings", [])}

        imdb_score  = parse_rating(data.get("imdbRating"))
        rt_score    = parse_rating(ratings.get("Rotten Tomatoes"))
        meta_score  = parse_rating(data.get("Metascore"))

        # The key derived signal: how much do critics and audiences disagree?
        # Positive = critics like it more than audiences
        # Negative = audiences like it more than critics
        critic_audience_gap = None
        if rt_score is not None and imdb_score is not None:
            # Both on 0-10 scale after normalisation
            rt_normalised = rt_score / 10.0  # rt_score is 0-100
            critic_audience_gap = round(rt_normalised - imdb_score, 2)

        rows.append({
            "imdb_id":              data.get("imdbID"),
            "title":                data.get("Title"),
            "year":                 data.get("Year"),
            "rated":                data.get("Rated"),
            "runtime_minutes":      data.get("Runtime", "").replace(" min", "") or None,
            "genre":                data.get("Genre"),
            "director":             data.get("Director"),
            "actors":               data.get("Actors"),
            "plot":                 data.get("Plot"),
            "country":              data.get("Country"),
            "box_office_usd":       data.get("BoxOffice", "").replace("$", "").replace(",", "") or None,
            "imdb_rating":          imdb_score,
            "imdb_votes":           data.get("imdbVotes", "").replace(",", "") or None,
            "rt_score":             rt_score,
            "metacritic_score":     meta_score,
            "critic_audience_gap":  critic_audience_gap,
            "awards":               data.get("Awards"),
            "ingested_at":          datetime.utcnow(),
        })

        # Be kind to the free tier — 1000 req/day limit
        if i % 10 == 0:
            print(f"  OMDB: {i+1}/{len(SEED_TITLES)} processed")
        time.sleep(0.1)

    df = pd.DataFrame(rows)
    print(f"  OMDB ingestion complete — {len(df)} rows")
    return df


# ── Standalone test ────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = ingest()
    print(df[["title", "imdb_rating", "rt_score", "metacritic_score", "critic_audience_gap"]].head(10))
    print(f"\nShape: {df.shape}")