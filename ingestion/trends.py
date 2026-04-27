"""
google_trends_ingestor.py
─────────────────────────
Pulls search interest data from Google Trends via the pytrends library.

Why Google Trends?
  Google search volume is the clearest signal of PUBLIC AWARENESS.
  Before a movie releases:
    - Search spike = anticipation (people looking it up)
    - Sustained interest = word-of-mouth spreading
    - Drop-off = audience moved on

  This is the "pre-release awareness" feature in our predictive mart.
  Importantly it's available BEFORE a movie opens — making it
  genuinely predictive, not just explanatory.

How pytrends works:
  It's an unofficial wrapper around Google's public Trends interface.
  Free, no API key needed. Rate limits are soft — we add delays.

What we capture:
  - Weekly search interest score (0-100, Google normalised) over 90 days
  - Peak interest score
  - Date of peak (how far before release?)

Returns: a pandas DataFrame with one row per (movie, week).
"""

import time
import pandas as pd
from datetime import datetime
from pytrends.request import TrendReq

# Movies to track — same seed list for consistency across sources
MOVIE_TITLES = [
    "Oppenheimer", "Barbie movie", "Dune Part Two",
    "Poor Things", "Killers of the Flower Moon",
    "Spider-Man Spider-Verse", "Guardians Galaxy",
    "John Wick", "Fast X", "The Flash",
    "Mission Impossible", "Indiana Jones",
    "Avatar Way of Water", "Top Gun Maverick",
    "Everything Everywhere All at Once",
    "Wonka", "Napoleon 2023", "The Marvels",
    "Elemental Pixar", "Blue Beetle",
]


def fetch_trends(keyword: str, timeframe: str = "today 3-m") -> pd.DataFrame:
    """
    Fetches Google search interest for one keyword over a timeframe.

    Args:
        keyword: movie title to search
        timeframe: 'today 3-m' = last 90 days, 'today 12-m' = last year

    Returns:
        DataFrame with columns: date, interest_score, keyword
    """
    try:
        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))
        pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="", gprop="")

        interest_df = pytrends.interest_over_time()

        if interest_df.empty:
            print(f"  Google Trends: no data for '{keyword}'")
            return pd.DataFrame()

        # Reset index so 'date' becomes a column
        interest_df = interest_df.reset_index()
        interest_df = interest_df[["date", keyword]].rename(columns={keyword: "interest_score"})
        interest_df["keyword"] = keyword

        return interest_df

    except Exception as e:
        print(f"  Google Trends error for '{keyword}': {e}")
        return pd.DataFrame()


def ingest() -> pd.DataFrame:
    """
    Main function called by the Airflow DAG.
    Returns a clean DataFrame ready for Snowflake loading.
    """
    print("Starting Google Trends ingestion...")
    all_frames = []

    for i, title in enumerate(MOVIE_TITLES):
        print(f"  Trends: fetching '{title}' ({i+1}/{len(MOVIE_TITLES)})")
        df = fetch_trends(title)

        if not df.empty:
            all_frames.append(df)

        # Google Trends rate limits aggressively — must sleep between requests
        # Free tier: ~5 requests/minute safely
        time.sleep(15)

    if not all_frames:
        print("  Warning: no trends data returned")
        return pd.DataFrame(columns=["keyword", "date", "interest_score", "ingested_at"])

    combined = pd.concat(all_frames, ignore_index=True)
    combined["ingested_at"] = datetime.utcnow()

    # Add peak stats per keyword
    peak_stats = (
        combined.groupby("keyword")["interest_score"]
        .agg(peak_score="max")
        .reset_index()
    )
    combined = combined.merge(peak_stats, on="keyword", how="left")

    print(f"  Google Trends ingestion complete — {len(combined)} rows across {len(all_frames)} movies")
    return combined


# ── Standalone test ────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Test with just 2 titles to avoid long wait
    import sys
    quick_test = MOVIE_TITLES[:2]
    MOVIE_TITLES.clear()
    MOVIE_TITLES.extend(quick_test)

    df = ingest()
    print(df.head(10))
    print(f"\nShape: {df.shape}")