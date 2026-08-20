"""
fetch_buzz_top80.py

WHY THIS EXISTS:
NewsAPI and Google Trends were originally only ever fetched for the
12-movie hardcoded MOVIE_LIST same root cause as the OMDB gap we
fixed earlier. Unlike OMDB (no meaningful daily limit for our volume),
NewsAPI's free tier caps at 100 requests/day and Google Trends risks
soft-blocking under heavy request volume, so fetching for all 600
movies isn't practical in one run.

This script instead targets the TOP 80 MOVIES BY REVENUE — the ones
most likely to have had real pre-release media coverage and search
interest in the first place. A $2,000 total-gross indie film was
never going to show up in Google Trends or national news coverage
regardless of how many API calls we make; buzz data is only
meaningful for movies that had real visibility.

Run from ingestion/:
    python fetch_buzz_top80.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from loguru import logger
from loader import get_connection, load_to_snowflake, log_audit


def get_top_movies_by_revenue(limit: int = 80) -> list:
    """Pull the top N movies by revenue from int_movies_unified."""
    conn = get_connection(schema="DEV_INTERMEDIATE")
    cur = conn.cursor()
    cur.execute(f"""
        SELECT TITLE
        FROM CINEMAIQ.DEV_INTERMEDIATE.INT_MOVIES_UNIFIED
        WHERE REVENUE_USD IS NOT NULL
        ORDER BY REVENUE_USD DESC
        LIMIT {limit}
    """)
    titles = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return titles


def run():
    titles = get_top_movies_by_revenue(80)
    logger.info(f"Fetching buzz data for top {len(titles)} movies by revenue")
    print("\nSample of movies included:")
    for t in titles[:10]:
        print(f"  - {t}")
    print(f"  ... and {len(titles) - 10} more\n")

    # ── NewsAPI ──────────────────────────────────────────────────
    logger.info("Fetching NewsAPI coverage...")
    from news_api import run as news_run
    news_df = news_run(titles, days_back=30)
    n_news = load_to_snowflake(news_df, "RAW_NEWS_ARTICLES", truncate=True)
    log_audit("RAW_NEWS_ARTICLES", n_news, "NEWSAPI_TOP80")
    logger.info(f"NewsAPI: loaded {n_news} articles")

    # ── Google Trends ────────────────────────────────────────────
    logger.info("Fetching Google Trends...")
    from trends import run as trends_run
    trends_df = trends_run(movie_titles=titles, timeframes=["today 12-m", "today 3-m"])
    n_trends = load_to_snowflake(trends_df, "RAW_GOOGLE_TRENDS", truncate=True)
    log_audit("RAW_GOOGLE_TRENDS", n_trends, "GOOGLE_TRENDS_TOP80")
    logger.info(f"Google Trends: loaded {n_trends} rows")

    print("\n" + "=" * 50)
    print("Summary")
    print("=" * 50)
    print(f"  Movies targeted:  {len(titles)}")
    print(f"  News articles:    {n_news}")
    print(f"  Trends rows:      {n_trends}")


if __name__ == "__main__":
    run()