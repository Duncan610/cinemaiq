"""
newsapi_ingestor.py
───────────────────
Pulls movie-related news articles from NewsAPI.

Why NewsAPI?
  Pre-release media coverage volume is a measurable proxy for "hype".
  Before a movie opens, journalists write about it. More articles =
  more public awareness = usually higher opening weekend.

  We capture:
    - Article count per movie (buzz volume)
    - Sentiment direction from headlines (positive / negative framing)
    - Source diversity (is it just fan sites, or mainstream media too?)

What it fetches:
  We search for each movie title in the "entertainment" category.
  NewsAPI free tier gives us 100 requests/day and articles from the
  last 30 days — perfect for pre-release buzz tracking.

Returns: a pandas DataFrame with one row per article.
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
BASE_URL = "https://newsapi.org/v2/everything"

# Same seed list as OMDB — we track coverage for the same movies
MOVIE_TITLES = [
    "Oppenheimer film", "Barbie movie", "Dune Part Two",
    "Poor Things film", "Killers of the Flower Moon",
    "Spider-Man Spider-Verse", "Guardians Galaxy Vol 3",
    "John Wick 4", "Fast X movie", "The Flash DC",
    "Mission Impossible Dead Reckoning", "Indiana Jones",
    "Avatar Way of Water", "Top Gun Maverick",
    "Everything Everywhere All at Once",
    "Wonka movie", "Napoleon movie 2023",
    "The Marvels movie", "Elemental Pixar",
    "Blue Beetle movie",
]

# How far back to look for articles (free tier: max 30 days)
DAYS_BACK = 30


def fetch_articles(query: str, max_articles: int = 20) -> list[dict]:
    """
    Fetches news articles for one movie query.
    Returns list of article dicts.
    """
    from_date = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")

    params = {
        "apiKey":   NEWSAPI_KEY,
        "q":        query,
        "language": "en",
        "sortBy":   "relevancy",
        "pageSize": min(max_articles, 100),  # API max is 100 per page
        "from":     from_date,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "ok":
            print(f"  NewsAPI error for '{query}': {data.get('message')}")
            return []

        articles = data.get("articles", [])
        print(f"  NewsAPI '{query}' — {len(articles)} articles found")
        return articles

    except Exception as e:
        print(f"  NewsAPI exception for '{query}': {e}")
        return []


def classify_sentiment(title: str, description: str) -> str:
    """
    Very simple rule-based headline sentiment.
    Not ML — just keyword matching for a quick signal.
    In Milestone 2 we'll replace this with a proper NLP call.
    """
    text = f"{title} {description}".lower()

    positive_words = [
        "hit", "success", "record", "blockbuster", "acclaimed",
        "brilliant", "best", "win", "award", "love", "incredible",
        "fantastic", "stunning", "triumph", "masterpiece",
    ]
    negative_words = [
        "flop", "fail", "bomb", "disappointing", "worst", "disaster",
        "controversy", "boycott", "criticism", "bad", "terrible",
        "underperform", "struggle", "weak", "poor",
    ]

    pos_count = sum(1 for w in positive_words if w in text)
    neg_count = sum(1 for w in negative_words if w in text)

    if pos_count > neg_count:
        return "positive"
    elif neg_count > pos_count:
        return "negative"
    else:
        return "neutral"


def ingest() -> pd.DataFrame:
    """
    Main function called by the Airflow DAG.
    Returns a clean DataFrame ready for Snowflake loading.
    """
    print("Starting NewsAPI ingestion...")
    rows = []

    for i, movie_query in enumerate(MOVIE_TITLES):
        articles = fetch_articles(movie_query, max_articles=20)

        for article in articles:
            title       = article.get("title", "") or ""
            description = article.get("description", "") or ""

            rows.append({
                "movie_query":       movie_query,
                "article_title":     title,
                "article_url":       article.get("url"),
                "source_name":       article.get("source", {}).get("name"),
                "author":            article.get("author"),
                "description":       description[:500],  # truncate long descriptions
                "published_at":      article.get("publishedAt"),
                "sentiment":         classify_sentiment(title, description),
                "ingested_at":       datetime.utcnow(),
            })

        # Respect rate limits — free tier = 100 req/day
        time.sleep(0.5)

        if i % 5 == 0:
            print(f"  NewsAPI: {i+1}/{len(MOVIE_TITLES)} queries done")

    df = pd.DataFrame(rows)
    print(f"  NewsAPI ingestion complete — {len(df)} rows")
    return df


# ── Standalone test ────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = ingest()
    print(df[["movie_query", "source_name", "sentiment", "published_at"]].head(10))
    print(f"\nShape: {df.shape}")
    print(f"Sentiment breakdown:\n{df['sentiment'].value_counts()}")