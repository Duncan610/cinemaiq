import os, time, requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

BASE_URL = "https://newsapi.org/v2/everything"


def get_api_key() -> str:
    key = os.getenv("NEWSAPI_KEY")
    if not key:
        raise ValueError("NEWSAPI_KEY not set")
    return key


def fetch_movie_coverage(movie_title: str, days_back: int = 30, page_size: int = 20) -> list[dict]:
    from_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    query = f'"{movie_title}" (movie OR film OR cinema)'
    try:
        resp = requests.get(BASE_URL, params={
            "apiKey": get_api_key(), "q": query, "from": from_date,
            "sortBy": "relevancy", "pageSize": page_size, "language": "en",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "ok":
            logger.warning(f"NewsAPI error '{movie_title}': {data.get('message')}")
            return []
        return data.get("articles", [])
    except Exception as e:
        logger.error(f"NewsAPI fetch failed '{movie_title}': {e}")
        return []


def flatten_article(article: dict, movie_title: str) -> dict:
    source = article.get("source", {})
    title_l = (article.get("title") or "").lower()
    desc_l = (article.get("description") or "").lower()
    pos_words = ["hit", "success", "brilliant", "stunning", "masterpiece", "amazing", "praised", "acclaimed", "triumph", "blockbuster"]
    neg_words = ["flop", "disappointing", "fails", "poor", "disaster", "terrible", "bomb", "worst", "controversy", "backlash"]
    text = f"{title_l} {desc_l}"
    pos_hits = sum(1 for w in pos_words if w in text)
    neg_hits = sum(1 for w in neg_words if w in text)
    return {
        "movie_query": movie_title, "article_id": hash(f"{movie_title}_{article.get('url','')}"),
        "source_name": source.get("name"), "author": article.get("author"),
        "headline": article.get("title"), "description": (article.get("description") or "")[:500],
        "url": article.get("url"), "published_at": (article.get("publishedAt") or "")[:10],
        "positive_hits": pos_hits, "negative_hits": neg_hits,
        "sentiment_score": pos_hits - neg_hits, "ingested_at": datetime.utcnow().isoformat(),
    }


def run(movie_titles: list[str], days_back: int = 30, delay: float = 1.0) -> pd.DataFrame:
    rows = []
    for title in movie_titles:
        for article in fetch_movie_coverage(title, days_back=days_back):
            rows.append(flatten_article(article, title))
        time.sleep(delay)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["url"])
    logger.info(f"NewsAPI ingestion complete: {len(df)} articles")
    return df


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    df = run(["Oppenheimer", "Barbie"], days_back=30)
    print(df[["movie_query", "source_name", "headline", "sentiment_score"]].head(10))