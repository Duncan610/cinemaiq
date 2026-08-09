import os, time, requests
import pandas as pd
from datetime import datetime
from loguru import logger

BASE_URL = "https://api.themoviedb.org/3"


def get_api_key() -> str:
    key = os.getenv("TMDB_API_KEY")
    if not key:
        raise ValueError("TMDB_API_KEY not set")
    return key


def fetch_movies_by_year(year: int, pages: int = 5) -> list[dict]:
    movies = []
    for page in range(1, pages + 1):
        try:
            resp = requests.get(f"{BASE_URL}/discover/movie", params={
                "api_key": get_api_key(), "primary_release_year": year,
                "sort_by": "revenue.desc", "page": page, "language": "en-US",
            }, timeout=10)
            resp.raise_for_status()
            movies.extend(resp.json().get("results", []))
        except Exception as e:
            logger.error(f"TMDB year={year} page={page} failed: {e}")
        time.sleep(0.3)
    return movies


def fetch_movie_details(movie_id: int) -> dict:
    try:
        resp = requests.get(f"{BASE_URL}/movie/{movie_id}", params={
            "api_key": get_api_key(), "append_to_response": "credits,keywords",
        }, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"TMDB detail failed id={movie_id}: {e}")
        return {}


def flatten_to_row(raw: dict) -> dict:
    genres = [g["name"] for g in raw.get("genres", [])]
    cast = raw.get("credits", {}).get("cast", [])
    crew = raw.get("credits", {}).get("crew", [])
    directors = [p["name"] for p in crew if p.get("job") == "Director"]
    keywords = [k["name"] for k in raw.get("keywords", {}).get("keywords", [])]
    return {
        "tmdb_id": raw.get("id"), "title": raw.get("title"),
        "original_title": raw.get("original_title"), "overview": raw.get("overview"),
        "tagline": raw.get("tagline"), "status": raw.get("status"),
        "release_date": raw.get("release_date"), "runtime_minutes": raw.get("runtime"),
        "budget": raw.get("budget"), "revenue": raw.get("revenue"),
        "popularity": raw.get("popularity"), "vote_average": raw.get("vote_average"),
        "vote_count": raw.get("vote_count"), "original_language": raw.get("original_language"),
        "genres": "|".join(genres), "directors": "|".join(directors),
        "top_cast": "|".join([c["name"] for c in cast[:5]]), "cast_size": len(cast),
        "keywords": "|".join(keywords[:10]), "ingested_at": datetime.utcnow().isoformat(),
    }


def run(years: list[int], pages_per_year: int = 5) -> pd.DataFrame:
    all_rows, seen_ids = [], set()
    for year in years:
        for movie in fetch_movies_by_year(year, pages=pages_per_year):
            mid = movie.get("id")
            if not mid or mid in seen_ids:
                continue
            seen_ids.add(mid)
            detail = fetch_movie_details(mid)
            if detail:
                all_rows.append(flatten_to_row(detail))
    df = pd.DataFrame(all_rows)
    logger.info(f"TMDB ingestion complete: {len(df)} movies")
    return df


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    df = run(years=[2023], pages_per_year=2)
    print(df[["tmdb_id", "title", "release_date", "revenue"]].head(10))
    print(f"Shape: {df.shape}")