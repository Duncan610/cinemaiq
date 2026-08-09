import os, time, requests
import pandas as pd
from datetime import datetime
from loguru import logger

BASE_URL = "http://www.omdbapi.com/"


def get_api_key() -> str:
    key = os.getenv("OMDB_API_KEY")
    if not key:
        raise ValueError("OMDB_API_KEY not set")
    return key


def fetch_by_title(title: str, year: int = None) -> dict:
    params = {"apikey": get_api_key(), "t": title, "type": "movie", "plot": "short"}
    if year:
        params["y"] = year
    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("Response") == "False":
            logger.warning(f"OMDB no result: '{title}' ({year})")
            return {}
        return data
    except Exception as e:
        logger.error(f"OMDB fetch failed '{title}': {e}")
        return {}


def parse_rating(ratings_list, source):
    for r in ratings_list:
        if r.get("Source") == source:
            v = r.get("Value", "")
            try:
                if "%" in v: return float(v.replace("%", ""))
                if "/" in v: return float(v.split("/")[0])
                return float(v)
            except ValueError:
                return None
    return None


def flatten_to_row(raw: dict) -> dict:
    ratings = raw.get("Ratings", [])
    rt = parse_rating(ratings, "Rotten Tomatoes")
    mc = parse_rating(ratings, "Metacritic")
    try:
        imdb = float(raw.get("imdbRating", "N/A"))
    except (ValueError, TypeError):
        imdb = None
    critic_scores = [s for s in [rt, mc] if s is not None]
    avg_critic = round(sum(critic_scores) / len(critic_scores), 1) if critic_scores else None
    imdb_norm = round(imdb * 10, 1) if imdb else None
    gap = round(avg_critic - imdb_norm, 1) if (avg_critic and imdb_norm) else None
    bo_raw = raw.get("BoxOffice", "N/A")
    try:
        box_office = int(bo_raw.replace("$", "").replace(",", "")) if bo_raw != "N/A" else None
    except (ValueError, AttributeError):
        box_office = None
    return {
        "imdb_id": raw.get("imdbID"), "title": raw.get("Title"), "year": raw.get("Year"),
        "rated": raw.get("Rated"), "released": raw.get("Released"), "runtime": raw.get("Runtime"),
        "genre": raw.get("Genre"), "director": raw.get("Director"), "actors": raw.get("Actors"),
        "plot": raw.get("Plot"), "country": raw.get("Country"), "awards": raw.get("Awards"),
        "imdb_rating": imdb, "imdb_votes": (raw.get("imdbVotes") or "").replace(",", "") or None,
        "rt_score": rt, "metacritic_score": mc, "avg_critic_score": avg_critic,
        "imdb_normalised": imdb_norm, "critic_audience_gap": gap, "box_office_usd": box_office,
        "ingested_at": datetime.utcnow().isoformat(),
    }


def run(movie_list: list, delay: float = 0.25) -> pd.DataFrame:
    rows = []
    for movie in movie_list:
        raw = fetch_by_title(movie.get("title", ""), movie.get("year"))
        if raw:
            rows.append(flatten_to_row(raw))
        time.sleep(delay)
    df = pd.DataFrame(rows)
    logger.info(f"OMDB ingestion complete: {len(df)} rows")
    return df


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    df = run([{"title": "Oppenheimer", "year": 2023}, {"title": "Barbie", "year": 2023}])
    print(df[["title", "imdb_rating", "rt_score", "critic_audience_gap"]])