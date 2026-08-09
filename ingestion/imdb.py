import argparse, gzip
import pandas as pd
from datetime import datetime
from loguru import logger


def load_basics(path: str, start_year: int = 2000) -> pd.DataFrame:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        df = pd.read_csv(f, sep="\t", na_values=["\\N"], dtype=str, low_memory=False)
    df = df[df["titleType"] == "movie"]
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce")
    df = df[df["startYear"] >= start_year]
    df = df[df["isAdult"] == "0"]
    df = df.rename(columns={
        "tconst": "imdb_id", "titleType": "title_type", "primaryTitle": "primary_title",
        "originalTitle": "original_title", "isAdult": "is_adult", "startYear": "release_year",
        "endYear": "end_year", "runtimeMinutes": "runtime_minutes", "genres": "genres",
    })
    df["ingested_at"] = datetime.utcnow().isoformat()
    logger.info(f"Basics filtered shape: {df.shape}")
    return df


def load_ratings(path: str) -> pd.DataFrame:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        df = pd.read_csv(f, sep="\t", na_values=["\\N"], dtype=str)
    df = df.rename(columns={"tconst": "imdb_id", "averageRating": "imdb_rating", "numVotes": "num_votes"})
    df["ingested_at"] = datetime.utcnow().isoformat()
    logger.info(f"Ratings shape: {df.shape}")
    return df


def run(basics_path: str, ratings_path: str):
    return load_basics(basics_path), load_ratings(ratings_path)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    from snowflake_loader import load_to_snowflake, log_audit

    parser = argparse.ArgumentParser()
    parser.add_argument("--basics", required=True)
    parser.add_argument("--ratings", required=True)
    args = parser.parse_args()

    basics, ratings = run(args.basics, args.ratings)
    print(basics.head(3).to_string())
    print(ratings.head(3).to_string())

    if input("Load to Snowflake? (yes/no): ").strip().lower() == "yes":
        load_to_snowflake(basics, "RAW_IMDB_BASICS", truncate=True)
        load_to_snowflake(ratings, "RAW_IMDB_RATINGS", truncate=True)
        log_audit("RAW_IMDB_BASICS", len(basics), "IMDB_LOCAL")
        log_audit("RAW_IMDB_RATINGS", len(ratings), "IMDB_LOCAL")
        print("Done!")