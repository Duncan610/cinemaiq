import argparse, gzip
import pandas as pd
from datetime import datetime
from loguru import logger
from loader import load_to_snowflake, log_audit


def load_basics(path: str, start_year: int = 2000, chunksize: int = 200_000) -> pd.DataFrame:
    """
    Memory-safe version: reads the file in chunks instead of all at once.
    Filters each chunk immediately, keeping only what we need in memory.
    Only pulls the columns we actually use — skips the rest.
    """
    opener = gzip.open if path.endswith(".gz") else open
    usecols = ["tconst", "titleType", "primaryTitle", "originalTitle",
               "isAdult", "startYear", "runtimeMinutes", "genres"]

    kept_chunks = []
    total_seen = 0

    with opener(path, "rt", encoding="utf-8") as f:
        reader = pd.read_csv(
            f, sep="\t", na_values=["\\N"], dtype=str,
            usecols=usecols, chunksize=chunksize, low_memory=False,
        )
        for i, chunk in enumerate(reader):
            total_seen += len(chunk)
            chunk = chunk[chunk["titleType"] == "movie"]
            chunk["startYear"] = pd.to_numeric(chunk["startYear"], errors="coerce")
            chunk = chunk[chunk["startYear"] >= start_year]
            chunk = chunk[chunk["isAdult"] == "0"]
            if not chunk.empty:
                kept_chunks.append(chunk)
            if i % 10 == 0:
                logger.info(f"  processed {total_seen:,} rows so far, kept {sum(len(c) for c in kept_chunks):,}")

    df = pd.concat(kept_chunks, ignore_index=True)
    df = df.rename(columns={
        "tconst": "imdb_id", "titleType": "title_type", "primaryTitle": "primary_title",
        "originalTitle": "original_title", "isAdult": "is_adult", "startYear": "release_year",
        "runtimeMinutes": "runtime_minutes", "genres": "genres",
    })
    df["ingested_at"] = datetime.utcnow().isoformat()
    logger.info(f"Basics final shape: {df.shape}  (filtered from {total_seen:,} total rows)")
    return df


def load_ratings(path: str, chunksize: int = 200_000) -> pd.DataFrame:
    """
    Ratings file is much smaller (~1.5M rows) but chunk it anyway for safety.
    """
    opener = gzip.open if path.endswith(".gz") else open
    chunks = []
    with opener(path, "rt", encoding="utf-8") as f:
        reader = pd.read_csv(f, sep="\t", na_values=["\\N"], dtype=str, chunksize=chunksize)
        for chunk in reader:
            chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    df = df.rename(columns={"tconst": "imdb_id", "averageRating": "imdb_rating", "numVotes": "num_votes"})
    df["ingested_at"] = datetime.utcnow().isoformat()
    logger.info(f"Ratings shape: {df.shape}")
    return df


def run(basics_path: str, ratings_path: str):
    return load_basics(basics_path), load_ratings(ratings_path)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

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