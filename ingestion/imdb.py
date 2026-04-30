"""
imdb_ingestor.py
────────────────
Loads IMDb's free public dataset files into Snowflake.

Why IMDb separately from OMDB?
  OMDB gives us IMDb ratings for ~50 specific movies we query.
  IMDb's own dataset gives us ratings for MILLIONS of movies —
  the full historical catalogue. This enables:
    - Genre-level benchmarks ("is 7.2 good for a horror film?")
    - Director/actor historical performance
    - Decade-level trend analysis

Where to get the files (FREE, no login):
  https://datasets.imdbws.com/
  - title.basics.tsv.gz   (movie metadata: title, year, genre, runtime)
  - title.ratings.tsv.gz  (average rating + vote count per movie)

  Download both, put them anywhere on your machine,
  then run: python imdb_ingestor.py --basics /path/to/title.basics.tsv.gz
                                    --ratings /path/to/title.ratings.tsv.gz

This script is designed to be run ONCE manually (not via Airflow DAG)
because the files are large (~1GB) and IMDb only updates them weekly.
The DAG handles the other 5 live API sources.

Returns: two DataFrames — one for basics, one for ratings.
         The loader will write them as separate tables in Snowflake RAW.
"""

import argparse
import pandas as pd
from datetime import datetime


def load_basics(filepath: str, movie_types_only: bool = True) -> pd.DataFrame:
    """
    Loads title.basics.tsv.gz — metadata for all IMDb titles.

    Filters to movies only (drops TV shows, shorts, episodes etc.)
    and to English-language titles to keep the dataset manageable.

    Args:
        filepath: path to title.basics.tsv.gz
        movie_types_only: if True, only keep titleType == 'movie'
    """
    print(f"  Loading IMDb basics from: {filepath}")

    df = pd.read_csv(
        filepath,
        sep="\t",
        compression="gzip",
        low_memory=False,
        na_values=["\\N"],   # IMDb uses \N for NULL
    )

    print(f"  Raw basics rows: {len(df):,}")

    # Filter to movies only
    if movie_types_only:
        df = df[df["titleType"] == "movie"].copy()
        print(f"  After movie filter: {len(df):,}")

    # Rename to snake_case
    df = df.rename(columns={
        "tconst":          "imdb_id",
        "titleType":       "title_type",
        "primaryTitle":    "primary_title",
        "originalTitle":   "original_title",
        "isAdult":         "is_adult",
        "startYear":       "start_year",
        "endYear":         "end_year",
        "runtimeMinutes":  "runtime_minutes",
        "genres":          "genres",
    })

    # Type conversions
    df["start_year"]      = pd.to_numeric(df["start_year"],      errors="coerce")
    df["runtime_minutes"] = pd.to_numeric(df["runtime_minutes"], errors="coerce")
    df["is_adult"]        = df["is_adult"].map({"0": False, "1": True})

    df["ingested_at"] = datetime.utcnow()

    print(f"  IMDb basics final shape: {df.shape}")
    return df


def load_ratings(filepath: str) -> pd.DataFrame:
    """
    Loads title.ratings.tsv.gz — average rating + vote count per title.

    Args:
        filepath: path to title.ratings.tsv.gz
    """
    print(f"  Loading IMDb ratings from: {filepath}")

    df = pd.read_csv(
        filepath,
        sep="\t",
        compression="gzip",
        na_values=["\\N"],
    )

    print(f"  Raw ratings rows: {len(df):,}")

    df = df.rename(columns={
        "tconst":        "imdb_id",
        "averageRating": "average_rating",
        "numVotes":      "num_votes",
    })

    df["ingested_at"] = datetime.utcnow()

    print(f"  IMDb ratings final shape: {df.shape}")
    return df


def ingest(basics_path: str, ratings_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads both files and previews them before asking for confirmation.
    Returns (basics_df, ratings_df).
    """
    basics_df  = load_basics(basics_path)
    ratings_df = load_ratings(ratings_path)

    print("\n── Preview: IMDb Basics ──")
    print(basics_df[["imdb_id", "primary_title", "start_year", "genres", "runtime_minutes"]].head(5).to_string())

    print("\n── Preview: IMDb Ratings ──")
    print(ratings_df.head(5).to_string())

    confirm = input("\nLoad both tables to Snowflake RAW? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("Aborted. Nothing was loaded to Snowflake.")
        return pd.DataFrame(), pd.DataFrame()

    return basics_df, ratings_df


# ── CLI entry point ────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load IMDb TSV files to Snowflake")
    parser.add_argument("--basics",  required=True, help="Path to title.basics.tsv.gz")
    parser.add_argument("--ratings", required=True, help="Path to title.ratings.tsv.gz")
    args = parser.parse_args()

    basics_df, ratings_df = ingest(args.basics, args.ratings)

    if not basics_df.empty:
        # Import here to avoid circular imports when called from DAG
        from snowflake_loader import load_to_snowflake
        load_to_snowflake(basics_df,  table_name="IMDB_BASICS")
        load_to_snowflake(ratings_df, table_name="IMDB_RATINGS")
        print("IMDb data loaded to Snowflake successfully.")