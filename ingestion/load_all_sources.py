import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from loader import load_to_snowflake, log_audit

TARGET_YEARS = [2019, 2020, 2021, 2022, 2023, 2024]
MOVIE_LIST = [
    {"title": "Oppenheimer", "year": 2023}, {"title": "Barbie", "year": 2023},
    {"title": "Top Gun: Maverick", "year": 2022}, {"title": "Avatar: The Way of Water", "year": 2022},
    {"title": "Everything Everywhere All at Once", "year": 2022}, {"title": "The Batman", "year": 2022},
    {"title": "Spider-Man: No Way Home", "year": 2021}, {"title": "No Time to Die", "year": 2021},
    {"title": "Tenet", "year": 2020}, {"title": "Parasite", "year": 2019},
    {"title": "Avengers: Endgame", "year": 2019}, {"title": "Joker", "year": 2019},
]
MOVIE_TITLES = [m["title"] for m in MOVIE_LIST]


def load_tmdb():
    print("\n=== TMDB ===")
    from tmdb import run
    df = run(years=TARGET_YEARS, pages_per_year=5)
    n = load_to_snowflake(df, "RAW_TMDB_MOVIES", truncate=True)
    log_audit("RAW_TMDB_MOVIES", n, "TMDB_API")
    print(f"Loaded {n} rows")


def load_omdb():
    print("\n=== OMDB ===")
    from omdb import run
    df = run(MOVIE_LIST)
    n = load_to_snowflake(df, "RAW_OMDB_RATINGS", truncate=True)
    log_audit("RAW_OMDB_RATINGS", n, "OMDB_API")
    print(f"Loaded {n} rows")


def load_newsapi():
    print("\n=== NewsAPI ===")
    from news_api import run
    df = run(MOVIE_TITLES, days_back=30)
    n = load_to_snowflake(df, "RAW_NEWS_ARTICLES", truncate=True)
    log_audit("RAW_NEWS_ARTICLES", n, "NEWSAPI")
    print(f"Loaded {n} rows")


def load_box_office():
    print("\n=== Box Office Mojo ===")
    from mojo import run
    df = run(start_year=2019, end_year=2024)
    n = load_to_snowflake(df, "RAW_BOX_OFFICE", truncate=True)
    log_audit("RAW_BOX_OFFICE", n, "BOX_OFFICE_MOJO")
    print(f"Loaded {n} rows")


def load_trends():
    print("\n=== Google Trends ===")
    from trends import run
    df = run(movie_titles=MOVIE_TITLES, timeframes=["today 12-m", "today 3-m"])
    n = load_to_snowflake(df, "RAW_GOOGLE_TRENDS", truncate=True)
    log_audit("RAW_GOOGLE_TRENDS", n, "GOOGLE_TRENDS")
    print(f"Loaded {n} rows")


if __name__ == "__main__":
    print("Loading all 5 remaining sources to Snowflake RAW...")
    print("(IMDb already loaded separately)\n")

    results = {}
    for name, fn in [
        ("TMDB", load_tmdb),
        ("OMDB", load_omdb),
        ("NewsAPI", load_newsapi),
        ("Box Office", load_box_office),
        ("Google Trends", load_trends),
    ]:
        try:
            fn()
            results[name] = "OK"
        except Exception as e:
            print(f"FAILED: {name} — {e}")
            results[name] = f"FAILED: {e}"

    print("\n" + "=" * 50)
    print("Summary")
    print("=" * 50)
    for name, status in results.items():
        print(f"  {name:15s} {status}")