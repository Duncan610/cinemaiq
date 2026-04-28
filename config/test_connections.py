"""
test_connections.py
───────────────────
Run BEFORE starting Airflow to confirm all 4 connections work.

Usage:
    cd config
    python test_connections.py
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

passed = 0
failed = 0


def check(name: str, fn):
    global passed, failed
    try:
        fn()
        print(f"  ✓ PASS — {name}")
        passed += 1
    except Exception as e:
        print(f"  ✗ FAIL — {name}")
        print(f"           {e}")
        failed += 1


# ── Test 1: TMDB API ──────────────────────────────────────────────────────
def test_tmdb():
    import requests
    key = os.getenv("TMDB_API_KEY")
    assert key, "TMDB_API_KEY not set in .env"
    r = requests.get(
        "https://api.themoviedb.org/3/movie/popular",
        params={"api_key": key, "page": 1},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    assert len(data.get("results", [])) > 0, "TMDB returned empty results"

check("TMDB API", test_tmdb)


# ── Test 2: OMDB API ──────────────────────────────────────────────────────
def test_omdb():
    import requests
    key = os.getenv("OMDB_API_KEY")
    assert key, "OMDB_API_KEY not set in .env"
    r = requests.get(
        "http://www.omdbapi.com/",
        params={"apikey": key, "t": "Oppenheimer", "type": "movie"},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    assert data.get("Response") == "True", f"OMDB error: {data.get('Error')}"
    assert data.get("Title"), "OMDB returned no title"

check("OMDB API", test_omdb)


# ── Test 3: NewsAPI ───────────────────────────────────────────────────────
def test_newsapi():
    import requests
    key = os.getenv("NEWSAPI_KEY")
    assert key, "NEWSAPI_KEY not set in .env"
    r = requests.get(
        "https://newsapi.org/v2/everything",
        params={
            "apiKey": key,
            "q": "Oppenheimer movie",
            "pageSize": 1,
            "language": "en",
        },
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    assert data.get("status") == "ok", f"NewsAPI error: {data.get('message')}"

check("NewsAPI", test_newsapi)


# ── Test 4: Snowflake ─────────────────────────────────────────────────────
def test_snowflake():
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
    row = cursor.fetchone()
    assert row[0], "No current user returned"
    assert row[1] == "CINEMAIQ", f"Wrong database: {row[1]}"
    conn.close()

check("Snowflake", test_snowflake)


# ── Summary ───────────────────────────────────────────────────────────────
print(f"\n  Results: {passed} passed, {failed} failed")

if failed > 0:
    print("\n  Fix the FAILs above before running the DAG.")
    print("  Most common causes:")
    print("    - .env file not filled in (copy from .env.example)")
    print("    - Snowflake ACCOUNT format wrong (should be like: abc12345.us-east-1)")
    print("    - Snowflake setup SQL not yet run")
    sys.exit(1)
else:
    print("\n  All connections working. You're ready to run the DAG.")