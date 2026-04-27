"""
box_office_ingestor.py
──────────────────────
Scrapes weekly domestic box office results from Box Office Mojo.

Why Box Office Mojo?
  This is the ground truth for movie financial performance.
  It's the Y variable in our entire project — the thing we're
  trying to predict/explain. Revenue data tells us:
    - Opening weekend gross (the most-watched metric)
    - Weekend rank (competitive context)
    - Week-over-week drop % (audience retention / word of mouth)
    - Total cumulative gross

  This is a PUBLIC web page — no API key needed.
  We use BeautifulSoup to parse the HTML table.

Note on scraping ethics:
  Box Office Mojo is owned by IMDb/Amazon. We scrape politely:
  - One request at a time
  - User-Agent header so we don't look like a bot
  - No hammering — we get weekly data, not per-minute

Returns: a pandas DataFrame with one row per movie per week.
"""

import time
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# Box Office Mojo weekly chart — shows top ~200 movies each week
BASE_URL = "https://www.boxofficemojo.com/weekly/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_weekly_chart(url: str = BASE_URL) -> list[dict]:
    """
    Scrapes the current weekly box office chart.
    Returns a list of dicts, one per movie.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"  Box Office Mojo fetch error: {e}")
        return []

    soup = BeautifulSoup(response.text, "lxml")

    # The main data table
    table = soup.find("table")
    if not table:
        print("  Box Office Mojo: could not find data table")
        return []

    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return []

    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 9:
            continue

        try:
            # Clean dollar amounts: '$12,345,678' → 12345678
            def clean_money(s: str) -> int | None:
                s = s.strip().replace("$", "").replace(",", "")
                return int(s) if s.isdigit() else None

            # Clean percentage: '-42.3%' → -42.3
            def clean_pct(s: str) -> float | None:
                s = s.strip().replace("%", "").replace("+", "")
                try:
                    return float(s)
                except ValueError:
                    return None

            rows.append({
                "rank":               int(cells[0].get_text(strip=True)) if cells[0].get_text(strip=True).isdigit() else None,
                "rank_last_week":     int(cells[1].get_text(strip=True)) if cells[1].get_text(strip=True).isdigit() else None,
                "title":              cells[2].get_text(strip=True),
                "studio":             cells[3].get_text(strip=True),
                "weekly_gross_usd":   clean_money(cells[4].get_text(strip=True)),
                "pct_change_week":    clean_pct(cells[5].get_text(strip=True)),
                "theatres":           clean_money(cells[6].get_text(strip=True)),
                "per_theatre_usd":    clean_money(cells[7].get_text(strip=True)),
                "total_gross_usd":    clean_money(cells[8].get_text(strip=True)),
                "weeks_in_release":   int(cells[9].get_text(strip=True)) if len(cells) > 9 and cells[9].get_text(strip=True).isdigit() else None,
            })
        except Exception as e:
            # Skip malformed rows silently
            continue

    return rows


def ingest() -> pd.DataFrame:
    """
    Main function called by the Airflow DAG.
    Returns a clean DataFrame ready for Snowflake loading.
    """
    print("Starting Box Office Mojo ingestion...")

    rows = fetch_weekly_chart()

    if not rows:
        print("  Warning: no rows returned from Box Office Mojo")
        # Return empty DataFrame with correct schema so the loader doesn't break
        return pd.DataFrame(columns=[
            "rank", "rank_last_week", "title", "studio",
            "weekly_gross_usd", "pct_change_week", "theatres",
            "per_theatre_usd", "total_gross_usd", "weeks_in_release",
            "scraped_date", "ingested_at",
        ])

    df = pd.DataFrame(rows)
    df["scraped_date"] = datetime.utcnow().date()
    df["ingested_at"]  = datetime.utcnow()

    print(f"  Box Office ingestion complete — {len(df)} rows")
    return df


# ── Standalone test ────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = ingest()
    print(df[["rank", "title", "weekly_gross_usd", "total_gross_usd"]].head(10))
    print(f"\nShape: {df.shape}")