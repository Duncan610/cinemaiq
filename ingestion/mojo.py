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


Returns: a pandas DataFrame with one row per movie per week.
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/",
}

BASE_URL = "https://www.boxofficemojo.com"


def scrape_yearly_chart(year: int) -> list[dict]:
    """
    Scrape Box Office Mojo yearly world chart for a given year.
    Handles tables with or without a <tbody> wrapper.
    """
    url = f"{BASE_URL}/year/world/{year}/"
    logger.info(f"Scraping: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")
    if not table:
        logger.warning(f"No table found for year {year}")
        return []

    # BOM does not always use <tbody> — get ALL rows, skip first (header)
    all_rows = table.find_all("tr")
    data_rows = all_rows[1:]

    if not data_rows:
        logger.warning(f"No data rows found for year {year}")
        return []

    movies = []

    def clean_number(text):
        cleaned = text.replace("$", "").replace(",", "").strip()
        return int(cleaned) if cleaned.lstrip("-").isdigit() else None

    for row in data_rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        try:
            rank_text  = cols[0].get_text(strip=True)
            title_text = cols[1].get_text(strip=True)
            title_link = cols[1].find("a")
            detail_url = BASE_URL + title_link["href"] if title_link else None

            worldwide_gross = clean_number(cols[2].get_text(strip=True))
            domestic_gross  = clean_number(cols[3].get_text(strip=True))
            opening_weekend = clean_number(cols[4].get_text(strip=True)) if len(cols) > 4 else None
            release_date    = cols[5].get_text(strip=True)               if len(cols) > 5 else None

            movies.append({
                "rank":            int(rank_text) if rank_text.isdigit() else None,
                "title":           title_text,
                "year":            year,
                "worldwide_gross": worldwide_gross,
                "domestic_gross":  domestic_gross,
                "opening_weekend": opening_weekend,
                "release_date":    release_date,
                "detail_url":      detail_url,
                "scraped_at":      datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"Row parse error (year={year}): {e}")
            continue

    logger.info(f"  -> {len(movies)} movies scraped for {year}")
    return movies


def scrape_movie_budget(detail_url: str) -> dict:
    """Visit individual movie page to get production budget."""
    if not detail_url:
        return {}
    try:
        resp = requests.get(detail_url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Detail page error {detail_url}: {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    for div in soup.find_all("div", class_=lambda c: c and "mojo-summary" in c):
        label = div.find("span", string=lambda s: s and "Production Budget" in s)
        if label:
            value = label.find_next("span")
            if value:
                raw = value.get_text(strip=True).replace("$", "").replace(",", "")
                return {"production_budget": int(raw) if raw.isdigit() else None}
    return {"production_budget": None}


def scrape_years(
    start_year: int,
    end_year: int,
    delay: float = 2.5,
    enrich_budget: bool = False,
) -> pd.DataFrame:
    """Scrape multiple years. enrich_budget=True hits individual pages for budget."""
    all_movies = []
    for year in range(start_year, end_year + 1):
        movies = scrape_yearly_chart(year)
        if enrich_budget:
            for movie in movies:
                movie.update(scrape_movie_budget(movie.get("detail_url")))
                time.sleep(delay)
        else:
            time.sleep(delay)
        all_movies.extend(movies)

    df = pd.DataFrame(all_movies)
    logger.info(f"Total movies collected: {len(df)}")
    return df


def save_to_csv(df: pd.DataFrame, output_path: str) -> None:
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} rows -> {output_path}")


if __name__ == "__main__":
    df = scrape_years(start_year=2022, end_year=2023, delay=2.5)

    if df.empty:
        print("No data returned — check logs above.")
    else:
        print(df.head(10).to_string())
        print(f"\nShape:   {df.shape}")
        print(f"Columns: {list(df.columns)}")
        save_to_csv(df, "box_office_mojo_raw.csv")