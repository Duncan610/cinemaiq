import time, requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from loguru import logger

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}
BASE_URL = "https://www.boxofficemojo.com"


def clean_number(text: str):
    cleaned = text.replace("$", "").replace(",", "").strip()
    return int(cleaned) if cleaned.lstrip("-").isdigit() else None


def scrape_year(year: int) -> list[dict]:
    url = f"{BASE_URL}/year/world/{year}/"
    logger.info(f"Scraping: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    data_rows = table.find_all("tr")[1:]  # skip header, no tbody wrapper
    movies = []
    for row in data_rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        try:
            rank_text = cols[0].get_text(strip=True)
            title_text = cols[1].get_text(strip=True)
            link = cols[1].find("a")
            detail_url = BASE_URL + link["href"] if link else None
            movies.append({
                "rank": int(rank_text) if rank_text.isdigit() else None,
                "title": title_text, "year": year,
                "worldwide_gross": clean_number(cols[2].get_text(strip=True)),
                "domestic_gross": clean_number(cols[3].get_text(strip=True)),
                "opening_weekend": clean_number(cols[4].get_text(strip=True)) if len(cols) > 4 else None,
                "release_date": cols[5].get_text(strip=True) if len(cols) > 5 else None,
                "detail_url": detail_url, "scraped_at": datetime.utcnow().isoformat(),
            })
        except Exception as e:
            logger.warning(f"Row parse error year={year}: {e}")
    logger.info(f"  -> {len(movies)} movies for {year}")
    return movies


def run(start_year: int = 2019, end_year: int = 2024, delay: float = 2.5) -> pd.DataFrame:
    all_movies = []
    for year in range(start_year, end_year + 1):
        all_movies.extend(scrape_year(year))
        time.sleep(delay)
    df = pd.DataFrame(all_movies)
    logger.info(f"Box Office ingestion complete: {len(df)} rows")
    return df


if __name__ == "__main__":
    df = run(start_year=2022, end_year=2023)
    print(df.head(10).to_string())
    print(f"Shape: {df.shape}")