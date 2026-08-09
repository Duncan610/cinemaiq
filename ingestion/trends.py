import time
import pandas as pd
from datetime import datetime
from pytrends.request import TrendReq
from loguru import logger


def get_client() -> TrendReq:
    return TrendReq(hl="en-US", tz=0, timeout=(10, 25), retries=3, backoff_factor=0.5)


def fetch_interest(keywords: list[str], timeframe: str = "today 12-m") -> pd.DataFrame:
    client = get_client()
    try:
        client.build_payload(keywords, timeframe=timeframe, geo="")
        df = client.interest_over_time()
        if df.empty:
            return pd.DataFrame()
        df = df.drop(columns=["isPartial"], errors="ignore").reset_index()
        df = df.melt(id_vars=["date"], var_name="keyword", value_name="interest_score")
        df["timeframe"] = timeframe
        df["ingested_at"] = datetime.utcnow().isoformat()
        return df
    except Exception as e:
        logger.error(f"Google Trends failed for {keywords}: {e}")
        return pd.DataFrame()


def run(movie_titles: list[str], timeframes: list[str] = None) -> pd.DataFrame:
    if timeframes is None:
        timeframes = ["today 12-m", "today 3-m"]
    queries = [f"{t} movie" for t in movie_titles]
    batches = [queries[i:i+5] for i in range(0, len(queries), 5)]

    all_dfs = []
    for tf in timeframes:
        for batch in batches:
            df = fetch_interest(batch, timeframe=tf)
            if not df.empty:
                all_dfs.append(df)
            time.sleep(3)

    if not all_dfs:
        return pd.DataFrame()
    result = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
    result["movie_title"] = result["keyword"].str.replace(" movie", "", regex=False).str.strip()
    logger.info(f"Google Trends ingestion complete: {len(result)} rows")
    return result


if __name__ == "__main__":
    df = run(["Oppenheimer", "Barbie"], timeframes=["today 12-m"])
    print(df.head(20))
    print(f"Shape: {df.shape}")