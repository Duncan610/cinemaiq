import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
from loguru import logger


def get_connection(schema: str = "RAW"):
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ.get("SNOWFLAKE_USER", "CINEMAIQ_SVC"),
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "CINEMAIQ"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CINEMAIQ_LOADING_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "CINEMAIQ_ROLE"),
        schema=schema,
    )


def load_to_snowflake(df: pd.DataFrame, table_name: str, truncate: bool = False) -> int:
    if df.empty:
        logger.warning(f"Empty DataFrame — skipping load to {table_name}")
        return 0

    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    df = df.astype(str).replace("nan", None).replace("None", None)

    conn = get_connection(schema="RAW")
    try:
        if truncate:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS CINEMAIQ.RAW.{table_name}")
            cur.close()

        success, _, num_rows, _ = write_pandas(
            conn, df, table_name=table_name, database="CINEMAIQ", schema="RAW",
            auto_create_table=True, overwrite=truncate, quote_identifiers=False,
        )
        if success:
            logger.info(f"Loaded {num_rows:,} rows -> CINEMAIQ.RAW.{table_name}")
        return num_rows
    finally:
        conn.close()


def log_audit(table_name: str, row_count: int, source: str) -> None:
    conn = get_connection(schema="RAW")
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS CINEMAIQ.RAW.PIPELINE_AUDIT (
                RUN_ID VARCHAR, TABLE_NAME VARCHAR, SOURCE VARCHAR,
                ROWS_LOADED INTEGER, LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        cur.execute(
            "INSERT INTO CINEMAIQ.RAW.PIPELINE_AUDIT (RUN_ID, TABLE_NAME, SOURCE, ROWS_LOADED) VALUES (%s,%s,%s,%s)",
            (datetime.utcnow().strftime("%Y%m%d_%H%M%S"), table_name, source, row_count),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()