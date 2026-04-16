import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from loguru import logger
from datetime import datetime


def get_snowflake_connection(schema: str = "RAW"):
    """Return an authenticated Snowflake connection."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ.get("SNOWFLAKE_USER", "CINEMAIQ_SVC"),
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "CINEMAIQ"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "CINEMAIQ_LOADING_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "CINEMAIQ_ROLE"),
        schema=schema,
    )


def load_dataframe_to_snowflake(
    df: pd.DataFrame,
    table_name: str,
    truncate: bool = False,
) -> None:
    """
    Load a DataFrame into Snowflake RAW schema.
    Auto-creates the table from DataFrame columns — all VARCHAR.
    Type casting happens downstream in dbt Staging models.
    """
    if df.empty:
        logger.warning(f"Empty DataFrame — skipping load to {table_name}")
        return

    # Normalize column names to uppercase
    df.columns = [c.upper() for c in df.columns]
    df = df.astype(str).replace("nan", None).replace("None", None)

    conn = get_snowflake_connection(schema="RAW")

    try:
        cursor = conn.cursor()

        if truncate:
            # Drop and recreate — cleaner than truncate for schema changes
            cursor.execute(f"DROP TABLE IF EXISTS CINEMAIQ.RAW.{table_name}")
            logger.info(f"Dropped CINEMAIQ.RAW.{table_name} for full refresh")

        # write_pandas with auto_create_table=True handles DDL for us
        success, num_chunks, num_rows, _ = write_pandas(
            conn,
            df,
            table_name=table_name,
            database="CINEMAIQ",
            schema="RAW",
            auto_create_table=True,   # creates the table automatically
            quote_identifiers=False,
            overwrite=truncate,
        )

        if success:
            logger.info(f"Loaded {num_rows} rows into CINEMAIQ.RAW.{table_name}")
        else:
            logger.error(f"Load failed for {table_name}")

        cursor.close()

    except Exception as e:
        logger.error(f"Snowflake load error for {table_name}: {e}")
        raise
    finally:
        conn.close()


def run_load_audit(table_name: str, row_count: int, source: str) -> None:
    """Write a run record to RAW.PIPELINE_AUDIT — auto-creates if missing."""
    conn = get_snowflake_connection(schema="RAW")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CINEMAIQ.RAW.PIPELINE_AUDIT (
                RUN_ID      VARCHAR,
                TABLE_NAME  VARCHAR,
                SOURCE      VARCHAR,
                ROWS_LOADED INTEGER,
                LOADED_AT   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        cursor.execute(
            """
            INSERT INTO CINEMAIQ.RAW.PIPELINE_AUDIT
                (RUN_ID, TABLE_NAME, SOURCE, ROWS_LOADED)
            VALUES (%s, %s, %s, %s)
            """,
            (datetime.utcnow().strftime("%Y%m%d_%H%M%S"), table_name, source, row_count),
        )
        conn.commit()
        cursor.close()
        logger.info(f"Audit logged: {table_name} — {row_count} rows")
    except Exception as e:
        logger.error(f"Audit log failed: {e}")
    finally:
        conn.close()