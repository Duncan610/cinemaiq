"""
cinemiq_bronze_dag.py
─────────────────────
The CinemaIQ Bronze Layer DAG.


This DAG runs daily at 6am UTC and:
  1. Runs 5 ingestors IN PARALLEL (they don't depend on each other)
  2. Each ingestor loads its DataFrame to Snowflake RAW
  3. A final audit task records row counts

Task names map directly to sources:
  ingest_tmdb          → RAW_TMDB_MOVIES
  ingest_omdb          → RAW_OMDB_RATINGS
  ingest_newsapi       → RAW_NEWS_ARTICLES
  ingest_box_office    → RAW_BOX_OFFICE
  ingest_google_trends → RAW_GOOGLE_TRENDS

IMDb is NOT in the DAG because it's a weekly manual file download,
not a live API. Run imdb_ingestor.py manually once from the CLI.
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add /opt/airflow/ingestion to Python path so Airflow can find our scripts
# (The docker-compose.yml mounts ./ingestion to /opt/airflow/ingestion)
sys.path.insert(0, "/opt/airflow/ingestion")

# ── Default arguments applied to every task ───────────────────────────────
default_args = {
    "owner": "cinemiq",
    "depends_on_past": False,           # don't wait for yesterday's run to succeed
    "email_on_failure": False,          # no email alerts (configure later)
    "email_on_retry": False,
    "retries": 2,                       # retry failed tasks twice
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ── DAG definition ─────────────────────────────────────────────────────────
dag = DAG(
    dag_id="cinemiq_bronze_ingestion",
    description="Daily Bronze layer: pull from 5 sources → Snowflake RAW",
    schedule_interval="0 6 * * *",      # 6am UTC every day
    start_date=datetime(2024, 1, 1),
    catchup=False,                      # don't backfill missed runs
    tags=["cinemiq", "bronze", "ingestion"],
    default_args=default_args,
    max_active_runs=1,                  # prevent overlapping DAG runs
)


# ── Task functions ─────────────────────────────────────────────────────────
# Each function imports its ingestor, runs it, and loads to Snowflake.
# The import is inside the function so Airflow only imports what it needs
# when the task actually runs (not at DAG parse time).

def run_tmdb(**context):
    from tmdb_ingestor import ingest
    from snowflake_loader import load_to_snowflake, verify_table

    df = ingest()
    rows = load_to_snowflake(df, table_name="TMDB_MOVIES")
    verify_table("TMDB_MOVIES")

    # Push row count to XCom so the audit task can read it
    context["ti"].xcom_push(key="rows_loaded", value=rows)
    return rows


def run_omdb(**context):
    from omdb_ingestor import ingest
    from snowflake_loader import load_to_snowflake, verify_table

    df = ingest()
    rows = load_to_snowflake(df, table_name="OMDB_RATINGS")
    verify_table("OMDB_RATINGS")

    context["ti"].xcom_push(key="rows_loaded", value=rows)
    return rows


def run_newsapi(**context):
    from newsapi_ingestor import ingest
    from snowflake_loader import load_to_snowflake, verify_table

    df = ingest()
    rows = load_to_snowflake(df, table_name="NEWS_ARTICLES")
    verify_table("NEWS_ARTICLES")

    context["ti"].xcom_push(key="rows_loaded", value=rows)
    return rows


def run_box_office(**context):
    from box_office_ingestor import ingest
    from snowflake_loader import load_to_snowflake, verify_table

    df = ingest()
    rows = load_to_snowflake(df, table_name="BOX_OFFICE")
    verify_table("BOX_OFFICE")

    context["ti"].xcom_push(key="rows_loaded", value=rows)
    return rows


def run_google_trends(**context):
    from google_trends_ingestor import ingest
    from snowflake_loader import load_to_snowflake, verify_table

    df = ingest()
    rows = load_to_snowflake(df, table_name="GOOGLE_TRENDS")
    verify_table("GOOGLE_TRENDS")

    context["ti"].xcom_push(key="rows_loaded", value=rows)
    return rows


def run_audit(**context):
    """
    Reads row counts from all 5 tasks via XCom and logs a summary.
    This is the final task — it only runs after all 5 ingestors succeed.
    """
    ti = context["ti"]
    sources = {
        "TMDB":          "ingest_tmdb",
        "OMDB":          "ingest_omdb",
        "NewsAPI":       "ingest_newsapi",
        "Box Office":    "ingest_box_office",
        "Google Trends": "ingest_google_trends",
    }

    print("\n══════════════════════════════════════")
    print("  CinemaIQ Bronze DAG — Audit Summary")
    print(f"  Run date: {context['ds']}")
    print("══════════════════════════════════════")

    total = 0
    for source_name, task_id in sources.items():
        rows = ti.xcom_pull(task_ids=task_id, key="rows_loaded") or 0
        total += rows
        print(f"  {source_name:<20} {rows:>8,} rows loaded")

    print(f"  {'TOTAL':<20} {total:>8,} rows")
    print("══════════════════════════════════════\n")
    return total


# ── Task definitions ───────────────────────────────────────────────────────

ingest_tmdb = PythonOperator(
    task_id="ingest_tmdb",
    python_callable=run_tmdb,
    dag=dag,
)

ingest_omdb = PythonOperator(
    task_id="ingest_omdb",
    python_callable=run_omdb,
    dag=dag,
)

ingest_newsapi = PythonOperator(
    task_id="ingest_newsapi",
    python_callable=run_newsapi,
    dag=dag,
)

ingest_box_office = PythonOperator(
    task_id="ingest_box_office",
    python_callable=run_box_office,
    dag=dag,
)

ingest_google_trends = PythonOperator(
    task_id="ingest_google_trends",
    python_callable=run_google_trends,
    dag=dag,
)

audit = PythonOperator(
    task_id="audit",
    python_callable=run_audit,
    dag=dag,
)

# ── Task dependencies (the "graph") ───────────────────────────────────────
# The 5 ingestors run IN PARALLEL (no arrows between them).
# The audit task waits for ALL 5 to complete first.
#
#   ingest_tmdb    ─┐
#   ingest_omdb    ─┤
#   ingest_newsapi ─┼─→ audit
#   ingest_box_off ─┤
#   ingest_trends  ─┘

[
    ingest_tmdb,
    ingest_omdb,
    ingest_newsapi,
    ingest_box_office,
    ingest_google_trends,
] >> audit