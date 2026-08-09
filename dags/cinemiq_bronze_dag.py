import sys
sys.path.insert(0, "/opt/airflow/ingestion")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

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

default_args = {"owner": "cinemiq", "retries": 2, "retry_delay": timedelta(minutes=5)}


def task_ingest_tmdb(**ctx):
    from tmdb import run                      
    from loader import load_to_snowflake, log_audit    
    df = run(years=TARGET_YEARS, pages_per_year=5)
    n = load_to_snowflake(df, "RAW_TMDB_MOVIES", truncate=True)
    log_audit("RAW_TMDB_MOVIES", n, "TMDB_API")
    ctx["ti"].xcom_push(key="rows", value=n)


def task_ingest_omdb(**ctx):
    from omdb import run                      
    from loader import load_to_snowflake, log_audit
    df = run(MOVIE_LIST)
    n = load_to_snowflake(df, "RAW_OMDB_RATINGS", truncate=True)
    log_audit("RAW_OMDB_RATINGS", n, "OMDB_API")
    ctx["ti"].xcom_push(key="rows", value=n)


def task_ingest_newsapi(**ctx):
    from news_api import run                  
    from loader import load_to_snowflake, log_audit
    df = run(MOVIE_TITLES, days_back=30)
    n = load_to_snowflake(df, "RAW_NEWS_ARTICLES", truncate=True)
    log_audit("RAW_NEWS_ARTICLES", n, "NEWSAPI")
    ctx["ti"].xcom_push(key="rows", value=n)


def task_ingest_box_office(**ctx):
    from mojo import run                     
    from loader import load_to_snowflake, log_audit
    df = run(start_year=2019, end_year=2024)
    n = load_to_snowflake(df, "RAW_BOX_OFFICE", truncate=True)
    log_audit("RAW_BOX_OFFICE", n, "BOX_OFFICE_MOJO")
    ctx["ti"].xcom_push(key="rows", value=n)


def task_ingest_trends(**ctx):
    from trends import run                   
    from loader import load_to_snowflake, log_audit
    df = run(movie_titles=MOVIE_TITLES, timeframes=["today 12-m", "today 3-m"])
    n = load_to_snowflake(df, "RAW_GOOGLE_TRENDS", truncate=True)
    log_audit("RAW_GOOGLE_TRENDS", n, "GOOGLE_TRENDS")
    ctx["ti"].xcom_push(key="rows", value=n)


def task_check_imdb(**ctx):
    from loader import get_connection          
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM CINEMAIQ.RAW.RAW_IMDB_BASICS")
    n = cur.fetchone()[0]
    cur.close(); conn.close()
    ctx["ti"].xcom_push(key="rows", value=n)


def task_summary(**ctx):
    ti = ctx["ti"]
    vals = {k: ti.xcom_pull(task_ids=k, key="rows") or 0 for k in
             ["ingest_tmdb", "ingest_omdb", "ingest_newsapi", "ingest_box_office", "ingest_trends", "check_imdb"]}
    print("CinemaIQ Bronze Summary:", vals, "Total:", sum(vals.values()))


with DAG(
    dag_id="cinemiq_bronze_ingestion",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cinemiq", "bronze"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    t1 = PythonOperator(task_id="ingest_tmdb", python_callable=task_ingest_tmdb)
    t2 = PythonOperator(task_id="ingest_omdb", python_callable=task_ingest_omdb)
    t3 = PythonOperator(task_id="ingest_newsapi", python_callable=task_ingest_newsapi)
    t4 = PythonOperator(task_id="ingest_box_office", python_callable=task_ingest_box_office)
    t5 = PythonOperator(task_id="ingest_trends", python_callable=task_ingest_trends)
    t6 = PythonOperator(task_id="check_imdb", python_callable=task_check_imdb)
    t7 = PythonOperator(task_id="summary", python_callable=task_summary)

    start >> [t1, t2, t3, t4, t5, t6] >> t7 >> end