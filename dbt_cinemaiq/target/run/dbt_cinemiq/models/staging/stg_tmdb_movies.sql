
  create or replace   view CINEMAIQ.DEV_staging.stg_tmdb_movies
  
   as (
    /*
stg_tmdb_movies.sql
Matches columns produced by ingestion/tmdb.py flatten_to_row():
tmdb_id, title, original_title, overview, tagline, status, release_date,
runtime_minutes, budget, revenue, popularity, vote_average, vote_count,
original_language, genres, directors, top_cast, cast_size, keywords, ingested_at
*/

WITH source AS(
    SELECT * FROM CINEMAIQ.RAW.raw_tmdb_movies
),

cleaned AS (
    SELECT
        TRY_CAST(tmdb_id AS INTEGER) AS tmdb_id,
        TRIM(title) AS title,
        TRIM(original_title) AS original_title,
        TRIM(overview) AS overview,
        TRIM(tagline) AS tagline,
        TRIM(status) AS status,
        TRY_CAST(release_date AS DATE) AS release_date,
        TRY_CAST(runtime_minutes AS INTEGER) AS runtime_minutes,
        NULLIF(TRY_CAST(budget AS BIGINT), 0) AS budget_usd,
        NULLIF(TRY_CAST(revenue AS BIGINT), 0) AS revenue_usd,
        TRY_CAST(popularity AS FLOAT) AS tmdb_popularity,
        TRY_CAST(vote_average AS FLOAT) AS tmdb_rating,
        TRY_CAST(vote_count AS INTEGER) AS vote_count,
        TRIM(original_language) AS original_language,
        genres AS genres_raw,
        directors AS directors_raw,
        top_cast AS top_cast_raw,
        TRY_CAST(cast_size AS INTEGER) AS cast_size,
        keywords AS keywords_raw,
        CASE
            WHEN TRY_CAST(budget AS BIGINT) > 0 and TRY_CAST(revenue AS BIGINT) > 0
            THEN round(TRY_CAST(revenue AS FLOAT) / TRY_CAST(budget AS FLOAT), 2)
            ELSE NULL
        END AS roi_ratio,
        YEAR(TRY_CAST(release_date AS DATE)) AS release_year,
        TRY_CAST(ingested_at AS TIMESTAMP_NTZ) AS ingested_at
    FROM SOURCE
),

filtered AS (
    SELECT * FROM cleaned
    WHERE tmdb_id IS NOT NULL AND title IS NOT NULL AND title != ''
)

SELECT * FROM filtered
  );

