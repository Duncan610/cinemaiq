/*
stg_google_trends.sql
Matches columns produced by ingestion/trends.py run() output (melted):
date, keyword, interest_score, timeframe, ingested_at, movie_title
*/

WITH source AS (
    SELECT * FROM CINEMAIQ.RAW.raw_google_trends
),

cleaned AS (
    SELECT
        TRIM(keyword) AS keyword_raw,
        TRIM(movie_title) AS movie_title_normalized,
        TRY_CAST(date AS DATE) AS trend_date,
        TRY_CAST(interest_score AS INTEGER) AS interest_score,
        CASE
            WHEN TRY_CAST(interest_score AS INTEGER) >= 75 THEN 'high'
            WHEN TRY_CAST(interest_score AS INTEGER) >= 40 THEN 'medium'
            WHEN TRY_CAST(interest_score AS INTEGER) >= 10 THEN 'low'
            ELSE 'minimal'
        END                                                AS interest_level,
        TRIM(timeframe)                                    AS timeframe_raw,
        CASE
            WHEN TRIM(timeframe) = 'today 3-m' THEN '3_month'
            WHEN TRIM(timeframe) = 'today 12-m' THEN '12_month'
            ELSE 'other'
        END AS window_type,
        TRY_CAST(ingested_at AS TIMESTAMP_NTZ) AS ingested_at
    FROM source
),

filtered AS (
    SELECT *
    FROM cleaned
    WHERE keyword_raw IS NOT NULL
      AND trend_date IS NOT NULL
      AND interest_score BETWEEN 0 AND 100
)

SELECT *
FROM filtered