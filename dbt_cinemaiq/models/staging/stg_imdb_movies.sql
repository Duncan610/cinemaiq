/*
stg_imdb_movies.sql
Matches columns produced by ingestion/imdb.py load_basics() + load_ratings():
basics: imdb_id, title_type, primary_title, original_title, is_adult,
        release_year, end_year, runtime_minutes, genres, ingested_at
ratings: imdb_id, imdb_rating, num_votes, ingested_at
*/

WITH basics AS (
    SELECT * FROM {{ source('raw', 'raw_imdb_basics') }}
),

ratings AS (
    SELECT * FROM {{ source('raw', 'raw_imdb_ratings') }}
),

joined AS (
    SELECT
        b.imdb_id,
        TRIM(b.primary_title) AS title,
        LOWER(TRIM(b.primary_title)) AS title_normalized,
        TRIM(b.original_title) AS original_title,
        TRIM(b.title_type) AS title_type,
        TRY_CAST(b.release_year AS INTEGER) AS release_year,
        TRY_CAST(b.runtime_minutes AS INTEGER) AS runtime_minutes,
        TRIM(b.genres) AS genres_raw,
        CASE
            WHEN b.is_adult = '1' THEN TRUE
            ELSE FALSE
        END AS is_adult,
        TRY_CAST(r.imdb_rating AS FLOAT) AS imdb_rating,
        TRY_CAST(r.num_votes AS INTEGER) AS num_votes,
        CASE
            WHEN TRY_CAST(r.num_votes AS INTEGER) >= 100000 THEN 'high_confidence'
            WHEN TRY_CAST(r.num_votes AS INTEGER) >= 10000 THEN 'medium_confidence'
            WHEN TRY_CAST(r.num_votes AS INTEGER) >= 1000 THEN 'low_confidence'
            ELSE 'minimal_votes'
        END AS vote_confidence,
        TRY_CAST(b.ingested_at AS TIMESTAMP_NTZ) AS ingested_at
    FROM basics b
    LEFT JOIN ratings r
        ON b.imdb_id = r.imdb_id
),

filtered AS (
    SELECT *
    FROM joined
    WHERE imdb_id IS NOT NULL
      AND title IS NOT NULL
      AND title != ''
      AND title_type = 'movie'
      AND is_adult = FALSE
      AND COALESCE(num_votes, 0) >= 100
)

SELECT *
FROM filtered