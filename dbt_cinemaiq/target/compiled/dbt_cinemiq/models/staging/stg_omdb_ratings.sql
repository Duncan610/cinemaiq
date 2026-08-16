/*
stg_omdb_ratings.sql
Matches columns produced by ingestion/omdb.py flatten_to_row():
imdb_id, title, year, rated, released, runtime, genre, director, actors,
plot, country, awards, imdb_rating, imdb_votes, rt_score, metacritic_score,
avg_critic_score, imdb_normalised, critic_audience_gap, box_office_usd, ingested_at
*/

WITH source AS (
    SELECT * FROM CINEMAIQ.RAW.raw_omdb_ratings
),

cleaned AS (
    SELECT
        TRIM(imdb_id) AS imdb_id,
        TRIM(title) AS title,
        TRY_CAST(LEFT(TRIM(year), 4) AS INTEGER) AS release_year,
        TRIM(rated) AS mpaa_rating,
        TRY_CAST(released AS DATE) AS release_date,
        TRIM(genre) AS genres_raw,
        TRIM(director) AS director,
        TRIM(actors) AS actors_raw,
        TRIM(plot) AS plot,
        TRIM(country) AS country,
        TRIM(awards) AS awards_text,
        TRY_CAST(imdb_rating AS FLOAT) AS imdb_rating,
        TRY_CAST(imdb_votes AS INTEGER) AS imdb_votes,
        TRY_CAST(rt_score AS FLOAT) AS rt_score,
        TRY_CAST(metacritic_score AS FLOAT) AS metacritic_score,
        TRY_CAST(avg_critic_score AS FLOAT) AS avg_critic_score,
        TRY_CAST(imdb_normalised AS FLOAT) AS imdb_score_normalised,
        TRY_CAST(critic_audience_gap AS FLOAT) AS critic_audience_gap,
        CASE
            WHEN TRY_CAST(critic_audience_gap AS FLOAT) > 20 THEN 'critics_loved_it'
            WHEN TRY_CAST(critic_audience_gap AS FLOAT) > 5 THEN 'critics_preferred'
            WHEN TRY_CAST(critic_audience_gap AS FLOAT) >= -5 THEN 'consensus'
            WHEN TRY_CAST(critic_audience_gap AS FLOAT) >= -20 THEN 'audiences_preferred'
            ELSE 'audiences_loved_it'
        END AS gap_category,
        TRY_CAST(box_office_usd AS BIGINT) AS omdb_box_office_usd,
        TRY_CAST(ingested_at AS TIMESTAMP_NTZ) AS ingested_at
    FROM source
),

filtered AS (
    SELECT *
    FROM cleaned
    WHERE imdb_id IS NOT NULL
      AND title IS NOT NULL
)

SELECT *
FROM filtered