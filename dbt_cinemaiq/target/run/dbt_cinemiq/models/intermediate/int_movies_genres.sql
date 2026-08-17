
  create or replace   view CINEMAIQ.DEV_intermediate.int_movies_genres
  
   as (
    /*int_movie_genres.sql

WHAT THIS MODEL DOES:
TMDB genres arrive as one pipe-delimited string per movie, e.g.
"Action|Adventure|Sci-Fi". That's fine for storage but useless for
analysis you can't GROUP BY genre when three genres live in one cell.

This model explodes that string into one row per (movie, genre) pair.
A movie with 3 genres becomes 3 rows here. 

GRAIN: one row per (tmdb_id, genre).
*/

WITH movies AS (
    SELECT
        tmdb_id,
        title,
        release_year,
        genres_raw
    FROM CINEMAIQ.DEV_intermediate.int_movies_unified
    WHERE genres_raw IS NOT NULL
      AND genres_raw != ''
),

exploded AS (
    SELECT
        tmdb_id,
        title,
        release_year,
        TRIM(value::STRING) AS genre
    FROM movies,
    LATERAL SPLIT_TO_TABLE(genres_raw, '|')
)

SELECT *
FROM exploded
WHERE genre IS NOT NULL
  AND genre != ''
  );

