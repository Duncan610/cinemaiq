
  create or replace   view CINEMAIQ.DEV_intermediate.int_movie_cast
  
   as (
    /*
int_movie_cast.sql

WHAT THIS MODEL DOES:
Same explosion pattern as int_movie_genres, applied to top_cast_raw.
TMDB gives us the top 5 billed actors per movie as one pipe-delimited
string. This model turns that into one row per (movie, actor) pair.

WHY TOP 5 ONLY:
The ingestor (tmdb.py) already limited this to the first 5 cast members
at fetch time full casts can run 50+ people deep and add noise without
adding signal for a "does star power drive revenue" analysis.

GRAIN: one row per (tmdb_id, actor_name).
Also includes cast_position (1 = top-billed) since billing order carries
real signal a lead actor's draw is not the same as actor #5's.
*/

WITH movies AS (
    SELECT
        tmdb_id,
        title,
        release_year,
        revenue_usd,
        top_cast_raw
    FROM CINEMAIQ.DEV_intermediate.int_movies_unified
    WHERE top_cast_raw IS NOT NULL
      AND top_cast_raw != ''
),

exploded AS (
    SELECT
        tmdb_id,
        title,
        release_year,
        revenue_usd,
        TRIM(value::STRING) AS actor_name,
        index + 1 AS cast_position  -- SPLIT_TO_TABLE index is 0-based
    FROM movies,
    LATERAL SPLIT_TO_TABLE(top_cast_raw, '|')
)

SELECT *
FROM exploded
WHERE actor_name IS NOT NULL
  AND actor_name != ''
  );

