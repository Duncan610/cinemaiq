
  
    

        create or replace transient table CINEMAIQ.DEV_marts.mart_genre_trends
         as
        (/*

mart_genre_trends.sql

WHAT THIS MART ANSWERS:

"Which genres are winning or fading over time?"

GRAIN: one row per (genre, release_year).

WHY A MOVIE CAN APPEAR IN MULTIPLE ROWS:

int_movie_genres already gave each movie one row per genre it's tagged
with (a movie tagged Action+Sci-Fi has 2 rows there). This mart sums
that back up by genre+year, so a 3-genre movie's revenue contributes
to 3 different genre buckets for that year. This is intentional — we
want "how did Action movies do in 2023" to include every Action movie,
regardless of what else it was also tagged as.

WHY AVG RATING ALONGSIDE AVG REVENUE:

A genre can be commercially strong but critically weak (or vice versa).
Tracking both lets a dashboard show "Horror grosses well but rates
lower than Drama"  a real pattern worth surfacing, not just revenue.
*/

WITH genres AS (

    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_movies_genres

),

movies AS (

    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_movies_unified

),

joined AS (

    SELECT
        g.genre,
        g.release_year,
        m.tmdb_id,
        m.revenue_usd,
        m.budget_usd,
        m.roi_ratio,
        m.tmdb_rating,
        m.imdb_rating,
        m.critic_audience_gap
    FROM genres g
    INNER JOIN movies m
        ON g.tmdb_id = m.tmdb_id
    WHERE g.release_year IS NOT NULL

),

aggregated AS (

    SELECT
        genre,
        release_year,
        COUNT(DISTINCT tmdb_id) AS movie_count,
        AVG(revenue_usd) AS avg_revenue_usd,
        SUM(revenue_usd) AS total_revenue_usd,
        AVG(budget_usd) AS avg_budget_usd,
        AVG(roi_ratio) AS avg_roi_ratio,
        AVG(tmdb_rating) AS avg_tmdb_rating,
        AVG(imdb_rating) AS avg_imdb_rating,
        AVG(critic_audience_gap) AS avg_critic_audience_gap
    FROM joined
    GROUP BY genre, release_year

),

rounded AS (

    SELECT
        genre,
        release_year,
        movie_count,
        ROUND(avg_revenue_usd, 0) AS avg_revenue_usd,
        total_revenue_usd,
        ROUND(avg_budget_usd, 0) AS avg_budget_usd,
        ROUND(avg_roi_ratio, 2) AS avg_roi_ratio,
        ROUND(avg_tmdb_rating, 2) AS avg_tmdb_rating,
        ROUND(avg_imdb_rating, 2) AS avg_imdb_rating,
        ROUND(avg_critic_audience_gap, 1) AS avg_critic_audience_gap
    FROM aggregated

)

SELECT *
FROM rounded
        );
      
  