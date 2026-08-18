/*
mart_success_drivers.sql

WHAT THIS MART ANSWERS:
"What combination of factors correlates with a movie's success?"
This is the kitchen-sink mart one row per movie with every signal
we have side by side: budget, critic/audience gap, genre count,
cast size, revenue, ROI. Built for exploratory analysis and for
feeding a BI tool's scatter plots / correlation views.

GRAIN: one row per tmdb_id (same grain as int_movies_unified).

WHY GENRE AND CAST ARE COUNTS HERE, NOT EXPLODED ROWS:
int_movie_genres and int_movie_cast deliberately explode to one-row-
per-genre / one-row-per-actor for mart_genre_trends. But this mart
needs ONE row per movie, so here we re-aggregate those back into
simple counts (genre_count, has_cast_data) enough to ask "do movies
with more genres tagged perform differently" without re-exploding.

SUCCESS METRIC CHOICE:
revenue_usd is the primary success signal (TMDB revenue, falling back
to Box Office Mojo worldwide gross where TMDB is null this fallback
already happened upstream in int_movies_unified). roi_ratio is kept
alongside it since a $50M movie that made $200M is arguably more
"successful" than a $300M movie that made $320M, even though the
second has higher raw revenue.
*/

WITH movies AS (
    SELECT *
    FROM {{ ref('int_movies_unified') }}
),

genre_counts AS (
    SELECT
        tmdb_id,
        COUNT(*) AS genre_count
    FROM {{ ref('int_movie_genres') }}
    GROUP BY tmdb_id
),

cast_counts AS (
    SELECT
        tmdb_id,
        COUNT(*) AS cast_count
    FROM {{ ref('int_movie_cast') }}
    GROUP BY tmdb_id
),

final AS (
    SELECT
        m.tmdb_id,
        m.imdb_id,
        m.title,
        m.release_date,
        m.release_year,

        -- Financials
        m.budget_usd,
        m.revenue_usd,
        m.roi_ratio,
        m.revenue_tier,

        -- Reception
        m.tmdb_rating,
        m.imdb_rating,
        m.rt_score,
        m.metacritic_score,
        m.critic_audience_gap,
        m.gap_category,

        -- Production signals
        COALESCE(g.genre_count, 0) AS genre_count,
        COALESCE(c.cast_count, 0) AS cast_count,
        m.runtime_minutes,
        m.mpaa_rating,

        -- Data completeness flags — lets analysis filter to fully-matched movies
        m.matched_imdb,
        m.matched_omdb,
        m.matched_box_office

    FROM movies m

    LEFT JOIN genre_counts g
        ON m.tmdb_id = g.tmdb_id

    LEFT JOIN cast_counts c
        ON m.tmdb_id = c.tmdb_id
)

SELECT *
FROM final;