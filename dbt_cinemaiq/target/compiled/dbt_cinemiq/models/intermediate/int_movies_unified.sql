/*int_movies_unified.sql

WHAT THIS MODEL DOES:
This is the spine of the Intermediate layer. It joins four independent
sources — TMDB, IMDb, OMDB, and Box Office Mojo — into ONE row per movie.

WHY THIS IS HARD:
Each source has a different idea of "movie identity":
  - TMDB:        tmdb_id (numeric)
  - IMDb/OMDB:   imdb_id (e.g. tt0111161)
  - Box Office:  no ID at all — just title + year

There is no shared foreign key across all four. So the join key here
is (title_normalized, release_year) — lowercased, trimmed titles matched
against release year. This is a FUZZY join, not an exact one, and it
will miss some movies where titles differ slightly between sources
(e.g. "Spider-Man: No Way Home" vs "Spider-Man No Way Home").

WHY TMDB IS THE ANCHOR:
We start from stg_tmdb_movies and LEFT JOIN everything else onto it.
TMDB has the richest metadata (budget, genres, cast) and the widest
release-year coverage, so it's the natural base table. Movies that
exist in Box Office Mojo but not TMDB are intentionally excluded here —
this model is "TMDB movies, enriched," not "union of everything."

GRAIN: one row per tmdb_id.
*/

WITH tmdb AS (
    SELECT * FROM CINEMAIQ.DEV_staging.stg_tmdb_movies
),

imdb AS (
    SELECT * FROM CINEMAIQ.DEV_staging.stg_imdb_movies
),

omdb AS (
    SELECT * FROM CINEMAIQ.DEV_staging.stg_omdb_ratings
),

box_office AS (
    SELECT * FROM CINEMAIQ.DEV_staging.stg_box_office
),

-- Normalise TMDB title once so every join below uses the same key
tmdb_keyed AS (
    SELECT
        *,
        LOWER(TRIM(title)) AS join_title,
        release_year AS join_year
    FROM tmdb
),

joined AS (
    SELECT
        -- Identifiers
        t.tmdb_id,
        i.imdb_id,

        -- Core metadata (TMDB is the source of truth here)
        t.title,
        t.original_title,
        t.release_date,
        t.release_year,
        t.status,
        t.original_language,
        t.runtime_minutes,
        t.overview,
        t.tagline,

        -- Financials — prefer TMDB, fall back to Box Office Mojo when TMDB is null
        COALESCE(t.budget_usd, NULL) AS budget_usd,
        COALESCE(t.revenue_usd, bo.worldwide_gross_usd) AS revenue_usd,
        bo.domestic_gross_usd,
        bo.worldwide_gross_usd,
        bo.opening_weekend_usd,
        bo.revenue_tier,
        t.roi_ratio,

        -- TMDB popularity/rating
        t.tmdb_rating,
        t.tmdb_popularity,
        t.vote_count,

        -- IMDb rating (independent audience signal, larger vote base)
        i.imdb_rating,
        i.num_votes AS imdb_num_votes,
        i.vote_confidence AS imdb_vote_confidence,

        -- OMDB — critic vs audience signal, the standout feature
        o.rt_score,
        o.metacritic_score,
        o.avg_critic_score,
        o.critic_audience_gap,
        o.gap_category,
        o.mpaa_rating,
        o.awards_text,

        -- Raw fields still needed for exploding in other Intermediate models
        t.genres_raw,
        t.directors_raw,
        t.top_cast_raw,
        t.keywords_raw,

        -- Match quality flags — lets downstream marts filter confidently
        CASE
            WHEN i.imdb_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS matched_imdb,

        CASE
            WHEN o.imdb_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS matched_omdb,

        CASE
            WHEN bo.title IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS matched_box_office

    FROM tmdb_keyed t

    -- IMDb: match on normalised title + year
    LEFT JOIN imdb i
        ON t.join_title = i.title_normalized
       AND t.join_year = i.release_year

    -- OMDB: match through the IMDb id we just resolved (most reliable path)
    -- falls back to title+year match if IMDb didn't match
    LEFT JOIN omdb o
        ON (
            i.imdb_id IS NOT NULL
            AND i.imdb_id = o.imdb_id
        )
        OR (
            i.imdb_id IS NULL
            AND t.join_title = LOWER(TRIM(o.title))
            AND t.join_year = o.release_year
        )

    -- Box Office Mojo: no ID, title + year only
    LEFT JOIN box_office bo
        ON t.join_title = bo.title_normalized
       AND t.join_year = bo.release_year
)

SELECT *
FROM joined