/*
int_movies_unified.sql

WHAT THIS MODEL DOES:
This query joins four independent
sources — TMDB, IMDb, OMDB, and Box Office Mojo — into ONE row per movie.

Each source has a different idea of "movie identity":
  - TMDB:        tmdb_id (numeric)
  - IMDb/OMDB:   imdb_id (e.g. tt0111161)
  - Box Office:  no ID at all — just title + year

There is no shared foreign key across all four. So the join key here
is (title_normalized, release_year) — lowercased, trimmed titles matched
against release year.

WHY WE DEDUPLICATE EACH SOURCE BEFORE JOINING:
Common titles ("Beast", "Smile", "Rocketman") exist as MULTIPLE different
movies released in the same year — foreign films sharing an English
title, remakes, festival re-releases. If IMDb has 2 different movies
both called "Beast" (2022), a naive join makes our single TMDB "Beast"
row match BOTH of them, doubling it in the output.

The fix: before joining, we rank each source's rows within
(title_normalized, year) and keep only the top-ranked one — the one
with the most votes/most data, since that's almost always the movie
the title actually refers to. This guarantees each join adds AT MOST
one row per tmdb_id, so tmdb_id stays unique in the final output.

WHY TMDB IS THE ANCHOR:
We start from stg_tmdb_movies and LEFT JOIN everything else onto it.
TMDB has the richest metadata (budget, genres, cast) and the widest
release-year coverage.

GRAIN: one row per tmdb_id.
*/

WITH tmdb AS (
    SELECT *
    FROM {{ ref('stg_tmdb_movies') }}
),

imdb AS (
    SELECT *
    FROM {{ ref('stg_imdb_movies') }}
),

omdb AS (
    SELECT *
    FROM {{ ref('stg_omdb_ratings') }}
),

box_office AS (
    SELECT *
    FROM {{ ref('stg_box_office') }}
),

-- Normalise TMDB title once so every join below uses the same key
tmdb_keyed AS (
    SELECT
        *,
        LOWER(TRIM(title)) AS join_title,
        release_year AS join_year
    FROM tmdb
),

-- Dedup IMDb: when multiple movies share a title+year, keep the one
-- with the most votes — the version people actually mean by that title
imdb_deduped AS (
    SELECT *
    FROM imdb
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY title_normalized, release_year
        ORDER BY num_votes DESC NULLS LAST
    ) = 1
),

-- Dedup OMDB: same logic, ranked by IMDb vote count as the tiebreaker
omdb_deduped AS (
    SELECT *
    FROM omdb
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(title)), release_year
        ORDER BY imdb_votes DESC NULLS LAST
    ) = 1
),

-- Dedup Box Office Mojo: rank by highest worldwide gross —
-- the version with actual box office data is the one we want
box_office_deduped AS (
    SELECT *
    FROM box_office
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY title_normalized, release_year
        ORDER BY worldwide_gross_usd DESC NULLS LAST
    ) = 1
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

    -- IMDb: match on normalised title + year, deduped source
    LEFT JOIN imdb_deduped i
        ON t.join_title = i.title_normalized
       AND t.join_year = i.release_year

    -- OMDB: match through the IMDb id we just resolved (most reliable path)
    -- falls back to title+year match if IMDb didn't match
    LEFT JOIN omdb_deduped o
        ON (
            i.imdb_id IS NOT NULL
            AND i.imdb_id = o.imdb_id
        )
        OR (
            i.imdb_id IS NULL
            AND t.join_title = LOWER(TRIM(o.title))
            AND t.join_year = o.release_year
        )

    -- Box Office Mojo: no ID, title + year only, deduped source
    LEFT JOIN box_office_deduped bo
        ON t.join_title = bo.title_normalized
       AND t.join_year = bo.release_year
)

SELECT *
FROM joined;