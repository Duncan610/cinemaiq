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

with tmdb as (
    select * from CINEMAIQ.DEV_staging.stg_tmdb_movies
),

imdb as (
    select * from CINEMAIQ.DEV_staging.stg_imdb_movies
),

omdb as (
    select * from CINEMAIQ.DEV_staging.stg_omdb_ratings
),

box_office as (
    select * from CINEMAIQ.DEV_staging.stg_box_office
),

-- Normalise TMDB title once so every join below uses the same key
tmdb_keyed as (
    select
        *,
        lower(trim(title))     as join_title,
        release_year           as join_year
    from tmdb
),

-- Dedup IMDb: when multiple movies share a title+year, keep the one
-- with the most votes — the version people actually mean by that title
imdb_deduped as (
    select *
    from imdb
    qualify row_number() over (
        partition by title_normalized, release_year
        order by num_votes desc nulls last
    ) = 1
),

-- Dedup OMDB: same logic, ranked by IMDb vote count as the tiebreaker
omdb_deduped as (
    select *
    from omdb
    qualify row_number() over (
        partition by lower(trim(title)), release_year
        order by imdb_votes desc nulls last
    ) = 1
),

-- Dedup Box Office Mojo: rank by highest worldwide gross —
-- the version with actual box office data is the one we want
box_office_deduped as (
    select *
    from box_office
    qualify row_number() over (
        partition by title_normalized, release_year
        order by worldwide_gross_usd desc nulls last
    ) = 1
),

joined as (
    select
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
        coalesce(t.budget_usd, null)                          as budget_usd,
        coalesce(t.revenue_usd, bo.worldwide_gross_usd)        as revenue_usd,
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
        i.num_votes                as imdb_num_votes,
        i.vote_confidence           as imdb_vote_confidence,

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
        case when i.imdb_id       is not null then true else false end as matched_imdb,
        case when o.imdb_id       is not null then true else false end as matched_omdb,
        case when bo.title        is not null then true else false end as matched_box_office

    from tmdb_keyed t

    -- IMDb: match on normalised title + year, deduped source
    left join imdb_deduped i
        on t.join_title = i.title_normalized
       and t.join_year  = i.release_year

    -- OMDB: match through the IMDb id we just resolved (most reliable path)
    -- falls back to title+year match if IMDb didn't match
    left join omdb_deduped o
        on (i.imdb_id is not null and i.imdb_id = o.imdb_id)
        or (i.imdb_id is null and t.join_title = lower(trim(o.title)) and t.join_year = o.release_year)

    -- Box Office Mojo: no ID, title + year only, deduped source
    left join box_office_deduped bo
        on t.join_title = bo.title_normalized
       and t.join_year  = bo.release_year
)

select * from joined