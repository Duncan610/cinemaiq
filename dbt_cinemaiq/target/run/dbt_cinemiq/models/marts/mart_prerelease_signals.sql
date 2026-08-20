
  
    

        create or replace transient table CINEMAIQ.DEV_marts.mart_prerelease_signals
         as
        (/*
mart_prerelease_signals.sql

WHAT THIS MART ANSWERS:
"Can pre-release signals predict a movie's box office outcome?"

REVISION NOTE:
This mart originally targeted OPENING WEEKEND specifically. That
required a working opening-weekend figure from Box Office Mojo, which
turned out not to exist on the yearly chart page we scrape (Box Office
Mojo restructured that page; it no longer has an "Opening" column at
all, confirmed by inspecting the actual table headers rather than
assuming a position). Getting real opening-weekend data would require
scraping hundreds of individual per-weekend pages out of scope for
now. This mart targets REVENUE_USD (total gross) instead. The
underlying question "do pre-release signals predict outcome" still
holds; the outcome variable is just less surgically "pre-release-only"
than opening weekend would have been, since total revenue is also
shaped by post-release word of mouth.

SCOPE NOTE ON PRE-RELEASE SIGNALS:
prerelease_article_count and prerelease_peak_search only have real
values for the ~80 highest-revenue movies in this dataset; that's
where NewsAPI and Google Trends fetches were targeted, since fetching
for all 600 movies isn't feasible under free-tier API limits. Movies
outside that top 80 will show null/zero for these columns; the
has_prerelease_signal_data flag makes that explicit so analysis can
filter to only the movies with real signal.

GRAIN: one row per movie (tmdb_id) with a non-null revenue_usd.
*/

WITH movies AS (
    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_movies_unified
    WHERE revenue_usd IS NOT NULL
),

news AS (
    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_news_coverage
),

trends_3m AS (
    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_trends_windowed
    WHERE window_type = '3_month'
),

joined AS (
    SELECT
        m.tmdb_id,
        m.title,
        m.release_date,
        m.release_year,
        m.revenue_usd,
        m.budget_usd,
        m.roi_ratio,
        m.revenue_tier,

        n.total_articles AS prerelease_article_count,
        n.avg_sentiment_rounded AS prerelease_sentiment,
        n.coverage_volume_category AS prerelease_coverage_level,

        t3.peak_interest_score AS prerelease_peak_search,
        t3.avg_interest_rounded AS prerelease_avg_search,
        t3.peak_interest_category AS prerelease_search_level

    FROM movies m

    LEFT JOIN news n
        ON LOWER(TRIM(m.title)) = n.title_normalized

    LEFT JOIN trends_3m t3
        ON LOWER(TRIM(m.title)) = t3.movie_title_normalized
),

scored AS (
    SELECT
        *,
        -- Revenue per dollar of budget — normalises for the fact that
        -- a $200M movie SHOULD gross more than a $20M movie in absolute terms
        CASE
            WHEN budget_usd IS NOT NULL
             AND budget_usd > 0
            THEN ROUND(revenue_usd / budget_usd, 3)
            ELSE NULL
        END AS revenue_to_budget_ratio,

        CASE
            WHEN prerelease_article_count IS NOT NULL
              OR prerelease_peak_search IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS has_prerelease_signal_data

    FROM joined
)

SELECT *
FROM scored
        );
      
  