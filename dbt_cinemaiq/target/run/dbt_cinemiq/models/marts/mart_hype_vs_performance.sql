
  
    

        create or replace transient table CINEMAIQ.DEV_marts.mart_hype_vs_performance
         as
        (/*
mart_hype_vs_performance.sql
WHAT THIS MART ANSWERS:
"Did pre-release buzz match the actual box office outcome?"
This is the storytelling mart — combines media coverage (NewsAPI) and
search interest (Google Trends) into one "hype score" per movie, then
sits it next to actual revenue so you can see where hype and performance
agreed (blockbusters everyone expected) and where they diverged
(quiet sleeper hits, or expensive flops nobody saw coming).

GRAIN: one row per movie (tmdb_id).

WHY BOTH TRENDS WINDOWS ARE COMBINED HERE (unlike mart_prerelease_signals):
This mart is about the FULL hype picture, not narrow prediction — so we
use both the 3-month and 12-month peak interest scores. mart_prerelease_
signals is intentionally narrower and uses only the 3-month window,
since that's the cleaner "right before release" signal for prediction.

HYPE SCORE CONSTRUCTION:
A simple normalized composite: average of (news coverage volume score,
peak search interest score), each already 0-100-ish scale from their
source. This is not a sophisticated model — it's a first-pass signal
meant to be visualized, not a predictive feature. Documented plainly
so nobody mistakes it for more than it is.
*/

WITH movies AS (
    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_movies_unified
),

news AS (
    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_news_coverage
),

trends_12m AS (
    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_trends_windowed
    WHERE window_type = '12_month'
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

        n.total_articles,
        n.unique_sources,
        n.avg_sentiment_rounded AS news_avg_sentiment,
        n.buzz_sentiment_category,
        n.coverage_volume_category,

        t12.peak_interest_score AS trends_peak_12m,
        t12.avg_interest_rounded AS trends_avg_12m,
        t3.peak_interest_score AS trends_peak_3m,
        t3.avg_interest_rounded AS trends_avg_3m

    FROM movies m

    LEFT JOIN news n
        ON LOWER(TRIM(m.title)) = n.title_normalized

    LEFT JOIN trends_12m t12
        ON LOWER(TRIM(m.title)) = t12.movie_title_normalized

    LEFT JOIN trends_3m t3
        ON LOWER(TRIM(m.title)) = t3.movie_title_normalized
),

scored AS (
    SELECT
        *,
        -- Simple composite hype score: average of news volume (capped 0-100
        -- via article count) and peak 12-month search interest (already 0-100)
        ROUND(
            (
                LEAST(COALESCE(total_articles, 0) * 5, 100)
                + COALESCE(trends_peak_12m, 0)
            ) / 2.0,
            1
        ) AS hype_score,

        CASE
            WHEN revenue_usd IS NOT NULL
             AND budget_usd IS NOT NULL
             AND budget_usd > 0
            THEN ROUND(revenue_usd / budget_usd, 2)
            ELSE NULL
        END AS performance_ratio

    FROM joined
),
classified AS (
    SELECT
        *,
        CASE
            WHEN hype_score >= 40 THEN 'high_hype'
            WHEN hype_score >= 20 THEN 'moderate_hype'
            ELSE 'low_hype'
        END AS hype_category,

        CASE
            WHEN hype_score >= 40
             AND revenue_tier IN ('blockbuster', 'wide_release')
                THEN 'hype_matched_performance'

            WHEN hype_score >= 40
             AND revenue_tier IN ('limited_release', 'low_performer')
                THEN 'overhyped'

            WHEN hype_score < 20
             AND revenue_tier IN ('blockbuster', 'wide_release')
                THEN 'quiet_success'

            ELSE 'as_expected'
        END AS hype_vs_outcome

    FROM scored
)

SELECT *
FROM classified
        );
      
  