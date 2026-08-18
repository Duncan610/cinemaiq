
  
    

        create or replace transient table CINEMAIQ.DEV_marts.mart_prerelease_signals
         as
        (/*
mart_prerelease_signals.sql

WHAT THIS MART ANSWERS:
"Can pre-release signals predict opening weekend specifically?"
This is the narrow, prediction-focused sibling of mart_hype_vs_performance.
Two deliberate scoping choices set it apart:

Only the 3-MONTH trends window is used, not 12-month. The 3-month
window is the cleanest "right before release" signal — the 12-month
window mixes in long-term franchise buildup that isn't a fair
predictor for a general model (a Marvel movie has 12-month interest
from being Marvel, not from THIS movie's marketing).

The outcome variable is OPENING WEEKEND, not total revenue. Total
revenue is shaped by word-of-mouth AFTER release — a bad opening
can still "legs" its way to a strong total. Opening weekend is the
one number that's purely a function of PRE-release anticipation,
which is exactly what this mart's inputs are trying to predict.

GRAIN: one row per movie (tmdb_id), restricted to movies that actually
have an opening_weekend_usd figure no opening weekend data means
this mart can't evaluate them, so they're excluded rather than kept
with a misleading null outcome.
*/

WITH movies AS (
    SELECT *
    FROM CINEMAIQ.DEV_intermediate.int_movies_unified
    WHERE opening_weekend_usd IS NOT NULL
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
        m.opening_weekend_usd,
        m.budget_usd,
        m.revenue_usd,

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
        -- Opening weekend per dollar of budget — normalises for the fact
        -- that a $200M movie SHOULD open bigger than a $20M movie
        CASE
            WHEN budget_usd IS NOT NULL
             AND budget_usd > 0
            THEN ROUND(opening_weekend_usd / budget_usd, 3)
            ELSE NULL
        END AS opening_to_budget_ratio,

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
      
  